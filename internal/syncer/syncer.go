package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"

	"github.com/FtlC-ian/zulcrawl/internal/config"
	"github.com/FtlC-ian/zulcrawl/internal/store"
	"github.com/FtlC-ian/zulcrawl/internal/zulip"
)

type Syncer struct {
	cfg   *config.Config
	api   *zulip.Client
	store *store.Store
	orgID int64
}

const (
	privateStreamID   int64 = 0
	privateStreamName       = "direct-messages"
	privateTopicName        = "direct messages"
)

type Options struct {
	Full    bool
	Streams []string
	Since   string
}

func New(cfg *config.Config, api *zulip.Client, st *store.Store) *Syncer {
	return &Syncer{cfg: cfg, api: api, store: st, orgID: 1}
}

func (s *Syncer) Sync(ctx context.Context, opts Options) error {
	streams, err := s.api.Streams(ctx)
	if err != nil {
		return err
	}
	users, err := s.api.Users(ctx)
	if err != nil {
		return err
	}
	for _, u := range users {
		if err := s.store.UpsertUser(ctx, store.User{
			ID:        u.UserID,
			OrgID:     s.orgID,
			Email:     u.Email,
			FullName:  u.FullName,
			IsBot:     u.IsBot,
			AvatarURL: u.AvatarURL,
		}); err != nil {
			return err
		}
	}

	selected := filterStreams(streams, s.cfg.Sync.Streams, s.cfg.Sync.ExcludeStreams, opts.Streams)
	if len(selected) > 0 {
		if err := s.syncStreams(ctx, selected, opts); err != nil {
			return err
		}
	}

	if err := s.syncPrivateMessages(ctx, opts); err != nil {
		return fmt.Errorf("private messages: %w", err)
	}

	return nil
}

func (s *Syncer) syncStreams(ctx context.Context, selected []zulip.Stream, opts Options) error {
	workers := s.cfg.Sync.Concurrency
	if workers <= 0 {
		workers = 4
	}
	ch := make(chan zulip.Stream)
	errCh := make(chan error, len(selected))
	wg := sync.WaitGroup{}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for st := range ch {
				if err := s.syncStream(ctx, st, opts); err != nil {
					errCh <- fmt.Errorf("stream %s: %w", st.Name, err)
				}
			}
		}()
	}

	for _, st := range selected {
		if err := s.store.UpsertStream(ctx, store.Stream{
			ID:          st.StreamID,
			OrgID:       s.orgID,
			Name:        st.Name,
			Description: st.Description,
			InviteOnly:  st.InviteOnly,
			IsWebPublic: st.IsWebPublic,
		}); err != nil {
			close(ch)
			wg.Wait()
			close(errCh)
			return err
		}
		// Respect context cancellation while distributing work.
		select {
		case ch <- st:
		case <-ctx.Done():
			close(ch)
			wg.Wait()
			close(errCh)
			return ctx.Err()
		}
	}
	close(ch)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Syncer) syncStream(ctx context.Context, st zulip.Stream, opts Options) error {
	anchor := "oldest"
	if !opts.Full {
		if last, err := s.store.LastMessageID(ctx, s.orgID, st.StreamID); err == nil && last > 0 {
			anchor = fmt.Sprintf("%d", last)
		}
	}

	touched := map[int64]struct{}{}
	var maxID int64
	for {
		resp, err := s.api.Messages(ctx, zulip.MessagesRequest{
			Anchor:    anchor,
			NumBefore: 0,
			NumAfter:  1000,
			Narrow: []map[string]any{{
				"operator": "stream",
				"operand":  st.Name,
			}},
			// Request rendered HTML so content is stored as HTML and
			// htmlToText can properly strip tags into clean plain text for FTS.
			ApplyMarkdown: true,
		})
		if err != nil {
			return err
		}
		if len(resp.Messages) == 0 {
			break
		}

		// Build the batch for this page.
		// Auto-upsert senders from message metadata. The /users endpoint may
		// not return bots, system users, or deactivated accounts, but every
		// message carries sender_id + sender_full_name, so we can ensure the
		// FK target exists before inserting the message.
		for _, m := range resp.Messages {
			if err := s.store.UpsertUser(ctx, store.User{
				ID:       m.SenderID,
				OrgID:    s.orgID,
				FullName: m.SenderFullName,
			}); err != nil {
				return fmt.Errorf("auto-upsert sender %d (%s): %w", m.SenderID, m.SenderFullName, err)
			}
		}
		batch := make([]store.Message, 0, len(resp.Messages))
		for _, m := range resp.Messages {
			topic := m.Topic
			if topic == "" {
				topic = m.Subject
			}
			ts := time.Unix(m.Timestamp, 0).UTC().Format(time.RFC3339)
			if opts.Since != "" && ts < opts.Since {
				continue
			}
			topicID, err := s.store.GetOrCreateTopic(ctx, s.orgID, st.StreamID, topic)
			if err != nil {
				return fmt.Errorf("get/create topic %q in stream %d: %w", topic, st.StreamID, err)
			}
			touched[topicID] = struct{}{}
			editTS := ""
			if m.EditTimestamp > 0 {
				editTS = time.Unix(m.EditTimestamp, 0).UTC().Format(time.RFC3339)
			}
			contentText := htmlToText(m.Content)
			hasLink := strings.Contains(strings.ToLower(m.Content), "href=") ||
				strings.Contains(contentText, "http://") ||
				strings.Contains(contentText, "https://")
			hasImage := strings.Contains(strings.ToLower(m.Content), "<img")
			hasAttachment := strings.Contains(strings.ToLower(m.Content), "class=\"message_inline_ref\"")

			batch = append(batch, store.Message{
				ID:            m.ID,
				OrgID:         s.orgID,
				StreamID:      st.StreamID,
				TopicID:       topicID,
				SenderID:      m.SenderID,
				Content:       m.Content,
				ContentText:   contentText,
				Timestamp:     ts,
				EditTimestamp: editTS,
				HasAttachment: hasAttachment,
				HasImage:      hasImage,
				HasLink:       hasLink,
				Reactions:     json.RawMessage(m.Reactions),
				// m.IsMe reflects the API's is_me_message boolean (/me action
				// messages).  m.Type=="private" means a direct message, which
				// is a separate concept and should not reach the stream syncer.
				IsMeMessage: m.IsMe,
			})
			if m.ID > maxID {
				maxID = m.ID
			}
		}

		// Wrap the entire page in one transaction for throughput.
		if err := s.store.UpsertMessageBatch(ctx, batch); err != nil {
			return fmt.Errorf("batch upsert (%d msgs): %w", len(batch), err)
		}

		// Advance anchor past the last seen ID to avoid re-fetching it on the
		// next page.  Zulip includes the anchor message in the result set, so
		// using last_id+1 prevents a redundant duplicate on every page turn.
		lastID := resp.Messages[len(resp.Messages)-1].ID
		anchor = fmt.Sprintf("%d", lastID+1)
		if resp.FoundNewest {
			break
		}
	}

	for tid := range touched {
		if err := s.store.RecomputeTopicStats(ctx, tid); err != nil {
			return err
		}
	}
	if maxID > 0 {
		if err := s.store.UpdateSyncState(ctx, s.orgID, st.StreamID, maxID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Syncer) syncPrivateMessages(ctx context.Context, opts Options) error {
	if err := s.store.UpsertStream(ctx, store.Stream{
		ID:          privateStreamID,
		OrgID:       s.orgID,
		Name:        privateStreamName,
		Description: "direct/private Zulip messages",
		InviteOnly:  true,
		IsWebPublic: false,
	}); err != nil {
		return err
	}

	anchor := "oldest"
	if !opts.Full {
		if last, err := s.store.LastMessageID(ctx, s.orgID, privateStreamID); err == nil && last > 0 {
			anchor = fmt.Sprintf("%d", last)
		}
	}

	touched := map[int64]struct{}{}
	var maxID int64
	for {
		resp, err := s.api.Messages(ctx, zulip.MessagesRequest{
			Anchor:    anchor,
			NumBefore: 0,
			NumAfter:  1000,
			Narrow: []map[string]any{{
				"operator": "is",
				"operand":  "private",
			}},
			ApplyMarkdown: true,
		})
		if err != nil {
			return err
		}
		if len(resp.Messages) == 0 {
			break
		}

		for _, m := range resp.Messages {
			if err := s.store.UpsertUser(ctx, store.User{
				ID:       m.SenderID,
				OrgID:    s.orgID,
				FullName: m.SenderFullName,
			}); err != nil {
				return fmt.Errorf("auto-upsert sender %d (%s): %w", m.SenderID, m.SenderFullName, err)
			}
		}

		batch := make([]store.Message, 0, len(resp.Messages))
		for _, m := range resp.Messages {
			ts := time.Unix(m.Timestamp, 0).UTC().Format(time.RFC3339)
			if opts.Since != "" && ts < opts.Since {
				continue
			}
			topicID, err := s.store.GetOrCreateTopic(ctx, s.orgID, privateStreamID, privateTopicName)
			if err != nil {
				return fmt.Errorf("get/create private topic: %w", err)
			}
			touched[topicID] = struct{}{}

			editTS := ""
			if m.EditTimestamp > 0 {
				editTS = time.Unix(m.EditTimestamp, 0).UTC().Format(time.RFC3339)
			}
			contentText := htmlToText(m.Content)
			hasLink := strings.Contains(strings.ToLower(m.Content), "href=") ||
				strings.Contains(contentText, "http://") ||
				strings.Contains(contentText, "https://")
			hasImage := strings.Contains(strings.ToLower(m.Content), "<img")
			hasAttachment := strings.Contains(strings.ToLower(m.Content), "class=\"message_inline_ref\"")

			batch = append(batch, store.Message{
				ID:            m.ID,
				OrgID:         s.orgID,
				StreamID:      privateStreamID,
				TopicID:       topicID,
				SenderID:      m.SenderID,
				Content:       m.Content,
				ContentText:   contentText,
				Timestamp:     ts,
				EditTimestamp: editTS,
				HasAttachment: hasAttachment,
				HasImage:      hasImage,
				HasLink:       hasLink,
				Reactions:     json.RawMessage(m.Reactions),
				IsMeMessage:   m.IsMe,
			})
			if m.ID > maxID {
				maxID = m.ID
			}
		}

		if err := s.store.UpsertMessageBatch(ctx, batch); err != nil {
			return fmt.Errorf("batch upsert (%d msgs): %w", len(batch), err)
		}

		lastID := resp.Messages[len(resp.Messages)-1].ID
		anchor = fmt.Sprintf("%d", lastID+1)
		if resp.FoundNewest {
			break
		}
	}

	for tid := range touched {
		if err := s.store.RecomputeTopicStats(ctx, tid); err != nil {
			return err
		}
	}
	if maxID > 0 {
		if err := s.store.UpdateSyncState(ctx, s.orgID, privateStreamID, maxID); err != nil {
			return err
		}
	}
	return nil
}

func filterStreams(all []zulip.Stream, includeCfg, excludeCfg, cliInclude []string) []zulip.Stream {
	include := make(map[string]bool)
	exclude := make(map[string]bool)
	for _, s := range excludeCfg {
		exclude[strings.ToLower(strings.TrimSpace(s))] = true
	}
	activeInclude := cliInclude
	if len(activeInclude) == 0 {
		activeInclude = includeCfg
	}
	for _, s := range activeInclude {
		if s = strings.ToLower(strings.TrimSpace(s)); s != "" {
			include[s] = true
		}
	}
	out := make([]zulip.Stream, 0, len(all))
	for _, st := range all {
		name := strings.ToLower(st.Name)
		if exclude[name] {
			continue
		}
		if len(include) > 0 && !include[name] {
			continue
		}
		out = append(out, st)
	}
	return out
}

// htmlToText strips HTML tags and returns clean plain text suitable for FTS
// indexing.  When apply_markdown=true (the default for this syncer), Zulip
// returns rendered HTML; this function reduces it to whitespace-normalised
// plain text.
func htmlToText(s string) string {
	n, err := html.Parse(strings.NewReader(s))
	if err != nil {
		// html.Parse is lenient and rarely errors; fall back to raw string.
		return strings.TrimSpace(s)
	}
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.TextNode {
			b.WriteString(node.Data)
			b.WriteByte(' ')
		}
		for c := node.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return strings.Join(strings.Fields(b.String()), " ")
}
