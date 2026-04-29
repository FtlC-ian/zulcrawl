package syncer

import (
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/net/html"

	"github.com/FtlC-ian/zulcrawl/internal/store"
)

const maxInlineAttachmentTextBytes = 64 * 1024

var uploadHrefRE = regexp.MustCompile(`(?i)^/user_uploads/`)

// parseMessageIndexables extracts lightweight structured data from Zulip's
// rendered message HTML. It intentionally never fetches remote content; only
// inline text already present in the rendered HTML is indexed.
func parseMessageIndexables(messageID, orgID, streamID, topicID int64, timestamp, renderedHTML string) ([]store.Mention, []store.Attachment) {
	n, err := html.Parse(strings.NewReader(renderedHTML))
	if err != nil {
		return nil, nil
	}

	mentionsByKey := map[string]store.Mention{}
	attachmentsByURL := map[string]store.Attachment{}

	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.ElementNode {
			if isMentionNode(node) {
				mention := store.Mention{
					MessageID: messageID,
					OrgID:     orgID,
					StreamID:  streamID,
					TopicID:   topicID,
					Timestamp: timestamp,
					Name:      strings.TrimPrefix(strings.TrimSpace(nodeText(node)), "@"),
					Kind:      mentionKind(node),
				}
				mention.UserID = mentionUserID(node)
				if mention.Name != "" || mention.UserID != 0 {
					key := mention.Kind + "|" + mention.Name + "|" + strconv.FormatInt(mention.UserID, 10)
					mentionsByKey[key] = mention
				}
			}

			if isAttachmentLink(node) {
				att := store.Attachment{
					MessageID: messageID,
					OrgID:     orgID,
					StreamID:  streamID,
					TopicID:   topicID,
					Timestamp: timestamp,
					URL:       attr(node, "href"),
					Title:     strings.TrimSpace(nodeText(node)),
				}
				att.FileName = attachmentFileName(att.URL, att.Title)
				att.ContentType = contentTypeFromName(att.FileName)
				att.Text = boundedText(textNearAttachment(node), maxInlineAttachmentTextBytes)
				att.Indexed = att.Text != ""
				attachmentsByURL[att.URL] = att
			}
		}
		for c := node.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)

	mentions := make([]store.Mention, 0, len(mentionsByKey))
	for _, m := range mentionsByKey {
		mentions = append(mentions, m)
	}
	attachments := make([]store.Attachment, 0, len(attachmentsByURL))
	for _, a := range attachmentsByURL {
		attachments = append(attachments, a)
	}
	return mentions, attachments
}

func isMentionNode(n *html.Node) bool {
	if n.Data != "span" && n.Data != "a" {
		return false
	}
	cls := " " + strings.ToLower(attr(n, "class")) + " "
	return strings.Contains(cls, " user-mention ") || strings.Contains(cls, " user-group-mention ")
}

func mentionKind(n *html.Node) string {
	cls := strings.ToLower(attr(n, "class"))
	if strings.Contains(cls, "user-group-mention") {
		return "group"
	}
	return "user"
}

func mentionUserID(n *html.Node) int64 {
	for _, name := range []string{"data-user-id", "data-user-group-id", "data-id"} {
		if v := strings.TrimSpace(attr(n, name)); v != "" {
			if id, err := strconv.ParseInt(v, 10, 64); err == nil {
				return id
			}
		}
	}
	return 0
}

func isAttachmentLink(n *html.Node) bool {
	if n.Data != "a" {
		return false
	}
	href := strings.TrimSpace(attr(n, "href"))
	if href == "" || !uploadHrefRE.MatchString(href) {
		return false
	}
	cls := strings.ToLower(attr(n, "class"))
	return strings.Contains(cls, "message_inline_ref") || isSmallTextLikeName(attachmentFileName(href, nodeText(n)))
}

func textNearAttachment(n *html.Node) string {
	// Zulip rendered uploads usually put the link and any preview text inside a
	// small wrapper. Index only that already-rendered text, not fetched content.
	for p := n.Parent; p != nil; p = p.Parent {
		if p.Type == html.ElementNode {
			cls := strings.ToLower(attr(p, "class"))
			if strings.Contains(cls, "message_inline_ref") || strings.Contains(cls, "message_inline_image") || p.Data == "p" || p.Data == "blockquote" {
				text := strings.TrimSpace(nodeText(p))
				if isSmallTextLikeName(attachmentFileName(attr(n, "href"), text)) {
					return text
				}
			}
		}
	}
	return ""
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if strings.EqualFold(a.Key, key) {
			return a.Val
		}
	}
	return ""
}

func nodeText(n *html.Node) string {
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

func attachmentFileName(rawURL, title string) string {
	if u, err := url.Parse(rawURL); err == nil {
		if base := path.Base(u.Path); base != "." && base != "/" && base != "" {
			return base
		}
	}
	return strings.TrimSpace(title)
}

func contentTypeFromName(name string) string {
	switch strings.ToLower(path.Ext(name)) {
	case ".txt", ".log", ".md", ".markdown", ".json", ".yaml", ".yml", ".csv", ".tsv", ".xml", ".html", ".htm", ".go", ".py", ".js", ".ts", ".css", ".sh", ".sql":
		return "text/plain"
	default:
		return ""
	}
}

func isSmallTextLikeName(name string) bool {
	return contentTypeFromName(name) != ""
}

func boundedText(text string, maxBytes int) string {
	text = strings.TrimSpace(text)
	if len(text) <= maxBytes {
		return text
	}
	return strings.TrimSpace(text[:maxBytes])
}
