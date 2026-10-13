package syncer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/FtlC-ian/zulcrawl/internal/zulip"
)

const repairPageBudget = 5

func nextAnchor(anchor string, messages []zulip.Message, newest bool) (string, error) {
	if len(messages) == 0 {
		if !newest {
			return "", fmt.Errorf("pagination stalled: empty page without found_newest (anchor=%s)", anchor)
		}
		return anchor, nil
	}
	for i, m := range messages {
		if m.ID <= 0 || m.ID == math.MaxInt64 || (i > 0 && m.ID <= messages[i-1].ID) {
			return "", fmt.Errorf("pagination stalled: unordered or invalid message IDs (anchor=%s)", anchor)
		}
	}
	last := messages[len(messages)-1].ID
	if previous, err := strconv.ParseInt(anchor, 10, 64); err == nil && last < previous {
		return "", fmt.Errorf("pagination stalled: page did not reach anchor %s (last_id=%d)", anchor, last)
	}
	if previous, err := strconv.ParseInt(anchor, 10, 64); err == nil && last == previous && !newest {
		return "", fmt.Errorf("pagination stalled: non-advancing anchor %s", anchor)
	}
	return strconv.FormatInt(last+1, 10), nil
}

func (s *Syncer) trackedSync(ctx context.Context, stream int64, run func() error) (err error) {
	if err = s.store.BeginSync(ctx, s.orgID, stream); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err = errors.Join(err, s.store.FinishSync(cleanup, s.orgID, stream, err))
	}()
	return run()
}

func (s *Syncer) repair(ctx context.Context, stream int64, run func(Options) error, opts Options) error {
	if opts.Full || opts.Since != "" {
		return nil
	}
	last, err := s.store.LastMessageID(ctx, s.orgID, stream)
	if err != nil {
		return err
	}
	if last == 0 {
		return nil
	}
	anchor, err := s.store.RepairAnchor(ctx, s.orgID, stream)
	if err != nil {
		return err
	}
	if anchor == "" {
		return nil
	}
	s.logger.log("stream %d: reconciling history (anchor=%s, budget=%d pages)", stream, anchor, repairPageBudget)
	return run(Options{repair: true, repairAnchor: anchor})
}
