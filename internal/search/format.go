package search

import (
	"fmt"
	"strings"
	"time"

	"github.com/debbie/zulcrawl/internal/store"
)

func FormatHits(hits []store.SearchHit) string {
	if len(hits) == 0 {
		return "No results."
	}
	var b strings.Builder
	for _, h := range hits {
		timeText := h.Timestamp
		if t, err := time.Parse(time.RFC3339, h.Timestamp); err == nil {
			timeText = t.Format("2006-01-02")
		}
		resolved := ""
		if h.Resolved {
			resolved = " [resolved]"
		}
		fmt.Fprintf(&b, "#%s > %s (%s)%s\n", h.StreamName, h.TopicName, timeText, resolved)
		fmt.Fprintf(&b, "  %s: %s\n\n", h.SenderName, h.Snippet)
	}
	return strings.TrimSpace(b.String())
}
