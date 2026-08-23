package pane

import (
	"strings"
	"testing"
)

func TestTruncatePreservesUnicodeBoundaries(t *testing.T) {
	if got := truncate("排程結果", 3); got != "排程…" {
		t.Fatalf("got %q", got)
	}
}

func TestEmptyViewShowsBotInboxStrings(t *testing.T) {
	m := model{}
	view := m.View()
	if !strings.Contains(view, "Bot inbox") {
		t.Fatalf("empty view missing title %q", "Bot inbox")
	}
	if !strings.Contains(view, "No bot runs yet.") {
		t.Fatalf("empty view missing empty state %q", "No bot runs yet.")
	}
}
