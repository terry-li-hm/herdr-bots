package herdr

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCommandTranscriptReadsTheCommandResultWindow(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-herdr")
	argvPath := filepath.Join(dir, "argv")
	script := `#!/bin/sh
for arg in "$@"; do
  printf '%s\n' "$arg" >> "` + argvPath + `"
done
printf '%s' 'transcript-body'
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}

	body, err := (&CLI{Bin: bin}).CommandTranscript(context.Background(), "p1")
	if err != nil {
		t.Fatalf("CommandTranscript: %v", err)
	}
	if body != "transcript-body" {
		t.Fatalf("body = %q want %q", body, "transcript-body")
	}

	raw, err := os.ReadFile(argvPath)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	want := []string{"pane", "read", "p1", "--source", "recent-unwrapped", "--lines", "200", "--format", "text"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v want %v", got, want)
	}
}
