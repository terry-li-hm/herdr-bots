package service

import (
	"strings"
	"testing"
)

func TestRenderLaunchdRejectsRelativePersistentPaths(t *testing.T) {
	_, err := RenderLaunchd(LaunchdConfig{Binary: "/tmp/bin", ConfigPath: "relative.yaml", StatePath: "/tmp/state.db", LogPath: "/tmp/run.log", Home: "/Users/test", Path: "/usr/bin:/bin"})
	if err == nil {
		t.Fatal("relative launchd paths should be rejected")
	}
}

func TestRenderLaunchdSupervisesDaemonWithoutShell(t *testing.T) {
	raw, err := RenderLaunchd(LaunchdConfig{Binary: "/tmp/a&b", ConfigPath: "/tmp/config.yaml", StatePath: "/tmp/state.db", LogPath: "/tmp/run.log", Home: "/Users/test", Path: "/usr/bin:/bin"})
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, want := range []string{"com.terry.herdr-bots", "<string>/tmp/a&amp;b</string>", "<string>daemon</string>", "<key>KeepAlive</key>", "<key>SuccessfulExit</key>", "<false/>"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in %s", want, text)
		}
	}
	if strings.Contains(text, "/bin/sh") || strings.Contains(text, "startup") {
		t.Fatalf("unexpected unsupervised wrapper: %s", text)
	}
}
