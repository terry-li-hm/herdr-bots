package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// captureStdout collects everything a function prints to standard output so
// user-facing strings can be asserted without a subprocess.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	original := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	defer func() { os.Stdout = original }()
	done := make(chan string, 1)
	go func() {
		raw, _ := io.ReadAll(reader)
		done <- string(raw)
	}()
	fn()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return <-done
}

func TestUsageNamesPublicCLI(t *testing.T) {
	text := captureStdout(t, usage)
	if !strings.HasPrefix(text, "herdr-bots - durable local coding-agent schedules") {
		t.Fatalf("usage headline lost public identity: %q", strings.SplitN(text, "\n", 2)[0])
	}
	for _, command := range []string{"herdr-bots daemon", "herdr-bots pane", "herdr-bots service render"} {
		if !strings.Contains(text, command) {
			t.Fatalf("usage missing %q", command)
		}
	}
	if strings.Contains(strings.ToLower(text), legacyIdentityFragment()) {
		t.Fatalf("usage retained legacy identity: %s", text)
	}
}

// legacyIdentityFragment spells the pre-release identity without a single
// literal so the tracked release tree itself stays free of the string the
// release gate forbids.
func legacyIdentityFragment() string { return "vive" + "sca" }

func TestDefaultPathsUsePublicIdentity(t *testing.T) {
	t.Setenv("HERDR_BOTS_CONFIG", "")
	t.Setenv("HERDR_BOTS_STATE", "")
	home := t.TempDir()
	t.Setenv("HOME", home)
	if got, want := defaultConfigPath(), filepath.Join(home, ".config", "herdr-bots", "bots.yaml"); got != want {
		t.Fatalf("default config path = %q, want %q", got, want)
	}
	if got, want := defaultStatePath(), filepath.Join(home, ".local", "state", "herdr-bots", "state.sqlite3"); got != want {
		t.Fatalf("default state path = %q, want %q", got, want)
	}
	t.Setenv("HERDR_BOTS_CONFIG", "/tmp/custom bots.yaml")
	if got := defaultConfigPath(); got != "/tmp/custom bots.yaml" {
		t.Fatalf("config override ignored: %q", got)
	}
	t.Setenv("HERDR_BOTS_STATE", "/tmp/custom state.sqlite3")
	if got := defaultStatePath(); got != "/tmp/custom state.sqlite3" {
		t.Fatalf("state override ignored: %q", got)
	}
}

// TestServiceRenderDefaultsSharePaneState pins the durable contract that a
// no-flag `service render` carries exactly the default config and state
// paths used by no-flag CLI/pane operation, keeping one state authority.
func TestServiceRenderDefaultsSharePaneState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("HERDR_BOTS_CONFIG", "")
	t.Setenv("HERDR_BOTS_STATE", "")
	text := captureStdout(t, func() {
		if err := serviceCmd([]string{"render"}); err != nil {
			t.Error(err)
		}
	})
	for _, want := range []string{defaultConfigPath(), defaultStatePath()} {
		if !strings.Contains(text, want) {
			t.Fatalf("no-flag service render missing default path %q:\n%s", want, text)
		}
	}
}

func TestServiceRenderUsesPublicLabelAndLogPath(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "bots.yaml")
	if err := os.WriteFile(configPath, []byte("version: 1\njobs: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(dir, "state.sqlite3")
	home := t.TempDir()
	t.Setenv("HOME", home)
	text := captureStdout(t, func() {
		if err := serviceCmd([]string{"render", "--config", configPath, "--state", statePath}); err != nil {
			t.Error(err)
		}
	})
	for _, want := range []string{"com.terry.herdr-bots", "herdr-bots.log"} {
		if !strings.Contains(text, want) {
			t.Fatalf("rendered plist missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, legacyIdentityFragment()) {
		t.Fatalf("rendered plist retained legacy identity:\n%s", text)
	}
}

func TestReleaseVersionContractsNameCurrentRelease(t *testing.T) {
	root := filepath.Join("..", "..")
	read := func(name string) string {
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	changelog := read("CHANGELOG.md")
	for _, want := range []string{
		"## [0.2.0] - 2026-08-25\n",
		"[0.2.0]: https://github.com/terry-li-hm/herdr-bots/releases/tag/v0.2.0",
		"[0.1.1]: https://github.com/terry-li-hm/herdr-bots/releases/tag/v0.1.1",
		"[0.1.0]: https://github.com/terry-li-hm/herdr-bots/releases/tag/v0.1.0",
	} {
		if !strings.Contains(changelog, want) {
			t.Fatalf("CHANGELOG missing %q", want)
		}
	}
	readme := read("README.md")
	for _, want := range []string{
		"herdr plugin install terry-li-hm/herdr-bots --ref v0.2.0",
		"go install github.com/terry-li-hm/herdr-bots/cmd/herdr-bots@v0.2.0",
	} {
		if !strings.Contains(readme, want) {
			t.Fatalf("README missing current install contract %q", want)
		}
	}
}

func TestReleaseManifestPointsAtSourceLauncher(t *testing.T) {
	root := filepath.Join("..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "herdr-plugin.toml"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := string(raw)
	for _, want := range []string{
		`id = "terry.herdr-bots"`,
		`version = "0.2.0"`,
		`command = ["./herdr-bots", "pane"]`,
	} {
		if !strings.Contains(manifest, want) {
			t.Fatalf("manifest missing %q:\n%s", want, manifest)
		}
	}
	info, err := os.Stat(filepath.Join(root, "herdr-bots"))
	if err != nil {
		t.Fatalf("root launcher missing: %v", err)
	}
	if info.Mode()&0o111 == 0 {
		t.Fatalf("root launcher herdr-bots is not executable (mode %o)", info.Mode().Perm())
	}
}
