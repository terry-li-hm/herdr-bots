package engine

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
)

func TestBoundedRepositoryChangesIncludesIgnoredFiles(t *testing.T) {
	root := t.TempDir()
	initGitRepo(t, root)

	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("ignored/\n"), 0o644); err != nil {
		t.Fatalf("write .gitignore: %v", err)
	}
	if out, err := exec.Command("git", "-C", root, "add", ".gitignore").CombinedOutput(); err != nil {
		t.Fatalf("git add .gitignore: %v: %s", err, out)
	}
	if out, err := exec.Command("git", "-C", root, "commit", "-m", "ignore").CombinedOutput(); err != nil {
		t.Fatalf("git commit: %v: %s", err, out)
	}

	if err := os.MkdirAll(filepath.Join(root, "ignored"), 0o755); err != nil {
		t.Fatalf("mkdir ignored: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "ignored", "secret.txt"), []byte("secret\n"), 0o644); err != nil {
		t.Fatalf("write ignored/secret.txt: %v", err)
	}

	changed, untracked, err := boundedRepositoryChanges(context.Background(), root)
	if err != nil {
		t.Fatalf("boundedRepositoryChanges: %v", err)
	}
	if !slices.Contains(changed, "ignored/secret.txt") {
		t.Errorf("changed = %v, want to contain ignored/secret.txt", changed)
	}
	if !slices.Contains(untracked, "ignored/secret.txt") {
		t.Errorf("untracked = %v, want to contain ignored/secret.txt", untracked)
	}
}

func TestValidateRepositoryPathGitDirectory(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{name: "git directory", path: ".git", wantErr: true},
		{name: "git config", path: ".git/config", wantErr: true},
		{name: "github workflow", path: ".github/workflows/ci.yml", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateRepositoryPath(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Errorf("validateRepositoryPath(%q) error = nil, want error", tt.path)
				}
				return
			}
			if err != nil {
				t.Errorf("validateRepositoryPath(%q) error = %v, want nil", tt.path, err)
			}
			if got != tt.path {
				t.Errorf("validateRepositoryPath(%q) = %q, want %q", tt.path, got, tt.path)
			}
		})
	}
}
