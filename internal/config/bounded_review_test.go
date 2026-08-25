package config

import (
	"fmt"
	"strings"
	"testing"
)

// boundedExecution is a minimal execution block that already validates, so a
// case can add exactly the bounded field under test and nothing else.
func boundedExecution(profile string) Execution {
	return Execution{
		Repository:        "/tmp/repo",
		Workspace:         WorkspaceWorktree,
		Harness:           HarnessPi,
		Provider:          "openai-codex",
		Model:             "gpt-5.6-sol",
		Thinking:          "high",
		PermissionProfile: profile,
	}
}

func TestBoundedBaseExecutionValidates(t *testing.T) {
	for _, profile := range []string{PermissionReadOnly, PermissionRepoWrite} {
		if err := boundedExecution(profile).validate("intake"); err != nil {
			t.Fatalf("base execution with %s must validate: %v", profile, err)
		}
	}
}

// TestOmittedBoundedFieldsPreserveSnapshot pins the compatibility contract: a
// job definition written before these fields existed must hash to what it
// hashed before, so adding the fields does not churn every stored revision.
func TestOmittedBoundedFieldsPreserveSnapshot(t *testing.T) {
	cfg, err := Load(writeConfig(t, validYAML()))
	if err != nil {
		t.Fatal(err)
	}
	job := cfg.Jobs[0]
	if job.Execution.HasBoundedInputs() || job.Execution.HasWriteScope() {
		t.Fatalf("omitted fields must not report bounded inputs or write scope: %+v", job.Execution)
	}

	raw, revision, err := job.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"inputs", "allowed_write_paths"} {
		if strings.Contains(string(raw), key) {
			t.Fatalf("omitted field %q must not appear in the snapshot: %s", key, raw)
		}
	}

	// An explicitly empty list is the same absence of scope as an omitted one,
	// so it must not produce a different revision either.
	job.Execution.Inputs = []InputSnapshot{}
	job.Execution.AllowedWritePaths = []string{}
	_, emptyRevision, err := job.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if emptyRevision != revision {
		t.Fatalf("empty lists changed the revision: %s != %s", emptyRevision, revision)
	}
}

// TestLoadAcceptsDatedInputAndReservedDestination goes through YAML rather than
// the struct so the field names an operator writes are covered too.
func TestLoadAcceptsDatedInputAndReservedDestination(t *testing.T) {
	body := strings.Replace(validYAML(), "      permission_profile: read-only-no-network\n", `      permission_profile: read-only-no-network
      inputs:
        - source: /var/reports/{year}/{month}/drift-{date}.csv
          destination: .herdr-bots/inputs/drift.csv
`, 1)
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatal(err)
	}
	execution := cfg.Jobs[0].Execution
	if !execution.HasBoundedInputs() || len(execution.Inputs) != 1 {
		t.Fatalf("inputs not decoded: %+v", execution.Inputs)
	}
	if got := execution.Inputs[0].Source; got != "/var/reports/{year}/{month}/drift-{date}.csv" {
		t.Fatalf("source = %q, placeholders must be preserved verbatim for expansion at run time", got)
	}
	if got := execution.Inputs[0].Destination; got != ".herdr-bots/inputs/drift.csv" {
		t.Fatalf("destination = %q", got)
	}
	if execution.HasWriteScope() {
		t.Fatalf("inputs must not imply a write scope: %+v", execution.AllowedWritePaths)
	}
}

func TestLoadAcceptsAllowedWritePaths(t *testing.T) {
	body := strings.Replace(validYAML(), "      permission_profile: read-only-no-network\n", `      permission_profile: repo-write-no-network
      allowed_write_paths:
        - docs/notes.md
        - reports/
`, 1)
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatal(err)
	}
	execution := cfg.Jobs[0].Execution
	if !execution.HasWriteScope() {
		t.Fatalf("allowed_write_paths not decoded: %+v", execution)
	}
	if got := execution.AllowedWritePaths; got[0] != "docs/notes.md" || got[1] != "reports/" {
		t.Fatalf("allowed_write_paths = %q", got)
	}
}

func TestValidInputs(t *testing.T) {
	cases := []struct {
		name   string
		inputs []InputSnapshot
	}{
		{"plain absolute source", []InputSnapshot{{Source: "/var/reports/drift.csv", Destination: ".herdr-bots/inputs/drift.csv"}}},
		{"dated source", []InputSnapshot{{Source: "/var/reports/{date}.csv", Destination: ".herdr-bots/inputs/drift.csv"}}},
		{"year and month source", []InputSnapshot{{Source: "/var/{year}/{month}/drift.csv", Destination: ".herdr-bots/inputs/drift.csv"}}},
		{"nested destination", []InputSnapshot{{Source: "/var/drift.csv", Destination: ".herdr-bots/inputs/weekly/drift.csv"}}},
		{"several distinct entries", []InputSnapshot{
			{Source: "/var/a.csv", Destination: ".herdr-bots/inputs/a.csv"},
			{Source: "/var/b.csv", Destination: ".herdr-bots/inputs/b.csv"},
			{Source: "/var/{date}/c.csv", Destination: ".herdr-bots/inputs/c.csv"},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			execution := boundedExecution(PermissionReadOnly)
			execution.Inputs = tc.inputs
			if err := execution.validate("intake"); err != nil {
				t.Fatalf("expected valid, got %v", err)
			}
			if !execution.HasBoundedInputs() {
				t.Fatal("HasBoundedInputs must report true")
			}
		})
	}
}

func TestInvalidInputs(t *testing.T) {
	cases := []struct {
		name   string
		input  InputSnapshot
		errHas string
	}{
		{"empty source", InputSnapshot{Source: "", Destination: ".herdr-bots/inputs/a.csv"}, "source must not be empty"},
		{"NUL in source", InputSnapshot{Source: "/var/a\x00.csv", Destination: ".herdr-bots/inputs/a.csv"}, "source must not contain NUL"},
		{"relative source", InputSnapshot{Source: "var/a.csv", Destination: ".herdr-bots/inputs/a.csv"}, "source must be an absolute path"},
		{"relative dated source", InputSnapshot{Source: "{date}/a.csv", Destination: ".herdr-bots/inputs/a.csv"}, "source must be an absolute path"},
		{"home-relative source", InputSnapshot{Source: "~/reports/a.csv", Destination: ".herdr-bots/inputs/a.csv"}, "source must be an absolute path"},
		{"unknown placeholder", InputSnapshot{Source: "/var/{week}.csv", Destination: ".herdr-bots/inputs/a.csv"}, "placeholders {date}, {year}, and {month}"},
		{"unbalanced open brace", InputSnapshot{Source: "/var/{date.csv", Destination: ".herdr-bots/inputs/a.csv"}, "placeholders {date}, {year}, and {month}"},
		{"unbalanced close brace", InputSnapshot{Source: "/var/date}.csv", Destination: ".herdr-bots/inputs/a.csv"}, "placeholders {date}, {year}, and {month}"},
		{"nested placeholder", InputSnapshot{Source: "/var/{da{date}te}.csv", Destination: ".herdr-bots/inputs/a.csv"}, "placeholders {date}, {year}, and {month}"},
		{"environment style placeholder", InputSnapshot{Source: "/var/${HOME}/a.csv", Destination: ".herdr-bots/inputs/a.csv"}, "placeholders {date}, {year}, and {month}"},
		{"traversal in source", InputSnapshot{Source: "/var/reports/../../etc/passwd", Destination: ".herdr-bots/inputs/a.csv"}, "source must not contain .."},
		{"traversal beside a placeholder", InputSnapshot{Source: "/var/{date}/../../etc/passwd", Destination: ".herdr-bots/inputs/a.csv"}, "source must not contain .."},

		{"empty destination", InputSnapshot{Source: "/var/a.csv", Destination: ""}, "destination must not be empty"},
		{"NUL in destination", InputSnapshot{Source: "/var/a.csv", Destination: ".herdr-bots/inputs/a\x00.csv"}, "destination must not contain NUL"},
		{"placeholder in destination", InputSnapshot{Source: "/var/a.csv", Destination: ".herdr-bots/inputs/{date}.csv"}, "destination must not use placeholders"},
		{"absolute destination", InputSnapshot{Source: "/var/a.csv", Destination: "/etc/passwd"}, "destination must be relative"},
		{"absolute destination under the reserved name", InputSnapshot{Source: "/var/a.csv", Destination: "/.herdr-bots/inputs/a.csv"}, "destination must be relative"},
		{"traversal in destination", InputSnapshot{Source: "/var/a.csv", Destination: ".herdr-bots/inputs/../../a.csv"}, "destination must not contain .."},
		{"traversal that cleans back inside", InputSnapshot{Source: "/var/a.csv", Destination: ".herdr-bots/inputs/sub/../a.csv"}, "destination must not contain .."},
		{"unclean destination", InputSnapshot{Source: "/var/a.csv", Destination: ".herdr-bots/inputs/./a.csv"}, "destination must be a clean relative path"},
		{"double slash in destination", InputSnapshot{Source: "/var/a.csv", Destination: ".herdr-bots/inputs//a.csv"}, "destination must be a clean relative path"},
		{"trailing slash destination", InputSnapshot{Source: "/var/a.csv", Destination: ".herdr-bots/inputs/sub/"}, "destination must be a clean relative path"},
		{"destination is the reserved directory", InputSnapshot{Source: "/var/a.csv", Destination: ".herdr-bots/inputs"}, "not the directory itself"},
		{"destination outside the reserved directory", InputSnapshot{Source: "/var/a.csv", Destination: "docs/a.csv"}, "destination must be under .herdr-bots/inputs/"},
		{"destination in a sibling of the reserved directory", InputSnapshot{Source: "/var/a.csv", Destination: ".herdr-bots/inputsX/a.csv"}, "destination must be under .herdr-bots/inputs/"},
		{"destination in the reserved parent", InputSnapshot{Source: "/var/a.csv", Destination: ".herdr-bots/a.csv"}, "destination must be under .herdr-bots/inputs/"},
		{"destination in .git", InputSnapshot{Source: "/var/a.csv", Destination: ".git/config"}, "destination must be under .herdr-bots/inputs/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			execution := boundedExecution(PermissionReadOnly)
			execution.Inputs = []InputSnapshot{tc.input}
			err := execution.validate("intake")
			if err == nil {
				t.Fatalf("expected rejection of %+v", tc.input)
			}
			if !strings.Contains(err.Error(), tc.errHas) {
				t.Fatalf("error %q does not mention %q", err, tc.errHas)
			}
		})
	}
}

func TestInputsRejectDuplicates(t *testing.T) {
	cases := []struct {
		name   string
		inputs []InputSnapshot
		errHas string
	}{
		{
			"duplicate source",
			[]InputSnapshot{
				{Source: "/var/a.csv", Destination: ".herdr-bots/inputs/a.csv"},
				{Source: "/var/a.csv", Destination: ".herdr-bots/inputs/b.csv"},
			},
			`duplicate source "/var/a.csv"`,
		},
		{
			"duplicate dated source",
			[]InputSnapshot{
				{Source: "/var/{date}.csv", Destination: ".herdr-bots/inputs/a.csv"},
				{Source: "/var/{date}.csv", Destination: ".herdr-bots/inputs/b.csv"},
			},
			"duplicate source",
		},
		{
			"duplicate destination",
			[]InputSnapshot{
				{Source: "/var/a.csv", Destination: ".herdr-bots/inputs/same.csv"},
				{Source: "/var/b.csv", Destination: ".herdr-bots/inputs/same.csv"},
			},
			`duplicate destination ".herdr-bots/inputs/same.csv"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			execution := boundedExecution(PermissionReadOnly)
			execution.Inputs = tc.inputs
			err := execution.validate("intake")
			if err == nil || !strings.Contains(err.Error(), tc.errHas) {
				t.Fatalf("error %v does not mention %q", err, tc.errHas)
			}
		})
	}
}

func TestInputCountBound(t *testing.T) {
	build := func(n int) []InputSnapshot {
		inputs := make([]InputSnapshot, 0, n)
		for i := 0; i < n; i++ {
			inputs = append(inputs, InputSnapshot{
				Source:      fmt.Sprintf("/var/reports/%d.csv", i),
				Destination: fmt.Sprintf(".herdr-bots/inputs/%d.csv", i),
			})
		}
		return inputs
	}

	atLimit := boundedExecution(PermissionReadOnly)
	atLimit.Inputs = build(MaxBoundedInputs)
	if err := atLimit.validate("intake"); err != nil {
		t.Fatalf("%d inputs must be accepted: %v", MaxBoundedInputs, err)
	}

	overLimit := boundedExecution(PermissionReadOnly)
	overLimit.Inputs = build(MaxBoundedInputs + 1)
	err := overLimit.validate("intake")
	if err == nil || !strings.Contains(err.Error(), "at most 32 entries") {
		t.Fatalf("%d inputs must be rejected, got %v", MaxBoundedInputs+1, err)
	}
}

func TestValidAllowedWritePaths(t *testing.T) {
	cases := []struct {
		name  string
		paths []string
	}{
		{"exact file", []string{"docs/notes.md"}},
		{"exact file at the repository top level", []string{"CHANGELOG.md"}},
		{"directory prefix", []string{"reports/"}},
		{"nested directory prefix", []string{"docs/generated/"}},
		{"dotfile that only looks like .git", []string{".gitignore"}},
		{"directory prefix that only looks like .git", []string{".github/"}},
		{"reserved parent sibling", []string{".herdr-bots/summary.md"}},
		{"prefix beside the reserved inputs directory", []string{".herdr-bots/reports/"}},
		{"mixed exact and prefix entries", []string{"docs/notes.md", "reports/", "CHANGELOG.md"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			execution := boundedExecution(PermissionRepoWrite)
			execution.AllowedWritePaths = tc.paths
			if err := execution.validate("intake"); err != nil {
				t.Fatalf("expected valid, got %v", err)
			}
			if !execution.HasWriteScope() {
				t.Fatal("HasWriteScope must report true")
			}
		})
	}
}

func TestInvalidAllowedWritePaths(t *testing.T) {
	cases := []struct {
		name   string
		path   string
		errHas string
	}{
		{"empty", "", "must not be empty"},
		{"NUL", "docs/no\x00tes.md", "must not contain NUL"},
		{"absolute", "/etc/hosts", "must be relative"},
		{"absolute directory prefix", "/etc/", "must be relative"},
		{"dot", ".", "not the repository root"},
		{"dot with trailing slash", "./", "not the repository root"},
		{"slash only", "/", "must be relative"},
		{"traversal", "../outside.md", "must not contain .."},
		{"traversal that cleans back inside", "docs/../notes.md", "must not contain .."},
		{"traversal prefix", "docs/../../", "must not contain .."},
		{"unclean leading dot", "./docs/notes.md", "must be a clean relative path"},
		{"double slash", "docs//notes.md", "must be a clean relative path"},
		{"double trailing slash", "reports//", "must be a clean relative path"},
		{"star wildcard", "docs/*.md", "wildcard characters"},
		{"question wildcard", "docs/?.md", "wildcard characters"},
		{"bracket wildcard", "docs/[a-z].md", "wildcard characters"},
		{"bare star", "*", "wildcard characters"},
		{"git directory", ".git", "must not grant writes to .git"},
		{"git directory prefix", ".git/", "must not grant writes to .git"},
		{"git descendant", ".git/config", "must not grant writes to .git"},
		{"git hooks prefix", ".git/hooks/", "must not grant writes to .git"},
		{"reserved inputs directory", ".herdr-bots/inputs", "immutable staged inputs"},
		{"reserved inputs prefix", ".herdr-bots/inputs/", "immutable staged inputs"},
		{"reserved inputs descendant", ".herdr-bots/inputs/drift.csv", "immutable staged inputs"},
		{"prefix above the reserved inputs directory", ".herdr-bots/", "immutable staged inputs"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			execution := boundedExecution(PermissionRepoWrite)
			execution.AllowedWritePaths = []string{tc.path}
			err := execution.validate("intake")
			if err == nil {
				t.Fatalf("expected rejection of %q", tc.path)
			}
			if !strings.Contains(err.Error(), tc.errHas) {
				t.Fatalf("error %q does not mention %q", err, tc.errHas)
			}
		})
	}
}

// TestAllowedWritePathsRequireRepoWrite pins that a write scope is meaningless
// without writes: declaring one on a read-only job is a contradiction the
// operator must resolve, not something to silently ignore.
func TestAllowedWritePathsRequireRepoWrite(t *testing.T) {
	execution := boundedExecution(PermissionReadOnly)
	execution.AllowedWritePaths = []string{"docs/notes.md"}
	err := execution.validate("intake")
	if err == nil || !strings.Contains(err.Error(), "only valid with the repo-write-no-network permission profile") {
		t.Fatalf("expected read-only rejection, got %v", err)
	}

	// The same list is fine once the job can actually write.
	execution.PermissionProfile = PermissionRepoWrite
	if err := execution.validate("intake"); err != nil {
		t.Fatalf("repo-write must accept the same list: %v", err)
	}
}

func TestAllowedWritePathCountBound(t *testing.T) {
	build := func(n int) []string {
		paths := make([]string, 0, n)
		for i := 0; i < n; i++ {
			paths = append(paths, fmt.Sprintf("docs/notes-%d.md", i))
		}
		return paths
	}

	atLimit := boundedExecution(PermissionRepoWrite)
	atLimit.AllowedWritePaths = build(MaxAllowedWritePaths)
	if err := atLimit.validate("intake"); err != nil {
		t.Fatalf("%d write paths must be accepted: %v", MaxAllowedWritePaths, err)
	}

	overLimit := boundedExecution(PermissionRepoWrite)
	overLimit.AllowedWritePaths = build(MaxAllowedWritePaths + 1)
	err := overLimit.validate("intake")
	if err == nil || !strings.Contains(err.Error(), "at most 64 entries") {
		t.Fatalf("%d write paths must be rejected, got %v", MaxAllowedWritePaths+1, err)
	}
}

func TestBoundedFieldsTravelInTheSnapshot(t *testing.T) {
	cfg, err := Load(writeConfig(t, validYAML()))
	if err != nil {
		t.Fatal(err)
	}
	job := cfg.Jobs[0]
	_, bare, err := job.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	job.Execution.Inputs = []InputSnapshot{{Source: "/var/a.csv", Destination: ".herdr-bots/inputs/a.csv"}}
	raw, withInputs, err := job.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if withInputs == bare {
		t.Fatal("declaring inputs must change the job revision")
	}
	if !strings.Contains(string(raw), `"destination":".herdr-bots/inputs/a.csv"`) {
		t.Fatalf("snapshot must record the declared destination: %s", raw)
	}
}
