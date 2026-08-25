package adapter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

const (
	testMarker = "HERDR_BOTS_RUN_X"
	testModel  = "claude-opus-5"
)

// resultLine builds a realistic one-line Claude CLI result JSON payload.
func resultLine(canonicalModel, provider string, inputTokens, outputTokens int) string {
	return resultLineWithUsage(map[string]any{
		canonicalModelKeyOrDefault(canonicalModel): usageEntry(canonicalModel, provider, inputTokens, outputTokens),
	})
}

// resultLineWithUsage builds a result payload with an explicit modelUsage map,
// so a test can control the keys independently of the canonical models.
func resultLineWithUsage(modelUsage map[string]any) string {
	payload := map[string]any{
		"type":       "result",
		"subtype":    "success",
		"is_error":   false,
		"result":     "done",
		"modelUsage": modelUsage,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func usageEntry(canonicalModel, provider string, inputTokens, outputTokens int) map[string]any {
	usage := map[string]any{
		"inputTokens":  inputTokens,
		"outputTokens": outputTokens,
		"provider":     provider,
	}
	if canonicalModel != "" {
		usage["canonicalModel"] = canonicalModel
	}
	return usage
}

func canonicalModelKeyOrDefault(canonicalModel string) string {
	if canonicalModel == "" {
		return testModel
	}
	return canonicalModel
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

type attestation struct {
	Version       int    `json:"version"`
	ExpectedModel string `json:"expected_model"`
	ObservedModel string `json:"observed_model"`
	Provider      string `json:"provider"`
	Verdict       string `json:"verdict"`
	ResultSHA256  string `json:"result_sha256"`
}

func TestParseClaudeModelAttestationSuccess(t *testing.T) {
	line := resultLine(testModel, "firstParty", 1200, 340)
	transcript := "some preamble\n" + line + "\n" + testMarker + ":0\n"

	raw, err := ParseClaudeModelAttestation(transcript, testMarker, testModel)
	if err != nil {
		t.Fatalf("ParseClaudeModelAttestation() error = %v, want nil", err)
	}

	var got attestation
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("unmarshal attestation: %v", err)
	}

	want := attestation{
		Version:       1,
		ExpectedModel: testModel,
		ObservedModel: testModel,
		Provider:      "firstParty",
		Verdict:       "attested",
		ResultSHA256:  sha256Hex(line),
	}
	if got != want {
		t.Errorf("attestation = %+v, want %+v", got, want)
	}
}

// Real Claude results also bill internal Haiku usage, so an auxiliary entry
// alongside the configured model must still attest.
func TestParseClaudeModelAttestationAllowsAuxiliaryUsage(t *testing.T) {
	line := resultLineWithUsage(map[string]any{
		testModel:          usageEntry(testModel, "firstParty", 1200, 340),
		"claude-haiku-4-5": usageEntry("claude-haiku-4-5", "firstParty", 90, 12),
	})
	transcript := line + "\n" + testMarker + ":0\n"

	raw, err := ParseClaudeModelAttestation(transcript, testMarker, testModel)
	if err != nil {
		t.Fatalf("ParseClaudeModelAttestation() error = %v, want nil", err)
	}

	var got attestation
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("unmarshal attestation: %v", err)
	}

	want := attestation{
		Version:       1,
		ExpectedModel: testModel,
		ObservedModel: testModel,
		Provider:      "firstParty",
		Verdict:       "attested",
		ResultSHA256:  sha256Hex(line),
	}
	if got != want {
		t.Errorf("attestation = %+v, want %+v", got, want)
	}
}

func TestParseClaudeModelAttestationLastResultLineWins(t *testing.T) {
	stale := resultLine(testModel, "firstParty", 10, 20)
	winner := resultLine(testModel, "firstParty", 999, 111)
	transcript := stale + "\n" +
		`{"type":"assistant","message":{"role":"assistant"}}` + "\n" +
		winner + "\n" +
		testMarker + ":0\n"

	raw, err := ParseClaudeModelAttestation(transcript, testMarker, testModel)
	if err != nil {
		t.Fatalf("ParseClaudeModelAttestation() error = %v, want nil", err)
	}

	var got attestation
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("unmarshal attestation: %v", err)
	}
	if got.ResultSHA256 != sha256Hex(winner) {
		t.Errorf("result_sha256 = %q, want hash of last result line %q", got.ResultSHA256, sha256Hex(winner))
	}
	if got.ResultSHA256 == sha256Hex(stale) {
		t.Error("result_sha256 matched the stale result line")
	}
}

func TestParseClaudeModelAttestationErrors(t *testing.T) {
	validLine := resultLine(testModel, "firstParty", 1200, 340)

	tests := []struct {
		name       string
		transcript string
	}{
		{
			name:       "model mismatch",
			transcript: resultLine("claude-sonnet-5", "firstParty", 1200, 340) + "\n" + testMarker + ":0\n",
		},
		{
			name:       "malformed json",
			transcript: `{"type":"result","modelUsage":{` + "\n" + testMarker + ":0\n",
		},
		{
			name:       "zero usage",
			transcript: resultLine(testModel, "firstParty", 0, 0) + "\n" + testMarker + ":0\n",
		},
		{
			name:       "wrong provider",
			transcript: resultLine(testModel, "thirdParty", 1200, 340) + "\n" + testMarker + ":0\n",
		},
		{
			name:       "missing canonical model",
			transcript: resultLine("", "firstParty", 1200, 340) + "\n" + testMarker + ":0\n",
		},
		{
			name: "canonical model contradicts the usage key",
			transcript: resultLineWithUsage(map[string]any{
				testModel: usageEntry("claude-sonnet-5", "firstParty", 1200, 340),
			}) + "\n" + testMarker + ":0\n",
		},
		{
			name:       "missing marker",
			transcript: "preamble\n" + validLine + "\n",
		},
		{
			name:       "result only after marker",
			transcript: testMarker + ":0\n" + validLine + "\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := ParseClaudeModelAttestation(tt.transcript, testMarker, testModel)
			if err == nil {
				t.Fatalf("ParseClaudeModelAttestation() = %s, want error", raw)
			}
			if raw != "" {
				t.Errorf("attestation = %s, want empty on error", raw)
			}
		})
	}
}
