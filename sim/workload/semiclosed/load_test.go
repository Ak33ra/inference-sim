package semiclosed

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadFixture(t *testing.T) *Corpus {
	t.Helper()
	c, err := Load("testdata/causal")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func copyFixture(t *testing.T, fixture string) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"manifest.json", "corpus.jsonl", "audit.jsonl"} {
		data, err := os.ReadFile(filepath.Join("testdata", fixture, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func rewriteArtifact(t *testing.T, dir, name string, edit func([]byte) []byte, resign bool) {
	t.Helper()
	path := filepath.Join(dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = edit(data)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if !resign || name == "manifest.json" {
		return
	}
	path = filepath.Join(dir, "manifest.json")
	meta, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(meta, &m); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	m["artifacts"].(map[string]any)[name] = map[string]any{"bytes": len(data), "sha256": hex.EncodeToString(hash[:])}
	meta, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, meta, 0600); err != nil {
		t.Fatal(err)
	}
}

func editJSONLine(t *testing.T, fn func(map[string]any)) func([]byte) []byte {
	t.Helper()
	return func(data []byte) []byte {
		lines := strings.Split(string(data), "\n")
		var row map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &row); err != nil {
			t.Fatal(err)
		}
		fn(row)
		b, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		lines[0] = string(b)
		return []byte(strings.Join(lines, "\n"))
	}
}

func TestLoadChecksArtifactAndSourceMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, artifact string
		resign         bool
		edit           func([]byte) []byte
	}{
		{"digest", "corpus.jsonl", false, func(b []byte) []byte { return append(b, '\n') }},
		{"request tokens", "corpus.jsonl", true, editJSONLine(t, func(m map[string]any) { m["requests"].([]any)[0].(map[string]any)["out"] = 0 })},
		{"request time", "corpus.jsonl", true, editJSONLine(t, func(m map[string]any) { m["requests"].([]any)[0].(map[string]any)["t"] = -1 })},
		{"prefix coverage", "corpus.jsonl", true, editJSONLine(t, func(m map[string]any) { m["requests"].([]any)[0].(map[string]any)["hash_ids"] = []int{1} })},
		{"duplicate trace", "corpus.jsonl", true, func(b []byte) []byte { return append(append([]byte(nil), b...), b...) }},
		{"missing session", "audit.jsonl", true, editJSONLine(t, func(m map[string]any) { m["requests"].([]any)[0].(map[string]any)["session_id"] = "missing" })},
		{"duplicate step", "audit.jsonl", true, editJSONLine(t, func(m map[string]any) { m["requests"].([]any)[1].(map[string]any)["step"] = 0 })},
		{"negative gap", "audit.jsonl", true, editJSONLine(t, func(m map[string]any) { m["requests"].([]any)[1].(map[string]any)["gap_from_previous_end_ms"] = -1 })},
		{"spawn", "audit.jsonl", true, editJSONLine(t, func(m map[string]any) { m["children"].([]any)[0].(map[string]any)["spawned_by_step"] = 3 })},
		{"spawn call order", "audit.jsonl", true, editJSONLine(t, func(m map[string]any) { m["children"].([]any)[0].(map[string]any)["spawned_by_call_index"] = 99 })},
		{"join", "audit.jsonl", true, editJSONLine(t, func(m map[string]any) { m["children"].([]any)[0].(map[string]any)["inferred_join_step"] = 3 })},
		{"totals", "manifest.json", false, func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"requests": 11`, `"requests": 12`, 1))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := copyFixture(t, "causal")
			rewriteArtifact(t, dir, tc.artifact, tc.edit, tc.resign)
			if _, err := Load(dir); err == nil {
				t.Fatal("corrupt conversion was accepted")
			}
		})
	}
	if got := loadFixture(t).totals; got.Families != 2 || got.Sessions != 6 || got.Requests != 11 || got.ZeroOutputRequests != 1 {
		t.Fatalf("wrong selected work: %+v", got)
	}
}
