package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/inference-sim/blis-schemas/spec/workload"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == childCommand {
		main()
		return
	}
	registerCorpus()
	os.Exit(m.Run())
}

func writeTestFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0644); err != nil {
		t.Fatal(err)
	}
}

func fixture(t *testing.T) (options, []byte, []byte) {
	t.Helper()
	dir := t.TempDir()
	input := filepath.Join(dir, "input")
	if err := os.Mkdir(input, 0755); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(input, "corpus.jsonl"), []byte("{}\n"))
	a, err := fingerprint(filepath.Join(input, "corpus.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	m := conversionManifest{FormatVersion: 1, Artifacts: map[string]artifact{"corpus.jsonl": a}}
	m.Source.Dataset, m.Source.Revision, m.Included.Families = dataset, "upstream-revision", 1
	manifest, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(input, "manifest.json"), manifest)
	template, err := os.ReadFile("../configs/workloads/agentx.yaml")
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "replay.yaml")
	writeTestFile(t, config, template)
	scenario := filepath.Join(dir, "scenario.yaml")
	writeTestFile(t, scenario, []byte("this is deliberately not a deployment\n"))
	defaults := filepath.Join(dir, "defaults.yaml")
	writeTestFile(t, defaults, []byte("{}\n"))
	return options{
		input: input, output: filepath.Join(dir, "output"), workload: config,
		scenario: scenario, defaults: defaults, catalog: dir, registry: dir,
		instances: 2, series: 1, changed: map[string]bool{},
	}, template, manifest
}

func TestResolvedWorkloadProvenanceAndOverrides(t *testing.T) {
	o, template, manifest := fixture(t)
	o.concurrency, o.seed, o.duration = 7, 99, 20*time.Minute
	o.changed = map[string]bool{"concurrency": true, "seed": true, "duration": true}
	r, _, err := resolveWorkload(template, manifest, o)
	if err != nil {
		t.Fatal(err)
	}
	if r.Corpus.Loader != corpusLoader || r.Corpus.Source.HuggingFace != dataset || r.Corpus.Source.File != "" || r.Corpus.Source.Revision != "upstream-revision" {
		t.Fatalf("incorrect derived-corpus provenance: %+v", r.Corpus)
	}
	if r.Concurrency != 7 || r.Seed() != 99 || r.Duration() != 1200 || r.SplitsFlattenedAgents() || r.IdleCap() != 10 {
		t.Fatalf("unexpected settings: %+v", r)
	}
	// Adding the research alias must not replace a standard loader's identity.
	if workload.AgentXLoaders["semianalysis_cc_traces_weka_062126_256k"] != "semianalysisai/cc-traces-weka-062126-256k" {
		t.Fatal("standard corpus alias changed")
	}
	p, err := prepare(o)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(p.args, " "), "--seed 99") || string(p.files["conversion-manifest.json"]) != string(manifest) {
		t.Fatalf("router seed or manifest snapshot missing: %v", p.args)
	}
}

func TestRejectsInvalidSettingsAndForeignProvenance(t *testing.T) {
	for _, name := range []string{"foreign-dataset", "missing-digest", "unknown-key", "extra-document", "corpus-in-template", "zero-concurrency", "short-duration", "disabled-idle-cap"} {
		t.Run(name, func(t *testing.T) {
			o, template, manifest := fixture(t)
			switch name {
			case "foreign-dataset":
				manifest = []byte(strings.ReplaceAll(string(manifest), dataset, "someone/another-dataset"))
			case "missing-digest":
				var m conversionManifest
				if err := json.Unmarshal(manifest, &m); err != nil {
					t.Fatal(err)
				}
				a := m.Artifacts["corpus.jsonl"]
				a.SHA256 = ""
				m.Artifacts["corpus.jsonl"] = a
				manifest, _ = json.Marshal(m)
			case "unknown-key":
				template = append(template, []byte("typo: true\n")...)
			case "extra-document":
				template = append(template, []byte("---\nconcurrency: 9\n")...)
			case "corpus-in-template":
				template = append(template, []byte("corpus:\n  loader: bogus\n")...)
			case "zero-concurrency":
				o.changed["concurrency"] = true
			case "short-duration":
				o.changed["duration"], o.duration = true, time.Minute
			case "disabled-idle-cap":
				template = []byte(strings.ReplaceAll(string(template), "system_idle_gap_cap_s: 10", "system_idle_gap_cap_s: 0"))
			}
			if _, _, err := resolveWorkload(template, manifest, o); err == nil {
				t.Fatal("accepted invalid input")
			}
		})
	}
}

func TestCorruptionAndOverwriteAreRejectedBeforeLaunch(t *testing.T) {
	o, _, _ := fixture(t)
	writeTestFile(t, filepath.Join(o.input, "corpus.jsonl"), []byte("changed\n"))
	if _, err := prepare(o); err == nil || !strings.Contains(err.Error(), "digest differ") {
		t.Fatalf("corrupt corpus: %v", err)
	}
	if _, err := os.Stat(o.output); !os.IsNotExist(err) {
		t.Fatalf("corruption created output: %v", err)
	}
	if err := os.Mkdir(o.output, 0755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(o.output, "keep")
	writeTestFile(t, marker, []byte("keep"))
	if _, err := prepare(o); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing output: %v", err)
	}
	if b, err := os.ReadFile(marker); err != nil || string(b) != "keep" {
		t.Fatalf("existing output changed: %q %v", b, err)
	}
}

func TestFailedBLISRunRetainsEvidence(t *testing.T) {
	o, _, _ := fixture(t)
	if err := run(context.Background(), o); err == nil {
		t.Fatal("invalid deployment succeeded")
	}
	b, err := os.ReadFile(filepath.Join(o.output, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var r runRecord
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	if r.Status != "failed" || r.Error == "" || r.ExitCode == nil || *r.ExitCode == 0 || r.FinishedAt == nil {
		t.Fatalf("missing failure evidence: %+v", r)
	}
	if r.Artifacts["stderr.log"].Bytes == 0 || r.Artifacts["workload.yaml"].SHA256 == "" || r.Executable.SHA256 == "" {
		t.Fatalf("missing artifact provenance: %+v", r.Artifacts)
	}
}
