package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	latencykernel "github.com/inference-sim/blis-latency-kernel"
	"github.com/inference-sim/blis-schemas/spec/deployment"
	"gopkg.in/yaml.v3"
)

func experimentFixture(t *testing.T, o options) (string, []byte) {
	t.Helper()
	dir := filepath.Join(filepath.Dir(o.input), "configs")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	a, err := fingerprint(filepath.Join(o.input, "corpus.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	series := 2.0
	c := experimentConfig{
		Version: 1, Input: "../input", CorpusSHA256: a.SHA256,
		Scenario: "../scenario.yaml", Workload: "../replay.yaml", Defaults: "../defaults.yaml",
		Instances: 4, Concurrency: 8, Seed: 17, Duration: "30m", Series: &series,
	}
	b, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "experiment.yaml")
	writeTestFile(t, path, b)
	return path, b
}

func TestExperimentReferencesOverridesAndSnapshot(t *testing.T) {
	o, template, manifest := fixture(t)
	config, b := experimentFixture(t, o)
	o.config = config
	o.concurrency = 3
	o.changed["concurrency"] = true
	resolved, saved, err := withExperiment(o)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.input != o.input || resolved.scenario != o.scenario || resolved.workload != o.workload || resolved.defaults != o.defaults {
		t.Fatalf("references were not relative to the experiment: %+v", resolved)
	}
	if resolved.instances != 4 || resolved.series != 2 || resolved.duration != 30*time.Minute || resolved.concurrency != 3 || resolved.seed != 17 {
		t.Fatalf("unexpected settings: %+v", resolved)
	}
	if o.changed["seed"] || string(saved) != string(b) {
		t.Fatal("caller state or original config changed")
	}
	r, _, err := resolveWorkload(template, manifest, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if r.Concurrency != 3 || r.Seed() != 17 || r.Duration() != 1800 {
		t.Fatalf("overrides lost: %+v", r)
	}
	p, err := prepare(o)
	if err != nil {
		t.Fatal(err)
	}
	if string(p.files["experiment.yaml"]) != string(b) {
		t.Fatal("experiment snapshot missing")
	}
	// An explicit path overrides a config-relative reference without rebasing it.
	o.input, o.changed["input"] = "relative-cli-input", true
	resolved, _, err = withExperiment(o)
	if err != nil || resolved.input != "relative-cli-input" {
		t.Fatalf("CLI path: %q %v", resolved.input, err)
	}
}

func TestExperimentPinsCorpusBeyondManifest(t *testing.T) {
	o, _, _ := fixture(t)
	config, b := experimentFixture(t, o)
	o.config = config
	a, err := fingerprint(filepath.Join(o.input, "corpus.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, config, []byte(strings.ReplaceAll(string(b), a.SHA256, strings.Repeat("0", 64))))
	if _, err := prepare(o); err == nil || !strings.Contains(err.Error(), "differs from experiment") {
		t.Fatalf("experiment pin not enforced: %v", err)
	}
	if _, err := os.Stat(o.output); !os.IsNotExist(err) {
		t.Fatalf("failed pin created output: %v", err)
	}
}

func TestExperimentRejectsTyposAndInvalidDefinitions(t *testing.T) {
	for _, change := range []struct{ name, from, to string }{
		{"typo", "concurrency:", "concurency:"},
		{"version", "version: 1", "version: 2"},
		{"missing input", "input: ../input", "input: ''"},
		{"replicas", "num_instances: 4", "num_instances: 0"},
		{"duration", "duration: 30m", "duration: 5s"},
		{"document", "", "---\nversion: 1\n"},
	} {
		t.Run(change.name, func(t *testing.T) {
			o, _, _ := fixture(t)
			o.config, _ = experimentFixture(t, o)
			b, err := os.ReadFile(o.config)
			if err != nil {
				t.Fatal(err)
			}
			if change.from == "" {
				b = append(b, []byte(change.to)...)
			} else {
				b = []byte(strings.ReplaceAll(string(b), change.from, change.to))
			}
			writeTestFile(t, o.config, b)
			if _, _, err := withExperiment(o); err == nil {
				t.Fatal("invalid experiment accepted")
			}
		})
	}
}

func TestStandardExperimentsHaveConsistentPlacement(t *testing.T) {
	paths, err := filepath.Glob("../configs/experiments/*.yaml")
	if err != nil || len(paths) != 4 {
		t.Fatalf("experiments: %v %v", paths, err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			o, err := parseOptions([]string{"--config", path})
			if err != nil {
				t.Fatal(err)
			}
			o, _, err = withExperiment(o)
			if err != nil {
				t.Fatal(err)
			}
			for _, ref := range []string{o.scenario, o.workload, o.defaults} {
				if _, err := os.Stat(ref); err != nil {
					t.Fatal(err)
				}
			}
			s, d, err := latencykernel.LoadBundle(o.scenario)
			if err != nil {
				t.Fatal(err)
			}
			if p := s.Validate(); !p.OK() {
				t.Fatal(p.Error())
			}
			if p := d.Validate(); !p.OK() {
				t.Fatal(p.Error())
			}
			if p := d.ValidateAgainstCluster(deployment.ClusterConstraints{Nodes: s.Cluster.Nodes, GPUsPerNode: s.Cluster.GPUsPerNode}); !p.OK() {
				t.Fatal(p.Error())
			}
			if len(d.Pools) != 1 {
				t.Fatal("expected one colocated pool")
			}
			pool := d.Pools[0]
			if pool.Parallel.TP != 8 || pool.Parallel.PP != 1 || pool.Parallel.DP != 1 || s.Cluster.GPUsPerNode != 8 || pool.Nodes != o.instances {
				t.Fatalf("instance count disagrees with placement: %+v %+v", o, pool)
			}
			if d.Offload != nil || d.Router != nil {
				t.Fatal("baseline serving semantics changed")
			}
		})
	}
}
