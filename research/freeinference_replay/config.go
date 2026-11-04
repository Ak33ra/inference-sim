package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/inference-sim/blis-schemas/spec/workload"
	"gopkg.in/yaml.v3"
)

type artifact struct {
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type conversionManifest struct {
	FormatVersion int `json:"format_version"`
	Source        struct {
		Dataset  string `json:"dataset"`
		Revision string `json:"declared_revision"`
	} `json:"source"`
	Included struct {
		Families int `json:"families"`
	} `json:"included"`
	Artifacts map[string]artifact `json:"artifacts"`
}

type preparedRun struct {
	output            string
	files             map[string][]byte
	args              []string
	corpusPath        string
	corpus            artifact
	catalog, registry string
}

func fingerprint(path string) (artifact, error) {
	f, err := os.Open(path)
	if err != nil {
		return artifact{}, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return artifact{SHA256: hex.EncodeToString(h.Sum(nil)), Bytes: n}, err
}

func resolveWorkload(template, manifestBytes []byte, o options) (*workload.Replay, artifact, error) {
	var m conversionManifest
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		return nil, artifact{}, fmt.Errorf("conversion manifest: %w", err)
	}
	c := m.Artifacts["corpus.jsonl"]
	if m.FormatVersion != 1 || m.Source.Dataset != dataset || m.Source.Revision == "" || m.Included.Families < 1 || c.Bytes < 1 {
		return nil, c, fmt.Errorf("expected a version 1 FreeInference conversion manifest with a revision, families and corpus artifact")
	}
	var r workload.Replay
	d := yaml.NewDecoder(bytes.NewReader(template))
	d.KnownFields(true)
	if err := d.Decode(&r); err != nil {
		return nil, c, fmt.Errorf("workload: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, c, fmt.Errorf("workload must contain exactly one YAML document")
	}
	if r.Workload != "" || r.Corpus != (workload.Corpus{}) {
		return nil, c, fmt.Errorf("workload must state settings inline and omit corpus; the conversion manifest supplies provenance")
	}
	// The source identifies the upstream Parquet dataset. The local derived Weka
	// artifact is pinned by its own digest and the saved conversion manifest.
	r.Corpus = workload.Corpus{
		Loader: corpusLoader, Format: workload.CorpusWekaTraceV7, Encoding: workload.CorpusJSONL,
		Source: workload.CorpusSource{HuggingFace: m.Source.Dataset, Revision: m.Source.Revision},
		SHA256: c.SHA256,
	}
	if o.changed["concurrency"] {
		r.Concurrency = o.concurrency
	}
	if o.changed["seed"] {
		r.RandomSeed = &o.seed
	}
	if o.changed["duration"] {
		if o.duration < 900*time.Second {
			return nil, c, fmt.Errorf("duration must be at least 900s")
		}
		r.BenchmarkDurationS = o.duration.Seconds()
	}
	if p := r.Validate(); !p.OK() {
		return nil, c, fmt.Errorf("workload: %s", p.Error())
	}
	return &r, c, nil
}

func prepare(o options) (*preparedRun, error) {
	o, experiment, err := withExperiment(o)
	if err != nil {
		return nil, err
	}
	if o.input == "" || o.output == "" || o.catalog == "" || o.registry == "" {
		return nil, fmt.Errorf("input, output, catalog and registry are required (catalog/registry may come from the environment)")
	}
	if o.instances < 1 || o.series < 0 || math.IsNaN(o.series) || math.IsInf(o.series, 0) {
		return nil, fmt.Errorf("num-instances must be positive and server-series-interval-s finite and nonnegative")
	}
	p := &preparedRun{files: make(map[string][]byte)}
	if experiment != nil {
		p.files["experiment.yaml"] = experiment
	}
	paths := []*string{&o.input, &o.output, &o.catalog, &o.registry, &o.scenario, &o.defaults}
	if o.cache != "" {
		paths = append(paths, &o.cache)
	}
	for _, path := range paths {
		abs, err := filepath.Abs(*path)
		if err != nil {
			return nil, err
		}
		*path = abs
	}
	p.output, p.catalog, p.registry = o.output, o.catalog, o.registry
	if _, err := os.Lstat(o.output); err == nil {
		return nil, fmt.Errorf("output already exists: %s", o.output)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	for _, path := range []string{o.catalog, o.registry} {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("expected directory: %s", path)
		}
	}
	manifestBytes, err := os.ReadFile(filepath.Join(o.input, "manifest.json"))
	if err != nil {
		return nil, err
	}
	template, err := os.ReadFile(o.workload)
	if err != nil {
		return nil, err
	}
	r, want, err := resolveWorkload(template, manifestBytes, o)
	if err != nil {
		return nil, err
	}
	p.corpusPath = filepath.Join(o.input, "corpus.jsonl")
	p.corpus, err = fingerprint(p.corpusPath)
	if err != nil {
		return nil, err
	}
	if p.corpus != want {
		return nil, fmt.Errorf("corpus bytes/digest differ from conversion manifest: got %+v, want %+v", p.corpus, want)
	}
	if o.corpusSHA != "" && p.corpus.SHA256 != o.corpusSHA {
		return nil, fmt.Errorf("corpus digest differs from experiment: got %s, want %s; update the experiment when selecting a different corpus", p.corpus.SHA256, o.corpusSHA)
	}
	p.files["conversion-manifest.json"] = manifestBytes
	p.files["workload.yaml"], err = yaml.Marshal(r)
	if err != nil {
		return nil, err
	}
	scenarioName := filepath.Join("scenarios", filepath.Base(o.scenario))
	for name, path := range map[string]string{scenarioName: o.scenario, "defaults.yaml": o.defaults} {
		p.files[name], err = os.ReadFile(path)
		if err != nil {
			return nil, err
		}
	}
	p.args = []string{
		childCommand, "replay", "--workload-spec", filepath.Join(o.output, "workload.yaml"),
		"--log", "info",
		"--corpus", p.corpusPath,
		"--scenario", filepath.Base(o.scenario), "--scenarios", filepath.Join(o.output, "scenarios"),
		"--catalog", o.catalog, "--registry", o.registry,
		"--defaults-filepath", filepath.Join(o.output, "defaults.yaml"),
		"--num-instances", strconv.Itoa(o.instances), "--seed", strconv.FormatInt(r.Seed(), 10),
		"--server-series-interval-s", strconv.FormatFloat(o.series, 'g', -1, 64),
		"--metrics-path", filepath.Join(o.output, "metrics.json"),
	}
	if o.cache != "" {
		p.args = append(p.args, "--corpus-cache", o.cache)
	}
	return p, nil
}
