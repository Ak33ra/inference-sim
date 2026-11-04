package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// An experiment binds reusable serving and replay settings to one converted
// corpus. Machine-specific dependency locations remain launcher flags/env vars.
type experimentConfig struct {
	Version      int      `yaml:"version"`
	Input        string   `yaml:"input"`
	CorpusSHA256 string   `yaml:"corpus_sha256"`
	Scenario     string   `yaml:"scenario"`
	Workload     string   `yaml:"workload"`
	Defaults     string   `yaml:"defaults"`
	Instances    int      `yaml:"num_instances"`
	Concurrency  int      `yaml:"concurrency"`
	Seed         int64    `yaml:"seed"`
	Duration     string   `yaml:"duration"`
	Series       *float64 `yaml:"server_series_interval_s"`
}

func withExperiment(o options) (options, []byte, error) {
	if o.config == "" {
		return o, nil, nil
	}
	b, err := os.ReadFile(o.config)
	if err != nil {
		return o, nil, err
	}
	var c experimentConfig
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	if err := d.Decode(&c); err != nil {
		return o, nil, fmt.Errorf("experiment: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return o, nil, fmt.Errorf("experiment must contain exactly one YAML document")
	}
	if c.Version != 1 || c.Input == "" || c.Scenario == "" || c.Workload == "" || c.Defaults == "" || c.Instances < 1 || c.Concurrency < 1 || c.Series == nil {
		return o, nil, fmt.Errorf("experiment requires version 1, input, scenario, workload, defaults, positive num_instances/concurrency and server_series_interval_s")
	}
	if digest, err := hex.DecodeString(c.CorpusSHA256); err != nil || len(digest) != 32 {
		return o, nil, fmt.Errorf("experiment corpus_sha256 must be a 64-character hex digest")
	}
	duration, err := time.ParseDuration(c.Duration)
	if err != nil || duration < 900*time.Second {
		return o, nil, fmt.Errorf("experiment duration must be a duration of at least 900s")
	}
	// Resolve references relative to the experiment, independent of the caller's
	// directory. CLI paths retain their usual working-directory interpretation.
	base, err := filepath.Abs(filepath.Dir(o.config))
	if err != nil {
		return o, nil, err
	}
	for _, path := range []struct {
		flag, value string
		dst         *string
	}{
		{"input", c.Input, &o.input}, {"scenario", c.Scenario, &o.scenario},
		{"workload", c.Workload, &o.workload}, {"defaults", c.Defaults, &o.defaults},
	} {
		if !o.changed[path.flag] {
			*path.dst = path.value
			if !filepath.IsAbs(*path.dst) {
				*path.dst = filepath.Join(base, *path.dst)
			}
		}
	}
	o.corpusSHA = strings.ToLower(c.CorpusSHA256)
	if !o.changed["num-instances"] {
		o.instances = c.Instances
	}
	if !o.changed["server-series-interval-s"] {
		o.series = *c.Series
	}
	if !o.changed["concurrency"] {
		o.concurrency = c.Concurrency
	}
	if !o.changed["seed"] {
		o.seed = c.Seed
	}
	if !o.changed["duration"] {
		o.duration = duration
	}
	o.changed = maps.Clone(o.changed)
	if o.changed == nil {
		o.changed = make(map[string]bool)
	}
	for _, name := range []string{"concurrency", "seed", "duration"} {
		o.changed[name] = true
	}
	return o, b, nil
}
