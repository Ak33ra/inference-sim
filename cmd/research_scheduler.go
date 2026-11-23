package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/inference-sim/inference-sim/sim"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var researchSchedulerMode, researchSchedulerConfigPath string

func registerResearchSchedulerFlags(c *cobra.Command) {
	c.Flags().StringVar(&researchSchedulerMode, "research-scheduler", "", "Research policy: mlfq or atlas (KV recomputation backend)")
	c.Flags().StringVar(&researchSchedulerConfigPath, "research-scheduler-config", "", "Strict JSON with quanta_ms and starvation_ratio")
}
func readResearchSchedulerConfig(path string) (sim.ResearchSchedulerConfig, error) {
	c := sim.DefaultResearchSchedulerConfig()
	if path == "" {
		return c, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	var raw json.RawMessage
	if err = decoder.Decode(&raw); err != nil {
		return c, err
	}
	if len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return c, fmt.Errorf("scheduler config must be an object")
	}
	fields := json.NewDecoder(bytes.NewReader(raw))
	fields.DisallowUnknownFields()
	if err = fields.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return c, fmt.Errorf("scheduler config must contain exactly one JSON object")
	}
	return c, c.Validate()
}
func researchSchedulerRuntime(c *cobra.Command) *sim.ResearchSchedulerRuntime {
	if researchSchedulerMode == "" {
		if researchSchedulerConfigPath != "" {
			logrus.Fatal("--research-scheduler-config requires --research-scheduler")
		}
		return nil
	}
	if c.Name() != "replay" || semiClosedInput == "" {
		logrus.Fatal("research scheduler currently requires semi-closed replay")
	}
	if prefillInstances > 0 || decodeInstances > 0 || prefillDecodeInstances > 0 || encodeInstances > 0 {
		logrus.Fatal("research scheduler requires colocated engines")
	}
	if scheduler != "fcfs" || preemptionPolicy != "fcfs" {
		logrus.Fatal("research scheduler cannot be combined with scheduler/preemption overrides")
	}
	cfg, err := readResearchSchedulerConfig(researchSchedulerConfigPath)
	if err != nil {
		logrus.Fatalf("research scheduler config: %v", err)
	}
	runtime, err := sim.NewResearchSchedulerRuntime(researchSchedulerMode, cfg)
	if err != nil {
		logrus.Fatal(err)
	}
	return runtime
}
