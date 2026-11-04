// checkweka validates a converted corpus through this checkout's AgentX loader.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/inference-sim/inference-sim/sim/workload"
)

func main() {
	dir := flag.String("input", "", "converter output directory")
	split := flag.Bool("split-flattened-agents", false, "apply AgentX's optional prefix-based agent detection")
	flag.Parse()
	if err := check(*dir, *split); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func check(dir string, split bool) error {
	if dir == "" {
		return fmt.Errorf("-input is required")
	}
	var manifest struct {
		Included struct {
			Families int `json:"families"`
		} `json:"included"`
		Artifacts map[string]struct {
			SHA256 string `json:"sha256"`
		} `json:"artifacts"`
	}
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &manifest); err != nil {
		return err
	}
	want := manifest.Artifacts["corpus.jsonl"].SHA256
	if len(want) != 64 || manifest.Included.Families <= 0 {
		return fmt.Errorf("manifest lacks a corpus digest or positive family count")
	}
	corpus, err := workload.LoadAgentXCorpus(filepath.Join(dir, "corpus.jsonl"), want, 0, split)
	if err != nil {
		return err
	}
	if len(corpus.Traces) != manifest.Included.Families {
		return fmt.Errorf("loaded %d families, manifest expected %d", len(corpus.Traces), manifest.Included.Families)
	}
	peak := 0
	for _, trace := range corpus.Traces {
		peak = max(peak, trace.Peak)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"families": len(corpus.Traces), "sha256": corpus.SHA256,
		"max_recorded_context": peak, "split_flattened_agents": split,
	})
}
