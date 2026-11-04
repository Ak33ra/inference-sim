package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/inference-sim/inference-sim/sim"
	"github.com/inference-sim/inference-sim/sim/workload/semiclosed"
	"github.com/spf13/cobra"
)

var (
	semiClosedInput      string
	semiClosedReport     string
	semiClosedJoinPolicy string
)

func registerSemiClosedReplayFlags(c *cobra.Command) {
	c.Flags().StringVar(&semiClosedInput, "semi-closed-input", "", "Replay every complete family from a FreeInference conversion directory, with recorded root arrivals and completion-driven continuations")
	c.Flags().StringVar(&semiClosedReport, "semi-closed-report", "", "Required with --semi-closed-input: write source accounting, request dependencies, and family/session lifetimes to this JSON file")
	c.Flags().StringVar(&semiClosedJoinPolicy, "semi-closed-join-policy", string(semiclosed.TimingInferred), "With --semi-closed-input: timing-inferred joins, or none for a sensitivity run")
}

func validateSemiClosedFlags(c *cobra.Command) error {
	if semiClosedInput == "" {
		if c.Flags().Changed("semi-closed-report") || c.Flags().Changed("semi-closed-join-policy") {
			return fmt.Errorf("semi-closed report and join policy require --semi-closed-input")
		}
		return nil
	}
	if semiClosedReport == "" {
		return fmt.Errorf("--semi-closed-input requires --semi-closed-report")
	}
	if c.Flags().Changed("results-path") {
		return fmt.Errorf("--results-path requires numeric TraceV2 request IDs; use --metrics-path for semi-closed per-request engine metrics")
	}
	for _, flag := range []string{"trace-header", "trace-data", "session-mode", "concurrent-sessions", "total-sessions", "shuffle-corpus", "think-time-ms", "think-time-dist", "trace-output", "agentic-scenario", "workload-spec", "corpus", "corpus-sha256", "corpus-cache", "concurrency", "benchmark-duration", "grace-period", "max-context-length", "horizon"} {
		if c.Flags().Changed(flag) {
			return fmt.Errorf("--%s is incompatible with --semi-closed-input; semi-closed replay retains all selected families and drains naturally", flag)
		}
	}
	report, err := filepath.Abs(semiClosedReport)
	if err != nil {
		return err
	}
	for _, path := range []string{replayMetricsPath, resultsPath, filepath.Join(semiClosedInput, "manifest.json"), filepath.Join(semiClosedInput, "corpus.jsonl"), filepath.Join(semiClosedInput, "audit.jsonl")} {
		if path == "" {
			continue
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		if report == abs {
			return fmt.Errorf("--semi-closed-report must be separate from input and engine result files")
		}
	}
	if replayMetricsPath != "" {
		metrics, err := filepath.Abs(replayMetricsPath)
		if err != nil {
			return err
		}
		for _, name := range []string{"manifest.json", "corpus.jsonl", "audit.jsonl"} {
			input, err := filepath.Abs(filepath.Join(semiClosedInput, name))
			if err != nil {
				return err
			}
			if metrics == input {
				return fmt.Errorf("--metrics-path must be separate from semi-closed input files")
			}
		}
	}
	return nil
}

func buildSemiClosedRun() (*semiclosed.Replay, error) {
	corpus, err := semiclosed.Load(semiClosedInput)
	if err != nil {
		return nil, err
	}
	return semiclosed.NewReplay(corpus, semiclosed.Config{JoinPolicy: semiclosed.JoinPolicy(semiClosedJoinPolicy)})
}

func validateSemiClosedDeployment(x *semiclosed.Replay, prefill, decode, shared int) error {
	if x != nil && (prefill > 0 || decode > 0 || shared > 0) && x.Summary().Selected.ZeroOutputRequests > 0 {
		return fmt.Errorf("semi-closed corpus contains zero-output requests, which the existing P/D engine cannot serve; use a colocated deployment to preserve the recorded zero outputs")
	}
	return nil
}

func saveSemiClosedReport(x *semiclosed.Replay) error {
	data, err := json.MarshalIndent(x.Summary(), "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(semiClosedReport, append(data, '\n'), 0644)
}

func finishSemiClosedRun(x *semiclosed.Replay, now int64, metrics *sim.Metrics, runErr error) error {
	completionErr := x.Finish(now, metrics)
	if err := saveSemiClosedReport(x); err != nil {
		if runErr != nil {
			return fmt.Errorf("replay failed: %w; save semi-closed report: %v", runErr, err)
		}
		return fmt.Errorf("save semi-closed report: %w", err)
	}
	if runErr != nil {
		return fmt.Errorf("replay failed: %w (workload report saved; engine aggregate unavailable)", runErr)
	}
	if completionErr != nil {
		return fmt.Errorf("%w (artifacts saved)", completionErr)
	}
	return nil
}
