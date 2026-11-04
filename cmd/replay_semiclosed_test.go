package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/inference-sim/inference-sim/sim/cluster"
	"github.com/inference-sim/inference-sim/sim/workload/semiclosed"
	"github.com/spf13/cobra"
)

func restoreSemiClosedFlags(t *testing.T) {
	t.Helper()
	input, report, join, session, metrics := semiClosedInput, semiClosedReport, semiClosedJoinPolicy, replaySessionMode, replayMetricsPath
	t.Cleanup(func() {
		semiClosedInput, semiClosedReport, semiClosedJoinPolicy, replaySessionMode, replayMetricsPath = input, report, join, session, metrics
	})
}

func TestSemiClosedFlagConflicts(t *testing.T) {
	for _, conflict := range []string{"horizon", "concurrency", "trace-header", "session-mode", "benchmark-duration"} {
		t.Run(conflict, func(t *testing.T) {
			restoreSemiClosedFlags(t)
			c := &cobra.Command{}
			registerSemiClosedReplayFlags(c)
			c.Flags().String(conflict, "", "")
			if err := c.ParseFlags([]string{"--semi-closed-input", "input", "--semi-closed-report", "report.json", "--" + conflict, "1"}); err != nil {
				t.Fatal(err)
			}
			if err := validateSemiClosedFlags(c); err == nil || !strings.Contains(err.Error(), conflict) {
				t.Fatalf("conflict accepted: %v", err)
			}
		})
	}
	t.Run("separate report", func(t *testing.T) {
		restoreSemiClosedFlags(t)
		c := &cobra.Command{}
		registerSemiClosedReplayFlags(c)
		if err := c.ParseFlags([]string{"--semi-closed-input", "input", "--semi-closed-report", "input/manifest.json"}); err != nil {
			t.Fatal(err)
		}
		if err := validateSemiClosedFlags(c); err == nil {
			t.Fatal("input overwrite accepted")
		}
	})
}

func TestSemiClosedZeroOutputPDRefused(t *testing.T) {
	c, err := semiclosed.Load("../sim/workload/semiclosed/testdata/causal")
	if err != nil {
		t.Fatal(err)
	}
	x, err := semiclosed.NewReplay(c, semiclosed.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateSemiClosedDeployment(x, 1, 1, 0); err == nil || !strings.Contains(err.Error(), "zero-output") {
		t.Fatalf("unsafe P/D zero-output path accepted: %v", err)
	}
	if err := validateSemiClosedDeployment(x, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
}

type semiClosedStartupFailure struct {
	cluster.Gateway
	err error
}

func (g semiClosedStartupFailure) Start(cluster.GatewayHost) error { return g.err }

func TestSemiClosedStartupErrorPreservesReport(t *testing.T) {
	restoreSemiClosedFlags(t)
	semiClosedReport = filepath.Join(t.TempDir(), "workload.json")
	c, err := semiclosed.Load("../sim/workload/semiclosed/testdata/causal")
	if err != nil {
		t.Fatal(err)
	}
	x, err := semiclosed.NewReplay(c, semiclosed.Config{})
	if err != nil {
		t.Fatal(err)
	}
	dep := newKernelDeployment(t, 42, nil)
	cs := cluster.NewClusterSimulator(dep.Config, cluster.NewSliceRequestSource(x.Start()), x.OnComplete)
	boom := errors.New("gateway startup failed")
	cs.SetGateway(semiClosedStartupFailure{err: boom})
	runErr := cs.Run()
	if !errors.Is(runErr, boom) || cs.AggregatedMetrics() != nil {
		t.Fatal("expected error before aggregate finalization")
	}
	if err := finishSemiClosedRun(x, cs.Clock(), cs.AggregatedMetrics(), runErr); !errors.Is(err, boom) {
		t.Fatalf("lost original run failure: %v", err)
	}
	data, err := os.ReadFile(semiClosedReport)
	if err != nil {
		t.Fatal(err)
	}
	var report semiclosed.Report
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.EngineAvailable || report.Complete || report.AccountingMatch || report.States[semiclosed.Censored] != 11 {
		t.Fatalf("startup failure report: %+v", report)
	}
}

func semiClosedCLIFixture(t *testing.T, model string, rejectedBudget int) string {
	t.Helper()
	dir := t.TempDir()
	manifestBytes, err := os.ReadFile("../sim/workload/semiclosed/testdata/causal/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		t.Fatal(err)
	}
	m["selection"].(map[string]any)["model"] = model
	for _, name := range []string{"corpus.jsonl", "audit.jsonl"} {
		data, err := os.ReadFile("../sim/workload/semiclosed/testdata/causal/" + name)
		if err != nil {
			t.Fatal(err)
		}
		data = bytes.ReplaceAll(data, []byte(`"m"`), []byte(`"`+model+`"`))
		if rejectedBudget > 0 {
			lines := strings.Split(strings.TrimSpace(string(data)), "\n")
			for i, line := range lines {
				var row map[string]any
				if err := json.Unmarshal([]byte(line), &row); err != nil {
					t.Fatal(err)
				}
				first := row["requests"].([]any)[0].(map[string]any)
				if name == "corpus.jsonl" {
					first["out"] = rejectedBudget
				} else {
					old := first["completion_tokens"].(float64)
					first["completion_tokens"] = rejectedBudget
					for _, stats := range []map[string]any{row["stats"].(map[string]any), m["included"].(map[string]any)} {
						stats["completion_tokens"] = stats["completion_tokens"].(float64) + float64(rejectedBudget) - old
						if old == 0 {
							stats["zero_output_requests"] = stats["zero_output_requests"].(float64) - 1
						}
					}
				}
				encoded, err := json.Marshal(row)
				if err != nil {
					t.Fatal(err)
				}
				lines[i] = string(encoded)
			}
			data = []byte(strings.Join(lines, "\n") + "\n")
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(data)
		m["artifacts"].(map[string]any)[name] = map[string]any{"bytes": len(data), "sha256": hex.EncodeToString(hash[:])}
	}
	manifestBytes, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), manifestBytes, 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func runSemiClosedCLI(t *testing.T, outputDir string, reject bool) ([]byte, []byte) {
	t.Helper()
	saved := captureCmdLevelVars()
	t.Cleanup(saved.restore)
	restoreSemiClosedFlags(t)
	catalog := setupKernelTestFixtures(t)
	deployment := newKernelDeployment(t, 42, nil)
	modelName := deployment.Config.Model
	rejectedBudget := 0
	if reject {
		rejectedBudget = int(deployment.Config.MaxModelLen) + 1
	}
	dir := semiClosedCLIFixture(t, modelName, rejectedBudget)
	c := &cobra.Command{}
	registerSimConfigFlags(c)
	registerSemiClosedReplayFlags(c)
	c.Flags().StringVar(&replaySessionMode, "session-mode", "fixed", "")
	c.Flags().StringVar(&replayMetricsPath, "metrics-path", "", "")
	if err := c.ParseFlags([]string{"--catalog", catalog, "--scenario", kernelTestScenario, "--scenarios", kernelScenariosDir(t), "--registry", kernelRegistryDir(t), "--defaults-filepath", "../defaults.yaml", "--semi-closed-input", dir, "--semi-closed-report", filepath.Join(outputDir, "workload.json"), "--metrics-path", filepath.Join(outputDir, "metrics.json")}); err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, r); r.Close(); close(done) }()
	defer func() { w.Close(); os.Stdout = old; <-done }()
	replayCmd.Run(c, nil)
	report, err := os.ReadFile(filepath.Join(outputDir, "workload.json"))
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := os.ReadFile(filepath.Join(outputDir, "metrics.json"))
	if err != nil {
		t.Fatal(err)
	}
	return report, metrics
}

func TestReplaySemiClosedSavesEngineAndWorkloadReports(t *testing.T) {
	a, m := runSemiClosedCLI(t, t.TempDir(), false)
	b, n := runSemiClosedCLI(t, t.TempDir(), false)
	if !bytes.Equal(a, b) || !bytes.Equal(m, n) {
		t.Fatal("replay artifacts are nondeterministic")
	}
	var report semiclosed.Report
	if err := json.Unmarshal(a, &report); err != nil {
		t.Fatal(err)
	}
	if !report.Complete || !report.AccountingMatch || report.States[semiclosed.Completed] != 11 || report.Engine.OutputTokens != 27 {
		t.Fatalf("wrong source accounting: %+v", report.Engine)
	}
	var metrics map[string]any
	if err := json.Unmarshal(m, &metrics); err != nil {
		t.Fatal(err)
	}
	if metrics["completed_requests"] != float64(11) || metrics["total_input_tokens"] != float64(report.Selected.PromptTokens) || metrics["total_output_tokens"] != float64(27) {
		t.Fatalf("engine totals: %v", metrics)
	}
	if _, ok := metrics["agentic"]; ok {
		t.Fatal("semi-closed replay changed AgentX metrics schema")
	}
}

func TestReplaySemiClosedFailureSavesArtifacts(t *testing.T) {
	if dir := os.Getenv("BLIS_SEMICLOSED_FAILURE_DIR"); dir != "" {
		runSemiClosedCLI(t, dir, true)
		os.Exit(0)
	}
	dir := t.TempDir()
	c := exec.Command(os.Args[0], "-test.run=^TestReplaySemiClosedFailureSavesArtifacts$")
	c.Env = append(os.Environ(), "BLIS_SEMICLOSED_FAILURE_DIR="+dir)
	output, err := c.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "artifacts saved") {
		t.Fatalf("expected failed replay with saved artifacts: %v\n%s", err, output)
	}
	for _, name := range []string{"workload.json", "metrics.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "workload.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report semiclosed.Report
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Complete || report.AccountingMatch || report.States[semiclosed.Failed] != 2 || report.States[semiclosed.Blocked] != 9 {
		t.Fatalf("failure accounting: %v", report.States)
	}
}
