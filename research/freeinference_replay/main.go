// freeinference_replay runs a converted corpus through BLIS's existing AgentX path.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/inference-sim/blis-schemas/spec/workload"
	"github.com/inference-sim/inference-sim/cmd"
)

const (
	corpusLoader = "freeinference_weka"
	dataset      = "harvardMadsys/freeinference_agentic_trace"
	childCommand = "_blis"
)

// This process-local extension is the only integration with the pinned schema.
// The stock BLIS executable and all AgentX timing/serving behavior are unchanged.
func registerCorpus() {
	workload.AgentXLoaders[corpusLoader] = dataset
}

type options struct {
	config, corpusSHA                                                     string
	input, output, workload, scenario, catalog, registry, defaults, cache string
	instances, concurrency                                                int
	seed                                                                  int64
	duration                                                              time.Duration
	series                                                                float64
	changed                                                               map[string]bool
}

func parseOptions(args []string) (options, error) {
	var o options
	f := flag.NewFlagSet("freeinference_replay", flag.ContinueOnError)
	f.StringVar(&o.config, "config", "", "named experiment YAML; explicit flags override its settings")
	f.StringVar(&o.input, "input", "", "converter output directory (required unless supplied by --config)")
	f.StringVar(&o.output, "output", "", "new run directory (required; never overwritten)")
	f.StringVar(&o.workload, "workload", "research/configs/workloads/agentx.yaml", "AgentX settings; corpus provenance is supplied from the input manifest")
	f.StringVar(&o.scenario, "scenario", "research/configs/clusters/glm-5.2-b200-tp8-2replicas.yaml", "self-contained BLIS Scenario/Deployment YAML")
	f.StringVar(&o.catalog, "catalog", os.Getenv("BLIS_CATALOG"), "catalog root (or BLIS_CATALOG)")
	f.StringVar(&o.registry, "registry", os.Getenv("BLIS_REGISTRY"), "registry root (or BLIS_REGISTRY)")
	f.StringVar(&o.defaults, "defaults", "defaults.yaml", "BLIS defaults file")
	f.StringVar(&o.cache, "corpus-cache", "", "optional shared parsed-corpus cache directory")
	f.IntVar(&o.instances, "num-instances", 2, "serving replicas; must agree with the deployment")
	f.IntVar(&o.concurrency, "concurrency", 0, "override family lanes in the workload")
	f.Int64Var(&o.seed, "seed", 42, "override workload and router seed")
	f.DurationVar(&o.duration, "duration", 0, "override simulated profiling duration (minimum 900s)")
	f.Float64Var(&o.series, "server-series-interval-s", 1, "server metrics bin width in simulated seconds; 0 disables")
	if err := f.Parse(args); err != nil {
		return o, err
	}
	if f.NArg() != 0 {
		return o, fmt.Errorf("unexpected arguments: %v", f.Args())
	}
	o.changed = make(map[string]bool)
	f.Visit(func(f *flag.Flag) { o.changed[f.Name] = true })
	return o, nil
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == childCommand {
		registerCorpus()
		os.Args = append(os.Args[:1], os.Args[2:]...)
		cmd.Execute()
		return
	}
	o, err := parseOptions(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err == nil {
		registerCorpus()
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		err = run(ctx, o)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
