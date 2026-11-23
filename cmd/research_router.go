package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/inference-sim/inference-sim/sim"
	"github.com/inference-sim/inference-sim/sim/cluster"
	"github.com/inference-sim/inference-sim/sim/researchrouter"
)

var (
	researchRouter          string
	researchRouterConfig    string
	researchRouterDecisions string
)

// researchRouterFile is intentionally the complete JSON allowlist. Keeping it
// private stops an experiment configuration from acquiring accidental knobs.
type researchRouterFile struct {
	SessionClassManifest *string               `json:"session_class_manifest"`
	ReservedInstances    *[]cluster.InstanceID `json:"reserved_instances"`
	LatencyUs            *int64                `json:"latency_us"`
	CacheEventDelayUs    *int64                `json:"cache_event_delay_us"`
	HistoryWindowS       *float64              `json:"history_window_s"`
	PrefillUsPerToken    *float64              `json:"prefill_us_per_token"`
	DecodeUsPerToken     *float64              `json:"decode_us_per_token"`
	InitialOutputTokens  *float64              `json:"initial_output_tokens"`
}

func loadResearchRouterConfig(path string, base routerInputs) (researchrouter.Config, error) {
	cfg := researchrouter.Config{
		Policy:            researchRouter,
		LatencyUs:         base.GatewayLatencyUs,
		CacheEventDelayUs: base.KVEventDelayUs,
		HistoryWindowS:    researchrouter.DefaultHistoryWindowS,
	}
	if path == "" {
		if researchRouter == researchrouter.PolicyPrebleE2 {
			return researchrouter.Config{}, fmt.Errorf("preble-e2 requires --research-router-config with calibrated pricing and initial_output_tokens")
		}
		return cfg, cfg.Validate()
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return researchrouter.Config{}, fmt.Errorf("read --research-router-config: %w", err)
	}
	if body := bytes.TrimSpace(b); len(body) == 0 || body[0] != '{' {
		return researchrouter.Config{}, fmt.Errorf("parse --research-router-config: must contain one JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var file researchRouterFile
	if err := dec.Decode(&file); err != nil {
		return researchrouter.Config{}, fmt.Errorf("parse --research-router-config: %w", err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return researchrouter.Config{}, fmt.Errorf("parse --research-router-config: %w", err)
	}
	if researchRouter != researchrouter.PolicySessionReservation && (file.SessionClassManifest != nil || file.ReservedInstances != nil) {
		return researchrouter.Config{}, fmt.Errorf("session_class_manifest and reserved_instances require session-reservation")
	}
	if file.SessionClassManifest != nil {
		cfg.SessionClassManifest = *file.SessionClassManifest
	}
	if file.ReservedInstances != nil {
		cfg.ReservedInstances = *file.ReservedInstances
	}
	if file.LatencyUs != nil {
		cfg.LatencyUs = *file.LatencyUs
	}
	if file.CacheEventDelayUs != nil {
		cfg.CacheEventDelayUs = *file.CacheEventDelayUs
	}
	if file.HistoryWindowS != nil {
		cfg.HistoryWindowS = *file.HistoryWindowS
	}
	if file.PrefillUsPerToken != nil {
		cfg.PrefillUsPerToken = *file.PrefillUsPerToken
	}
	if file.DecodeUsPerToken != nil {
		cfg.DecodeUsPerToken = *file.DecodeUsPerToken
	}
	if file.InitialOutputTokens != nil {
		cfg.InitialOutputTokens = *file.InitialOutputTokens
	}
	if researchRouter == researchrouter.PolicyPrebleE2 && file.InitialOutputTokens == nil {
		return researchrouter.Config{}, fmt.Errorf("preble-e2 requires explicit initial_output_tokens; it is a calibration prior, never a trace-output oracle")
	}
	if err := cfg.Validate(); err != nil {
		return researchrouter.Config{}, err
	}
	return cfg, nil
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	err := dec.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return fmt.Errorf("must contain one JSON object")
	}
	return err
}

type researchAuditWriter interface{ WriteAudit(string) error }

func newReplayGateway(in routerInputs, model string) (cluster.Gateway, researchAuditWriter, error) {
	if researchRouter == cluster.PolicySessionSplit || researchRouter == cluster.PolicyRequestSplit {
		return newCentralSplitGateway(in.GatewayLatencyUs)
	}
	if researchRouter == "" {
		return newGateway(in, model), nil, nil
	}
	cfg, err := loadResearchRouterConfig(researchRouterConfig, in)
	if err != nil {
		return nil, nil, err
	}
	gw, err := researchrouter.New(cfg)
	if err != nil {
		return nil, nil, err
	}
	return gw, gw, nil
}

// replayGatewaySeed preserves TraceV2's router-seed field even when a research
// gateway replaces llm-d. It is the same derivation llm-d uses when the trace
// did not record a router seed.
func replayGatewaySeed(in routerInputs) int64 {
	if in.Seed != nil {
		return *in.Seed
	}
	return sim.NewPartitionedRNG(sim.NewSimulationKey(seed)).ForSubsystem("epp").Int63()
}

func newCentralSplitGateway(latencyUs int64) (cluster.Gateway, researchAuditWriter, error) {
	var config struct {
		RemainingManifest string `json:"remaining_manifest"`
		ServiceManifest   string `json:"service_manifest"`
		LatencyUs         *int64 `json:"latency_us"`
	}
	b, err := os.ReadFile(researchRouterConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("%s configuration: %w", researchRouter, err)
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&config); err != nil {
		return nil, nil, err
	}
	if err := ensureJSONEOF(d); err != nil {
		return nil, nil, err
	}
	if researchRouter == cluster.PolicyRequestSplit {
		if config.ServiceManifest == "" || config.RemainingManifest != "" {
			return nil, nil, fmt.Errorf("request-split requires service_manifest and forbids remaining_manifest")
		}
	} else if config.RemainingManifest == "" || config.ServiceManifest != "" {
		return nil, nil, fmt.Errorf("session-split requires remaining_manifest and forbids service_manifest")
	}
	if config.LatencyUs != nil {
		latencyUs = *config.LatencyUs
	}
	if researchRouter == cluster.PolicyRequestSplit {
		gw, err := cluster.NewRequestSplitGateway(config.ServiceManifest, latencyUs)
		if err != nil {
			return nil, nil, err
		}
		return gw, gw, nil
	}
	gw, err := cluster.NewSessionSplitGateway(config.RemainingManifest, latencyUs)
	if err != nil {
		return nil, nil, err
	}
	return gw, gw, nil
}
