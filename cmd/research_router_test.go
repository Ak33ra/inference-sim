package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/inference-sim/inference-sim/sim/researchrouter"
)

func TestResearchRouterConfigStrictBoundary(t *testing.T) {
	oldPolicy, oldPath := researchRouter, researchRouterConfig
	t.Cleanup(func() { researchRouter, researchRouterConfig = oldPolicy, oldPath })
	base := routerInputs{GatewayLatencyUs: 19, KVEventDelayUs: 23}
	write := func(name, body string) string {
		p := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	researchRouter = researchrouter.PolicyJSQ
	if _, err := loadResearchRouterConfig(write("null.json", "null"), base); err == nil || !strings.Contains(err.Error(), "one JSON object") {
		t.Fatalf("null config error = %v, want object rejection", err)
	}
	if _, err := loadResearchRouterConfig(write("unknown.json", `{"unknown":1}`), base); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown config error = %v, want unknown-field rejection", err)
	}
	if _, err := loadResearchRouterConfig(write("negative.json", `{"prefill_us_per_token":-1}`), base); err == nil || !strings.Contains(err.Error(), "non-negative") {
		t.Fatalf("negative unused calibration error = %v, want boundary rejection", err)
	}

	researchRouter = researchrouter.PolicyPrebleE2
	if _, err := loadResearchRouterConfig(write("missing-prior.json", `{"prefill_us_per_token":1,"decode_us_per_token":2}`), base); err == nil || !strings.Contains(err.Error(), "initial_output_tokens") {
		t.Fatalf("missing cold-prior error = %v, want required-field rejection", err)
	}
	cfg, err := loadResearchRouterConfig(write("valid.json", `{"latency_us":5,"cache_event_delay_us":7,"history_window_s":180,"prefill_us_per_token":1,"decode_us_per_token":2,"initial_output_tokens":128}`), base)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LatencyUs != 5 || cfg.CacheEventDelayUs != 7 || cfg.InitialOutputTokens != 128 {
		t.Fatalf("config = %+v, want parsed strict values", cfg)
	}
}

func TestResearchRouterSessionReservationConfig(t *testing.T) {
	old := researchRouter
	t.Cleanup(func() { researchRouter = old })
	path := filepath.Join(t.TempDir(), "router.json")
	body := `{"session_class_manifest":"/frozen/classes.json","reserved_instances":["instance_0","instance_1"]}`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	researchRouter = researchrouter.PolicySessionReservation
	cfg, err := loadResearchRouterConfig(path, routerInputs{})
	if err != nil || cfg.SessionClassManifest != "/frozen/classes.json" || len(cfg.ReservedInstances) != 2 {
		t.Fatalf("reservation config = %+v, %v", cfg, err)
	}
	researchRouter = researchrouter.PolicyJSQ
	if _, err := loadResearchRouterConfig(path, routerInputs{}); err == nil {
		t.Fatal("JSQ accepted reservation-only parameters")
	}
	researchRouter = researchrouter.PolicySessionReservation
	if _, err := loadResearchRouterConfig("", routerInputs{}); err == nil {
		t.Fatal("reservation accepted absent manifest")
	}
}

func TestSessionSplitConfigStrictBoundary(t *testing.T) {
	oldPolicy, oldPath := researchRouter, researchRouterConfig
	t.Cleanup(func() { researchRouter, researchRouterConfig = oldPolicy, oldPath })
	researchRouter = "session-split"
	for _, body := range []string{`{}`, `null`, `{"remaining_manifest":"missing","unknown":1}`, `{"remaining_manifest":"missing"} {}`} {
		researchRouterConfig = filepath.Join(t.TempDir(), "split.json")
		if err := os.WriteFile(researchRouterConfig, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := newReplayGateway(routerInputs{}, ""); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
}

func TestRequestSplitConfigStrictBoundary(t *testing.T) {
	oldPolicy, oldPath := researchRouter, researchRouterConfig
	t.Cleanup(func() { researchRouter, researchRouterConfig = oldPolicy, oldPath })
	researchRouter = "request-split"
	for _, body := range []string{`{}`, `null`, `{"remaining_manifest":"other"}`, `{"service_manifest":"missing","remaining_manifest":"other"}`, `{"service_manifest":"missing","unknown":1}`} {
		researchRouterConfig = filepath.Join(t.TempDir(), "request.json")
		if err := os.WriteFile(researchRouterConfig, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := newReplayGateway(routerInputs{}, ""); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
}
