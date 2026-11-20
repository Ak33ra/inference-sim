package researchrouter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/inference-sim/inference-sim/sim"
	"github.com/inference-sim/inference-sim/sim/cluster"
	"github.com/inference-sim/inference-sim/sim/internal/testutil/fakelatency"
)

type reservationHost struct {
	*witnessHost
	instances []*cluster.InstanceSimulator
	gateway   *Gateway
}

func (h *reservationHost) Instances() []*cluster.InstanceSimulator { return h.instances }
func (h *reservationHost) Dispatch(req *sim.Request, placement cluster.Placement) {
	req.AssignedInstance = string(placement.Decode)
	h.gateway.OnDispatched(req, h.Now())
}

func reservationFixture(t *testing.T) (Config, sessionClassManifest) {
	t.Helper()
	var m sessionClassManifest
	m.FormatVersion, m.TraceSHA256 = 1, strings.Repeat("a", 64)
	m.Ranking.Metric = "minimum_session_elapsed_us_across_cache_off_isolated_family_runs"
	m.Ranking.ReferenceLabel = "best-observed isolated-family session reference; siblings still compete"
	m.Ranking.ReferenceRunCount, m.Ranking.ShortCount, m.Ranking.LongCount = 35, 1, 2
	m.Ranking.TiePolicy = "reference_elapsed_us_then_runtime_session_id"
	m.Sessions = map[string]sessionReference{
		"a/same":  {TraceID: "a", SessionID: "same", Class: "short", ElapsedUs: 10, Run: "isolated-a"},
		"b/same":  {TraceID: "b", SessionID: "same", Class: "long", ElapsedUs: 10, Run: "isolated-a"},
		"b/other": {TraceID: "b", SessionID: "other", Class: "long", ElapsedUs: 20, Run: "isolated-b"},
	}
	path := filepath.Join(t.TempDir(), "classes.json")
	saveReservationManifest(t, path, m)
	return Config{Policy: PolicySessionReservation, SessionClassManifest: path,
		ReservedInstances: []cluster.InstanceID{"instance_0", "instance_1"}}, m
}

func saveReservationManifest(t *testing.T, path string, m sessionClassManifest) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func newReservationHost() *reservationHost {
	h := &reservationHost{witnessHost: newWitnessHost()}
	for i := 0; i < 8; i++ {
		cfg := sim.SimConfig{Seed: 42, KVCacheConfig: sim.NewKVCacheConfig(100, 4, 0, 0, 0, 0),
			BatchConfig: sim.NewBatchConfig(16, 64, 0), LatencyModel: fakelatency.New()}
		h.instances = append(h.instances, cluster.NewInstanceSimulator(cluster.InstanceID(fmt.Sprintf("instance_%d", i)), cfg))
	}
	return h
}

func startReservationFixture(t *testing.T) (*Gateway, *reservationHost) {
	t.Helper()
	cfg, _ := reservationFixture(t)
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h := newReservationHost()
	h.gateway = g
	if err := g.Start(h); err != nil {
		t.Fatal(err)
	}
	return g, h
}

func TestSessionReservationEligibilityAndFixedIdentity(t *testing.T) {
	g, h := startReservationFixture(t)
	for _, inst := range h.instances {
		g.loads[inst.ID()] = 100
	}
	g.loads["instance_0"] = 0
	short := &sim.Request{ID: "short", SessionID: "a/same"}
	g.dispatch(short)
	if short.AssignedInstance != "instance_0" {
		t.Fatalf("short session did not use reserved idle server: %s", short.AssignedInstance)
	}
	g.loads["instance_2"] = 2
	long := &sim.Request{ID: "long", SessionID: "b/same"}
	g.dispatch(long)
	if long.AssignedInstance != "instance_2" {
		t.Fatalf("long chose %s, want instance_2", long.AssignedInstance)
	}
	h.instances[0].State = sim.InstanceStateDraining
	h.instances[1].State = sim.InstanceStateDraining
	g.loads["instance_2"] = 0
	next := &sim.Request{ID: "long-next", SessionID: "b/same", OutputTokens: make([]sim.TokenID, 10000)}
	g.dispatch(next)
	if next.AssignedInstance != "instance_2" {
		t.Fatalf("reservation identities shifted when instances drained: %s", next.AssignedInstance)
	}
	if g.decisions[0].SessionClass != "short" || g.decisions[1].SessionClass != "long" || g.decisions[2].SessionClass != "long" {
		t.Fatal("class changed across turns or namespaces")
	}
}

func TestSessionReservationAbortsWithoutClassOrEligibleServer(t *testing.T) {
	for _, session := range []string{"", "unknown", "b/same"} {
		t.Run(string(session), func(t *testing.T) {
			g, h := startReservationFixture(t)
			if session == "b/same" {
				for _, inst := range h.instances[2:] {
					inst.State = sim.InstanceStateDraining
				}
			}
			g.dispatch(&sim.Request{ID: "request", SessionID: session})
			if h.aborted == nil || len(g.decisions) != 0 {
				t.Fatal("invalid class/eligibility did not abort before placement")
			}
		})
	}
	g, h := startReservationFixture(t)
	for _, inst := range h.instances {
		inst.State = sim.InstanceStateDraining
	}
	g.dispatch(&sim.Request{ID: "short", SessionID: "a/same"})
	if h.aborted == nil {
		t.Fatal("no routable endpoints did not abort")
	}
}

func TestSessionReservationRetainsLoadUntilCompletion(t *testing.T) {
	g, h := startReservationFixture(t)
	for _, inst := range h.instances {
		g.loads[inst.ID()] = 5
	}
	g.loads["instance_2"] = 0
	req := &sim.Request{ID: "long", SessionID: "b/same"}
	g.dispatch(req)
	g.OnDone(req, 200, cluster.DoneCompleted)
	h.runThrough(199)
	if g.loads["instance_2"] != 1 {
		t.Fatal("future completion released outstanding load early")
	}
	h.runThrough(200)
	if g.loads["instance_2"] != 0 {
		t.Fatal("completion did not release outstanding load")
	}
}

func TestSessionReservationShortUsesExactJSQTies(t *testing.T) {
	g, h := startReservationFixture(t)
	control, err := New(Config{Policy: PolicyJSQ})
	if err != nil {
		t.Fatal(err)
	}
	ch := newReservationHost()
	ch.gateway = control
	if err := control.Start(ch); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		a := &sim.Request{ID: fmt.Sprint(i), SessionID: "a/same", InputTokens: make([]sim.TokenID, 4)}
		b := *a
		g.dispatch(a)
		control.dispatch(&b)
		if a.AssignedInstance != b.AssignedInstance {
			t.Fatalf("short JSQ tie differs at %d", i)
		}
	}
	if h.aborted != nil || ch.aborted != nil {
		t.Fatal("unexpected abort")
	}
	actual := g.Decisions()
	want := control.Decisions()
	for i := range actual {
		actual[i].Policy = "jsq"
		actual[i].Mode = "jsq"
		actual[i].SessionID = ""
		actual[i].SessionClass = ""
	}
	if !reflect.DeepEqual(actual, want) {
		t.Fatal("short policy changed ordinary JSQ decision values")
	}
	path := filepath.Join(t.TempDir(), "audit.json")
	if err := control.WriteAudit(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"SessionClassManifest", "ReservedInstances", "session_class", "session_id"} {
		if strings.Contains(string(data), field) {
			t.Fatalf("ordinary JSQ gained %s field", field)
		}
	}
}

func TestSessionReservationManifestRankAndStartValidation(t *testing.T) {
	cfg, m := reservationFixture(t)
	if _, err := loadSessionClassManifest(cfg.SessionClassManifest); err != nil {
		t.Fatal(err)
	}
	wrong := m.Sessions["b/same"]
	wrong.Class = "short"
	m.Sessions["b/same"] = wrong
	saveReservationManifest(t, cfg.SessionClassManifest, m)
	if _, err := loadSessionClassManifest(cfg.SessionClassManifest); err == nil {
		t.Fatal("class/rank mismatch accepted")
	}
	cfg, _ = reservationFixture(t)
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h := newReservationHost()
	h.instances = h.instances[1:]
	if err := g.Start(h); err == nil {
		t.Fatal("missing reserved endpoint accepted")
	}
	cfg.ReservedInstances = []cluster.InstanceID{"instance_0", "instance_2"}
	if _, err := New(cfg); err == nil {
		t.Fatal("different reservation identities accepted")
	}
}
