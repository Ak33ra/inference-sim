package semiclosed

import (
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const forkDAG = `{"id":"f","models":["m"],"block_size":64,"hash_id_scope":"local","requests":[
{"req_id":0,"parent_ids":[],"type":"n","t":0,"api_time":1,"model":"m","in":65,"out":2,"hash_ids":[0]},
{"type":"subagent","agent_id":"a","requests":[{"req_id":2,"parent_ids":[0],"type":"n","t":1.001,"api_time":2,"model":"m","in":64,"out":0,"hash_ids":[0]}]},
{"type":"subagent","agent_id":"b","requests":[{"req_id":1,"parent_ids":[0],"type":"n","t":1.001,"api_time":3,"model":"m","in":64,"out":3,"hash_ids":[0]}]},
{"req_id":3,"parent_ids":[0,2,1],"type":"n","t":4.002,"api_time":1,"think_time":3.002,"model":"m","in":65,"out":1,"hash_ids":[0]}]}`

func loadDAGTest(t *testing.T, data string) *Corpus {
	t.Helper()
	c, err := readDAG(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestDAGForkJoinAndTokenConservation(t *testing.T) {
	c := loadDAGTest(t, forkDAG)
	if c.totals != (Totals{Families: 1, Sessions: 3, Requests: 4, PromptTokens: 258, OutputTokens: 6, ZeroOutputRequests: 1}) {
		t.Fatal(c.totals)
	}
	x, err := NewReplay(c, Config{})
	if err != nil {
		t.Fatal(err)
	}
	root := x.Start()[0]
	admit(t, x, root)
	children := complete(x, root, 10_000_000)
	a, b := findRequest(t, children, "f/2"), findRequest(t, children, "f/1")
	if a.ArrivalTime != 10_001_000 || b.ArrivalTime != 10_001_000 {
		t.Fatal("spawn residual was lost")
	}
	admit(t, x, a)
	admit(t, x, b)
	if !reflect.DeepEqual(root.InputTokens[:64], a.InputTokens) || len(a.OutputTokens) != 0 {
		t.Fatal("prefix or zero output changed")
	}
	if got := complete(x, b, 20_000_000); len(got) != 0 {
		t.Fatal("join released before both children")
	}
	joined := complete(x, a, 30_000_000)
	if len(joined) != 1 || joined[0].ID != "f/3" || joined[0].ArrivalTime != 30_001_000 {
		t.Fatalf("join must wait for latest actual completion plus 1ms, got %+v", joined)
	}
	admit(t, x, joined[0])
	complete(x, joined[0], 40_000_000)
	r := x.Summary()
	if !r.Complete || r.JoinPolicy != ExplicitDAG || *r.Families[0].MakespanUs != 40_000_000 || *r.Sessions[0].CompletionUs != 40_000_000 {
		t.Fatalf("bad DAG lifetime report: %+v", r.Families)
	}
	if _, err := NewReplay(c, Config{JoinPolicy: NoJoins}); err == nil {
		t.Fatal("explicit dependency removal accepted")
	}
}

func TestDAGValidation(t *testing.T) {
	cases := map[string]string{
		"missing id":       strings.Replace(forkDAG, `"req_id":0,`, "", 1),
		"missing parents":  strings.Replace(forkDAG, `"parent_ids":[],`, "", 1),
		"missing time":     strings.Replace(forkDAG, `"t":0,`, "", 1),
		"negative time":    strings.Replace(forkDAG, `"t":0,`, `"t":-0.0000001,`, 1),
		"unknown parent":   strings.Replace(forkDAG, `"parent_ids":[0]`, `"parent_ids":[99]`, 1),
		"duplicate parent": strings.Replace(forkDAG, `"parent_ids":[0]`, `"parent_ids":[0,0]`, 1),
		"self edge":        strings.Replace(forkDAG, `"parent_ids":[]`, `"parent_ids":[0]`, 1),
		"duplicate id":     strings.Replace(forkDAG, `"req_id":2`, `"req_id":0`, 1),
		"negative delay":   strings.Replace(forkDAG, `"t":4.002`, `"t":4`, 1),
		"mixed model":      strings.Replace(forkDAG, `"model":"m"`, `"model":"other"`, 1),
		"negative tokens":  strings.Replace(forkDAG, `"out":0`, `"out":-1`, 1),
		"prefix overflow":  strings.Replace(forkDAG, `"hash_ids":[0]`, `"hash_ids":[0,1]`, 1),
		"cycle":            `{"id":"c","models":["m"],"block_size":64,"hash_id_scope":"local","requests":[{"req_id":0,"parent_ids":[1],"type":"n","model":"m","in":1,"out":0,"t":0,"api_time":0},{"req_id":1,"parent_ids":[0],"type":"n","model":"m","in":1,"out":0,"t":0,"api_time":0}]}`,
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := readDAG(strings.NewReader(data)); err == nil {
				t.Fatal("invalid graph accepted")
			}
		})
	}
}

func TestDAGSubmicrosecondEndpointRounding(t *testing.T) {
	data := `{"id":"f","models":["m"],"block_size":64,"hash_id_scope":"local","requests":[
{"req_id":0,"parent_ids":[],"type":"n","t":0.0000006,"api_time":0.0000006,"model":"m","in":1,"out":1},
{"req_id":1,"parent_ids":[0],"type":"n","t":0.0000012,"api_time":0,"model":"m","in":1,"out":1}]}`
	x, err := NewReplay(loadDAGTest(t, data), Config{})
	if err != nil {
		t.Fatal(err)
	}
	root := x.Start()[0]
	admit(t, x, root)
	next := complete(x, root, 100)
	if root.ArrivalTime != 1 || len(next) != 1 || next[0].ArrivalTime != 100 {
		t.Fatal("rounded source completion introduced a spurious delay")
	}
}

func TestDAGForwardReferencesAndMultipleRoots(t *testing.T) {
	var row map[string]any
	if err := json.Unmarshal([]byte(forkDAG), &row); err != nil {
		t.Fatal(err)
	}
	requests := row["requests"].([]any)
	requests[0], requests[3] = requests[3], requests[0]
	data, _ := json.Marshal(row)
	c := loadDAGTest(t, string(data))
	x, err := NewReplay(c, Config{})
	if err != nil || len(x.Start()) != 1 {
		t.Fatalf("forward references failed: %v", err)
	}
	data = []byte(strings.Replace(forkDAG, `"parent_ids":[0]`, `"parent_ids":[]`, 1))
	c = loadDAGTest(t, string(data))
	x, err = NewReplay(c, Config{Arrivals: ArrivalConfig{Mode: "fixed", IntervalS: 1}})
	if err != nil {
		t.Fatal(err)
	}
	roots := x.Start()
	if len(roots) != 2 || roots[0].ArrivalTime != 0 || roots[1].ArrivalTime != 1_001_000 {
		t.Fatal("multiple roots lost their relative offsets")
	}
}

func TestDAGArrivals(t *testing.T) {
	data := forkDAG + "\n" + strings.Replace(forkDAG, `"id":"f"`, `"id":"g"`, 1)
	c := loadDAGTest(t, data)
	a := ArrivalConfig{Mode: "fixed", IntervalS: 0.25}
	x, err := NewReplay(c, Config{Arrivals: a})
	if err != nil {
		t.Fatal(err)
	}
	roots := x.Start()
	if len(roots) != 2 || roots[0].ArrivalTime != 0 || roots[1].ArrivalTime != 250_000 {
		t.Fatal("fixed spacing incorrect")
	}
	for _, r := range roots {
		admit(t, x, r)
	}
	if reflect.DeepEqual(roots[0].InputTokens, roots[1].InputTokens) {
		t.Fatal("prefix IDs leaked across families")
	}
	a = ArrivalConfig{Mode: "poisson", RatePerS: 2, Seed: 42}
	one, _ := c.arrivals(a)
	two, _ := c.arrivals(a)
	if !reflect.DeepEqual(one, two) || one[0] != 0 || one[1] <= 0 {
		t.Fatal("seeded arrival schedule is not reproducible")
	}
	a.Seed++
	three, _ := c.arrivals(a)
	if reflect.DeepEqual(one, three) {
		t.Fatal("arrival seed has no effect")
	}
	for _, invalid := range []ArrivalConfig{{Mode: "bad"}, {Mode: "poisson"}, {Mode: "poisson", RatePerS: math.NaN()}, {Mode: "fixed", IntervalS: -1}, {Mode: "fixed", IntervalS: -1e-7}, {Mode: "fixed", IntervalS: math.Inf(1)}, {Mode: "trace", IntervalS: 1}} {
		if _, err := NewReplay(c, Config{Arrivals: invalid}); err == nil {
			t.Fatalf("invalid arrivals accepted: %+v", invalid)
		}
	}
}

func TestDAGCompressedInput(t *testing.T) {
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd is not installed")
	}
	cmd := exec.Command("zstd", "-c")
	cmd.Stdin = strings.NewReader(forkDAG)
	compressed, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "trace.jsonl.zst")
	if err := os.WriteFile(path, compressed, 0600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadInput(path)
	if err != nil || c.totals.Requests != 4 {
		t.Fatalf("compressed load failed: %v", err)
	}
	if err := os.WriteFile(path, compressed[:len(compressed)/2], 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadInput(path); err == nil {
		t.Fatal("truncated compressed input accepted")
	}
}
