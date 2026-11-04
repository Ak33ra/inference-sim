package semiclosed

import (
	"reflect"
	"testing"

	"github.com/inference-sim/inference-sim/sim"
)

func newFixtureReplay(t *testing.T, policy JoinPolicy) *Replay {
	t.Helper()
	x, err := NewReplay(loadFixture(t), Config{JoinPolicy: policy})
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func admit(t *testing.T, x *Replay, r *sim.Request) []*sim.Request {
	t.Helper()
	ok, more := x.Gate(r, r.ArrivalTime)
	if !ok {
		t.Fatalf("gate refused %s", r.ID)
	}
	return more
}

func complete(x *Replay, r *sim.Request, at int64) []*sim.Request {
	copy := *r
	copy.State, copy.ProgressIndex = sim.StateCompleted, int64(len(r.InputTokens)+max(0, len(r.OutputTokens)-1))
	copy.GeneratedTokens = len(r.OutputTokens)
	return finishTimers(x, x.OnComplete(&copy, at))
}

func finishTimers(x *Replay, timers []*sim.Request) []*sim.Request {
	var more []*sim.Request
	for _, timer := range timers {
		_, out := x.Gate(timer, timer.ArrivalTime)
		more = append(more, out...)
	}
	return more
}

func findRequest(t *testing.T, rs []*sim.Request, id string) *sim.Request {
	t.Helper()
	for _, r := range rs {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("missing %s in %+v", id, rs)
	return nil
}

func TestRecordedBirthsAndExactWireTokens(t *testing.T) {
	x := newFixtureReplay(t, TimingInferred)
	roots := x.Start()
	if len(roots) != 2 || roots[0].ArrivalTime != 0 || roots[1].ArrivalTime != 2000 || len(x.Start()) != 0 {
		t.Fatal("root births changed or Start repeated them")
	}
	for _, r := range roots {
		if len(r.InputTokens) != 0 {
			t.Fatal("prompt allocated before dispatch")
		}
		admit(t, x, r)
		if r.MaxOutputLen <= 0 || r.Agentic != nil {
			t.Fatal("unexpected wire template or budget")
		}
	}
	if len(roots[0].InputTokens) != 128 || len(roots[0].OutputTokens) != 3 || len(roots[1].InputTokens) != 64 || len(roots[1].OutputTokens) != 0 {
		t.Fatal("source tokens changed")
	}
	if reflect.DeepEqual(roots[0].InputTokens[:64], roots[1].InputTokens) {
		t.Fatal("family-local block identities collided")
	}
	children := complete(x, roots[0], 20000)
	c1 := findRequest(t, children, "fixture/A/c1/0")
	c2 := findRequest(t, children, "fixture/A/c2/0")
	if c1.ArrivalTime != 25000 || c2.ArrivalTime != 26000 {
		t.Fatal("post-response spawn gap changed")
	}
	admit(t, x, c1)
	admit(t, x, c2)
	if !reflect.DeepEqual(c1.InputTokens, roots[0].InputTokens) || !reflect.DeepEqual(c2.InputTokens[:64], c1.InputTokens[:64]) {
		t.Fatal("mapped prefix sharing changed")
	}
	if c2.InputTokens[64] == c1.InputTokens[64] {
		t.Fatal("unknown suffix overlaps a mapped prefix")
	}
	if len(complete(x, roots[0], 21000)) != 0 {
		t.Fatal("duplicate completion released work")
	}
	if x.states[x.byID[roots[0].ID]].request != nil {
		t.Fatal("completed request retained by driver")
	}
	if len(complete(x, c1, 30000)) != 0 {
		t.Fatal("join released before every child")
	}
	joined := complete(x, c2, 35000)
	if len(joined) != 1 || joined[0].ArrivalTime != 40000 {
		t.Fatalf("join should use 5ms residual, got %+v", joined)
	}
	admit(t, x, joined[0])
	next := complete(x, joined[0], 50000)
	if len(next) != 1 || next[0].ArrivalTime != 100000 {
		t.Fatal("ordinary end-to-start gap changed")
	}
	overlap := admit(t, x, next[0])
	if len(overlap) != 1 || overlap[0].ArrivalTime != 105000 {
		t.Fatal("overlap did not anchor to actual send")
	}
	admit(t, x, overlap[0])
	if len(complete(x, next[0], 150000)) != 0 {
		t.Fatal("join released before overlapping child ended")
	}
	childNext := complete(x, overlap[0], 160000)
	if len(childNext) != 1 || childNext[0].ArrivalTime != 165000 {
		t.Fatal("overlap child continuation lost gap")
	}
	admit(t, x, childNext[0])
	last := complete(x, childNext[0], 180000)
	if len(last) != 1 || last[0].ArrivalTime != 260000 {
		t.Fatal("overlap join residual should be 80ms")
	}
	admit(t, x, last[0])
	background := complete(x, last[0], 280000)
	if len(background) != 1 || background[0].ArrivalTime != 285000 {
		t.Fatal("background child missing")
	}
	if x.Summary().Families[0].CompletionUs != nil {
		t.Fatal("family left before background child drained")
	}
	admit(t, x, background[0])
	complete(x, background[0], 320000)
	if got := x.Summary().Families[0].CompletionUs; got == nil || *got != 320000 {
		t.Fatal("family did not leave after all descendants drained")
	}
	bnext := complete(x, roots[1], 5000)
	if len(bnext) != 1 || bnext[0].ArrivalTime != 10000 {
		t.Fatal("zero-output completion lost continuation")
	}
}

func TestNoJoinSensitivityAndFailureClosure(t *testing.T) {
	x := newFixtureReplay(t, NoJoins)
	r := x.Start()[0]
	admit(t, x, r)
	more := complete(x, r, 20000)
	parent := findRequest(t, more, "fixture/A/A/1")
	if parent.ArrivalTime != 50000 {
		t.Fatal("no-join policy should retain ordinary 30ms parent gap")
	}
	x = newFixtureReplay(t, TimingInferred)
	r = x.Start()[0]
	admit(t, x, r)
	finishTimers(x, x.OnUnserved(r, 1000))
	x.OnUnserved(r, 2000)
	if got := x.Summary().States; got[Failed] != 1 || got[Blocked] != 8 || got[Scheduled] != 1 || got[Pending] != 1 {
		t.Fatalf("failed prerequisite closure: %v", got)
	}
	if err := x.Finish(2000, sim.NewMetrics()); err == nil {
		t.Fatal("incomplete replay passed accounting")
	}
	if got := x.Summary().States[Censored]; got != 2 {
		t.Fatalf("expected 2 censored requests, got %d", got)
	}
}

func TestFailedOverlapParentLetsIssuedChildDrain(t *testing.T) {
	x := newFixtureReplay(t, TimingInferred)
	i := x.byID["fixture/A/A/2"]
	r := x.schedule(i, 100000)
	children := admit(t, x, r)
	failure := x.OnUnserved(r, 110000)
	admit(t, x, children[0])
	finishTimers(x, failure)
	if got := x.states[x.byID[children[0].ID]].state; got != Issued {
		t.Fatalf("issued overlap child was %s", got)
	}
	next := complete(x, children[0], 120000)
	if len(next) != 1 || next[0].ID != "fixture/A/c3/1" || next[0].ArrivalTime != 125000 {
		t.Fatal("issued child's independent continuation was blocked")
	}
	admit(t, x, next[0])
	if len(complete(x, next[0], 130000)) != 0 {
		t.Fatal("child completion released failed parent join")
	}
	if got := x.states[x.byID[children[0].ID]].state; got != Completed {
		t.Fatal("issued child did not drain")
	}
}

func TestJoinResidualUsesLatestRecordedPrerequisite(t *testing.T) {
	c := &Corpus{
		families: []family{{id: "f", root: 0, sessions: []int{0, 1}}},
		sessions: []session{{id: "root", requests: []int{0, 1, 2}, spawn: -1, join: -1}, {id: "child", requests: []int{3}, spawn: 0, join: 2}},
		requests: []request{
			{id: "r0", session: 0, startMs: 0, endMs: 10, prompt: 64, output: 1},
			{id: "r1", session: 0, step: 1, startMs: 20, endMs: 50, prompt: 64, output: 1},
			{id: "r2", session: 0, step: 2, startMs: 60, endMs: 70, prompt: 64, output: 1},
			{id: "child", session: 1, startMs: 10, endMs: 30, prompt: 64, output: 1},
		},
	}
	x, err := NewReplay(c, Config{})
	if err != nil {
		t.Fatal(err)
	}
	root := x.Start()[0]
	admit(t, x, root)
	more := complete(x, root, 5000)
	child := findRequest(t, more, "child")
	admit(t, x, child)
	next := findRequest(t, more, "r1")
	admit(t, x, next)
	if len(complete(x, child, 20000)) != 0 {
		t.Fatal("join ignored unfinished parent")
	}
	joined := complete(x, next, 40000)
	if len(joined) != 1 || joined[0].ArrivalTime != 50000 {
		t.Fatal("join did not use 10ms residual after latest recorded prerequisite")
	}
}

func TestSameTimeSiblingsPreserveSourceCallOrder(t *testing.T) {
	dir := copyFixture(t, "parity")
	rewriteArtifact(t, dir, "corpus.jsonl", editJSONLine(t, func(m map[string]any) {
		rows := m["requests"].([]any)
		rows[1], rows[2] = rows[2], rows[1]
	}), true)
	rewriteArtifact(t, dir, "audit.jsonl", editJSONLine(t, func(m map[string]any) {
		children := m["children"].([]any)
		children[0].(map[string]any)["spawned_by_call_index"] = 1
		children[1].(map[string]any)["spawned_by_call_index"] = 0
		children[0], children[1] = children[1], children[0]
		group := m["inferred_groups"].([]any)[0].(map[string]any)["children"].([]any)
		group[0], group[1] = group[1], group[0]
	}), true)
	c, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	x, err := NewReplay(c, Config{})
	if err != nil {
		t.Fatal(err)
	}
	root := x.Start()[0]
	admit(t, x, root)
	children := complete(x, root, 5000)
	if len(children) != 2 || children[0].ID != "fixture/P/pc2/0" || children[1].ID != "fixture/P/pc1/0" || children[0].ArrivalTime != children[1].ArrivalTime {
		t.Fatalf("same-time siblings lost source call order: %+v", children)
	}
}

func TestSameTimeChildrenPrecedeParentContinuation(t *testing.T) {
	c, err := Load("testdata/parity-background")
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []JoinPolicy{TimingInferred, NoJoins} {
		t.Run(string(policy), func(t *testing.T) {
			x, err := NewReplay(c, Config{JoinPolicy: policy})
			if err != nil {
				t.Fatal(err)
			}
			root := x.Start()[0]
			admit(t, x, root)
			var sent []string
			for _, req := range complete(x, root, 5000) {
				if req.ArrivalTime != 5000 {
					t.Fatalf("%s arrival %d != parent completion 5000", req.ID, req.ArrivalTime)
				}
				admit(t, x, req)
				sent = append(sent, req.ID)
			}
			want := []string{"fixture/P/pc1/0", "fixture/P/pc2/0", "fixture/P/P/1"}
			if !reflect.DeepEqual(sent, want) {
				t.Fatalf("same-time sends %v != source occurrence order %v", sent, want)
			}
		})
	}
}
