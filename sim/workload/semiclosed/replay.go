package semiclosed

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/inference-sim/inference-sim/sim"
)

type State string

const (
	Pending   State = "pending"
	Scheduled State = "scheduled"
	Issued    State = "issued"
	Completed State = "completed"
	Failed    State = "failed"
	Blocked   State = "blocked"
	Censored  State = "censored"
)

type Config struct {
	JoinPolicy JoinPolicy
	Arrivals   ArrivalConfig
}

type requestState struct {
	state      State
	request    *sim.Request
	terminal   *sim.Request
	outcome    State
	arrivalUs  int64
	terminalUs int64
	sent       bool
	reason     string
}

// Replay owns lifecycle state on the cluster's event goroutine. Only requests
// whose prerequisites are satisfied enter the cluster's arrival queue.
type Replay struct {
	corpus            *Corpus
	policy            JoinPolicy
	deps              []dependency
	states            []requestState
	byID              map[string]int
	started           bool
	finished          bool
	engine            EngineTotals
	engineAvailable   bool
	endUs             int64
	ttfts             map[string]float64
	e2es              map[string]float64
	metricCompletions map[string]float64
	arrivals          ArrivalConfig
	birthUs           []int64
}

func NewReplay(c *Corpus, cfg Config) (*Replay, error) {
	if c == nil || len(c.requests) == 0 {
		return nil, fmt.Errorf("semi-closed replay requires a loaded nonempty corpus")
	}
	if cfg.JoinPolicy == "" {
		cfg.JoinPolicy = TimingInferred
	}
	if c.explicitDeps != nil {
		if cfg.JoinPolicy != TimingInferred && cfg.JoinPolicy != ExplicitDAG {
			return nil, fmt.Errorf("explicit DAG input cannot disable its parent dependencies")
		}
		cfg.JoinPolicy = ExplicitDAG
	} else if cfg.JoinPolicy != TimingInferred && cfg.JoinPolicy != NoJoins {
		return nil, fmt.Errorf("unknown semi-closed join policy %q", cfg.JoinPolicy)
	}
	if cfg.Arrivals.Mode == "" {
		cfg.Arrivals.Mode = "trace"
	}
	births, err := c.arrivals(cfg.Arrivals)
	if err != nil {
		return nil, err
	}
	deps := c.explicitDeps
	if deps == nil {
		deps = c.dependencies(cfg.JoinPolicy)
	}
	for ri, offset := range c.rootOffsetsUs {
		fi := c.sessions[c.requests[ri].session].family
		if births[fi] > math.MaxInt64-offset {
			return nil, fmt.Errorf("root arrival exceeds simulation clock")
		}
	}
	x := &Replay{corpus: c, policy: cfg.JoinPolicy, deps: deps, arrivals: cfg.Arrivals, birthUs: births, states: make([]requestState, len(c.requests)), byID: make(map[string]int, len(c.requests))}
	for i, r := range c.requests {
		x.states[i].state = Pending
		x.byID[r.id] = i
	}
	return x, nil
}

func (x *Replay) Start() []*sim.Request {
	if x.started || x.finished {
		return nil
	}
	x.started = true
	var out []*sim.Request
	for ri, d := range x.deps {
		if len(d.prerequisites) != 0 {
			continue
		}
		fi := x.corpus.sessions[x.corpus.requests[ri].session].family
		out = append(out, x.schedule(ri, x.birthUs[fi]+x.corpus.rootOffsetsUs[ri]))
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ArrivalTime != out[j].ArrivalTime {
			return out[i].ArrivalTime < out[j].ArrivalTime
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func (x *Replay) schedule(i int, due int64) *sim.Request {
	r := x.corpus.requests[i]
	s := &x.states[i]
	s.state, s.arrivalUs = Scheduled, due
	s.request = &sim.Request{ID: r.id, ArrivalTime: due}
	return s.request
}

func (x *Replay) Gate(req *sim.Request, now int64) (bool, []*sim.Request) {
	i, ok := x.byID[req.ID]
	if !ok || x.finished {
		return false, nil
	}
	s := &x.states[i]
	if s.terminal == req {
		s.terminal = nil
		if s.outcome == Failed {
			x.fail(i, now, s.reason)
			return false, nil
		}
		s.state, s.terminalUs, s.request = Completed, now, nil
		return false, x.release(i)
	}
	if s.state == Issued && s.request == req && req.Redirected {
		return true, nil
	}
	if s.state != Scheduled || s.request != req {
		return false, nil
	}
	if now != s.arrivalUs {
		x.fail(i, now, "arrival timer fired at the wrong time")
		return false, nil
	}
	r := x.corpus.requests[i]
	req.InputTokens = make([]sim.TokenID, r.prompt)
	for block, id := range r.blocks {
		for j := block * 64; j < (block+1)*64; j++ {
			req.InputTokens[j] = id
		}
	}
	for j := len(r.blocks) * 64; j < len(req.InputTokens); j++ {
		req.InputTokens[j] = r.suffixToken
	}
	req.OutputTokens = make([]sim.TokenID, r.output)
	for j := range req.OutputTokens {
		req.OutputTokens[j] = r.outputToken
	}
	explicitOutputBudget := max(1, r.output)
	req.MaxOutputLen = explicitOutputBudget
	req.Model, req.Streaming, req.RoundIndex = r.model, r.stream, r.step
	req.SessionID = x.corpus.families[x.corpus.sessions[r.session].family].id + "/" + x.corpus.sessions[r.session].id
	req.State = sim.StateQueued
	s.state, s.sent = Issued, true
	return true, x.release(i)
}

func (x *Replay) OnComplete(req *sim.Request, now int64) []*sim.Request {
	i, ok := x.byID[req.ID]
	if !ok && req.State != sim.StateCompleted {
		stageFailureID := strings.TrimSuffix(strings.TrimSuffix(req.ID, "_prefill"), "_decode")
		i, ok = x.byID[stageFailureID]
	}
	if !ok || x.finished || x.states[i].state != Issued {
		return nil
	}
	r := x.corpus.requests[i]
	if req.State != sim.StateCompleted || req.LengthCapped || req.GeneratedTokens != r.output {
		return x.scheduleTerminal(i, now, Failed, "request failed or completed with a different token count")
	}
	return x.scheduleTerminal(i, now, Completed, "")
}

func (x *Replay) OnUnserved(req *sim.Request, now int64) []*sim.Request {
	i, ok := x.byID[req.ID]
	if ok && !x.finished && x.states[i].state == Issued {
		return x.scheduleTerminal(i, now, Failed, "request was not served")
	}
	return nil
}

func (x *Replay) scheduleTerminal(i int, now int64, outcome State, reason string) []*sim.Request {
	s := &x.states[i]
	if s.terminal != nil {
		return nil
	}
	s.terminal = &sim.Request{ID: x.corpus.requests[i].id, ArrivalTime: now}
	s.outcome, s.reason = outcome, reason
	return []*sim.Request{s.terminal}
}

func (x *Replay) release(i int) []*sim.Request {
	var out []*sim.Request
	for _, dependent := range x.deps[i].dependents {
		if x.states[dependent].state != Pending {
			continue
		}
		d := x.deps[dependent]
		ready, at := true, int64(0)
		for _, p := range d.prerequisites {
			s := x.states[p.request]
			if p.onSend {
				if !s.sent || s.state == Failed {
					ready = false
					break
				}
				at = max(at, s.arrivalUs)
			} else {
				if s.state != Completed {
					ready = false
					break
				}
				at = max(at, s.terminalUs)
			}
		}
		if !ready {
			continue
		}
		if at > math.MaxInt64-d.delayUs {
			x.fail(dependent, at, "dependency delay exceeds simulation clock")
			continue
		}
		out = append(out, x.schedule(dependent, at+d.delayUs))
	}
	return out
}

func (x *Replay) fail(i int, now int64, reason string) {
	s := &x.states[i]
	s.state, s.terminalUs, s.reason, s.request = Failed, now, reason, nil
	queue := append([]int(nil), x.deps[i].dependents...)
	seen := make(map[int]bool)
	for len(queue) > 0 {
		j := queue[0]
		queue = queue[1:]
		if seen[j] {
			continue
		}
		seen[j] = true
		s := &x.states[j]
		if s.state == Pending || s.state == Scheduled {
			s.state, s.terminalUs, s.reason, s.request = Blocked, now, x.corpus.requests[i].id, nil
			queue = append(queue, x.deps[j].dependents...)
		}
	}
}

// Finish records incomplete work before checking source-to-engine conservation.
func (x *Replay) Finish(now int64, metrics *sim.Metrics) error {
	if !x.finished {
		x.finished, x.endUs = true, now
		for i := range x.states {
			s := &x.states[i]
			if s.state == Pending || s.state == Scheduled || s.state == Issued {
				s.state, s.terminalUs, s.reason = Censored, now, "simulation ended before completion"
			}
			s.request = nil
			s.terminal = nil
		}
		if metrics != nil {
			x.engineAvailable = true
			x.engine = EngineTotals{CompletedRequests: metrics.CompletedRequests, PromptTokens: int64(metrics.TotalInputTokens), OutputTokens: int64(metrics.TotalOutputTokens)}
			x.ttfts = metrics.RequestTTFTs
			x.e2es = metrics.RequestE2Es
			x.metricCompletions = metrics.RequestCompletionTimes
		}
	}
	r := x.Summary()
	if !r.Complete || !r.AccountingMatch {
		return fmt.Errorf("semi-closed replay incomplete or accounting mismatch: states=%v, selected=%+v, engine=%+v", r.States, r.Selected, r.Engine)
	}
	return nil
}
