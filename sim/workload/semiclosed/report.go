package semiclosed

import "encoding/json"

type EngineTotals struct {
	CompletedRequests int   `json:"completed_requests"`
	PromptTokens      int64 `json:"prompt_tokens"`
	OutputTokens      int64 `json:"output_tokens"`
}

type TokenTotals struct {
	PromptTokens int64 `json:"prompt_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

type Report struct {
	FormatVersion       int                   `json:"format_version"`
	JoinPolicy          JoinPolicy            `json:"join_policy"`
	JoinAssumption      string                `json:"join_assumption"`
	Arrivals            ArrivalConfig         `json:"arrivals"`
	Complete            bool                  `json:"complete"`
	AccountingMatch     bool                  `json:"accounting_match"`
	Source              json.RawMessage       `json:"source"`
	ManifestSHA256      string                `json:"manifest_sha256"`
	OriginMs            int64                 `json:"origin_ms"`
	EndUs               int64                 `json:"end_us"`
	Selected            Totals                `json:"selected"`
	Engine              EngineTotals          `json:"engine"`
	EngineAvailable     bool                  `json:"engine_available"`
	States              map[State]int         `json:"states"`
	SourceTokensByState map[State]TokenTotals `json:"source_tokens_by_state"`
	Families            []FamilyReport        `json:"families"`
	Sessions            []SessionReport       `json:"sessions"`
	Requests            []RequestReport       `json:"requests"`
}

type FamilyReport struct {
	TraceID      string        `json:"trace_id"`
	OriginMs     int64         `json:"origin_ms"`
	BirthUs      int64         `json:"birth_us"`
	ArrivalUs    *int64        `json:"arrival_us"`
	CompletionUs *int64        `json:"completion_us"`
	MakespanUs   *int64        `json:"makespan_us"`
	States       map[State]int `json:"states"`
}

type SessionReport struct {
	TraceID        string        `json:"trace_id"`
	SessionID      string        `json:"session_id"`
	SpawnRequestID string        `json:"spawn_request_id,omitempty"`
	InferredJoinID string        `json:"inferred_join_request_id,omitempty"`
	ArrivalUs      *int64        `json:"arrival_us"`
	CompletionUs   *int64        `json:"completion_us"`
	States         map[State]int `json:"states"`
}

type RequestReport struct {
	RequestID          string               `json:"request_id"`
	TraceID            string               `json:"trace_id"`
	SessionID          string               `json:"session_id"`
	Step               int                  `json:"step"`
	StartMs            int64                `json:"start_ms"`
	EndMs              int64                `json:"end_ms"`
	PromptTokens       int                  `json:"prompt_tokens"`
	OutputTokens       int                  `json:"output_tokens"`
	State              State                `json:"state"`
	Reason             string               `json:"reason,omitempty"`
	ArrivalUs          *int64               `json:"arrival_us"`
	CompletionUs       *int64               `json:"completion_us"`
	TerminalUs         *int64               `json:"terminal_us"`
	TTFTUs             *float64             `json:"ttft_us"`
	E2EUs              *int64               `json:"e2e_us"`
	EngineE2EUs        *float64             `json:"engine_e2e_us"`
	EngineCompletionUs *float64             `json:"engine_completion_us"`
	DelayUs            int64                `json:"delay_us"`
	Prerequisites      []PrerequisiteReport `json:"prerequisites"`
}

type PrerequisiteReport struct {
	RequestID string `json:"request_id"`
	On        string `json:"on"`
}

func pointer[T any](v T) *T { return &v }

func (x *Replay) Summary() Report {
	c := x.corpus
	r := Report{FormatVersion: 1, JoinPolicy: x.policy,
		JoinAssumption: "join candidates are inferred from recorded timing, not observed causal barriers",
		Source:         append(json.RawMessage(nil), c.manifest...), ManifestSHA256: c.manifestSHA256,
		OriginMs: c.originMs, EndUs: x.endUs, Selected: c.totals, Engine: x.engine, EngineAvailable: x.engineAvailable,
		States: make(map[State]int), SourceTokensByState: make(map[State]TokenTotals)}
	r.Arrivals = x.arrivals
	if x.policy == ExplicitDAG {
		r.JoinAssumption = "explicit parent_ids are completion prerequisites; delays are recorded gaps after the latest parent completion"
	}
	for _, state := range []State{Pending, Scheduled, Issued, Completed, Failed, Blocked, Censored} {
		r.States[state] = 0
		r.SourceTokensByState[state] = TokenTotals{}
	}
	for i, req := range c.requests {
		s := x.states[i]
		r.States[s.state]++
		tokens := r.SourceTokensByState[s.state]
		tokens.PromptTokens += int64(req.prompt)
		tokens.OutputTokens += int64(req.output)
		r.SourceTokensByState[s.state] = tokens
		session := c.sessions[req.session]
		rr := RequestReport{RequestID: req.id, TraceID: c.families[session.family].id, SessionID: session.id, Step: req.step,
			StartMs: req.startMs, EndMs: req.endMs, PromptTokens: req.prompt, OutputTokens: req.output, State: s.state, Reason: s.reason,
			DelayUs: x.deps[i].delayUs, Prerequisites: []PrerequisiteReport{}}
		if s.sent {
			rr.ArrivalUs = pointer(s.arrivalUs)
		}
		if s.state == Completed {
			rr.CompletionUs, rr.E2EUs = pointer(s.terminalUs), pointer(s.terminalUs-s.arrivalUs)
			if ttft, ok := x.ttfts[req.id]; ok {
				rr.TTFTUs = pointer(ttft)
			}
			if e2e, ok := x.e2es[req.id]; ok {
				rr.EngineE2EUs = pointer(e2e)
			}
			if at, ok := x.metricCompletions[req.id]; ok {
				rr.EngineCompletionUs = pointer(at)
			}
		}
		if s.state == Completed || s.state == Failed || s.state == Blocked || s.state == Censored {
			rr.TerminalUs = pointer(s.terminalUs)
		}
		for _, p := range x.deps[i].prerequisites {
			on := "completion"
			if p.onSend {
				on = "send"
			}
			rr.Prerequisites = append(rr.Prerequisites, PrerequisiteReport{RequestID: c.requests[p.request].id, On: on})
		}
		r.Requests = append(r.Requests, rr)
	}
	for _, s := range c.sessions {
		sr := SessionReport{TraceID: c.families[s.family].id, SessionID: s.id, States: make(map[State]int)}
		if s.spawn >= 0 {
			sr.SpawnRequestID = c.requests[s.spawn].id
		}
		if s.join >= 0 {
			sr.InferredJoinID = c.requests[s.join].id
		}
		last := int64(0)
		for _, i := range s.requests {
			state := x.states[i]
			sr.States[state.state]++
			if state.sent && (sr.ArrivalUs == nil || state.arrivalUs < *sr.ArrivalUs) {
				sr.ArrivalUs = pointer(state.arrivalUs)
			}
			last = max(last, state.terminalUs)
		}
		if sr.States[Completed] == len(s.requests) {
			sr.CompletionUs = pointer(last)
		}
		r.Sessions = append(r.Sessions, sr)
	}
	for fi, f := range c.families {
		fr := FamilyReport{TraceID: f.id, OriginMs: f.originMs, BirthUs: x.birthUs[fi], States: make(map[State]int)}
		complete, last := true, int64(0)
		for _, si := range f.sessions {
			for _, i := range c.sessions[si].requests {
				s := x.states[i]
				fr.States[s.state]++
				complete = complete && s.state == Completed
				last = max(last, s.terminalUs)
				if s.sent && (fr.ArrivalUs == nil || s.arrivalUs < *fr.ArrivalUs) {
					fr.ArrivalUs = pointer(s.arrivalUs)
				}
			}
		}
		if complete {
			fr.CompletionUs, fr.MakespanUs = pointer(last), pointer(last-*fr.ArrivalUs)
		}
		r.Families = append(r.Families, fr)
	}
	r.Complete = r.States[Completed] == c.totals.Requests
	r.AccountingMatch = x.finished && x.engine.CompletedRequests == c.totals.Requests && x.engine.PromptTokens == c.totals.PromptTokens && x.engine.OutputTokens == c.totals.OutputTokens
	return r
}
