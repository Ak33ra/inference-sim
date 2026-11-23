package sim

import (
	"fmt"
	"math"
	"sort"
)

type ProgramID string

type ResearchSchedulerConfig struct {
	QuantaMS        []float64 `json:"quanta_ms"`
	StarvationRatio float64   `json:"starvation_ratio"`
}

func DefaultResearchSchedulerConfig() ResearchSchedulerConfig {
	return ResearchSchedulerConfig{[]float64{100, 200, 400, 800, 1600, 3200, 6400, 12800}, 10}
}

func (c ResearchSchedulerConfig) Validate() error {
	if len(c.QuantaMS) == 0 || len(c.QuantaMS) > 64 {
		return fmt.Errorf("quanta_ms must have 1..64 entries")
	}
	var total float64
	for _, q := range c.QuantaMS {
		if math.IsNaN(q) || math.IsInf(q, 0) || q < .001 {
			return fmt.Errorf("quanta_ms entries must be finite and at least 0.001")
		}
		total += q
	}
	if total > float64(math.MaxInt64)/1000 {
		return fmt.Errorf("quanta_ms overflow microseconds")
	}
	if math.IsNaN(c.StarvationRatio) || math.IsInf(c.StarvationRatio, 0) || c.StarvationRatio <= 0 {
		return fmt.Errorf("starvation_ratio must be finite and positive")
	}
	return nil
}

type ResearchCallStats struct {
	ProgramID          ProgramID `json:"program_id"`
	InheritedServiceUs int64     `json:"inherited_service_us"`
	ModelServiceUs     int64     `json:"model_service_us"`
	QueueWaitUs        int64     `json:"queue_wait_us"`
	Demotions          int64     `json:"demotions"`
	Promotions         int64     `json:"promotions"`
	Displacements      int64     `json:"displacements"`
	Resumes            int64     `json:"resumes"`
	MemoryPreemptions  int64     `json:"memory_preemptions"`
	RecomputedTokens   int64     `json:"recomputed_tokens"`
	Completed          bool      `json:"completed"`
}
type ResearchProgramStats struct {
	ServiceUs int64 `json:"service_us"`
	WaitUs    int64 `json:"wait_us"`
}
type ResearchSchedulerReport struct {
	PrefixRegistration string                             `json:"prefix_registration"`
	BottomQueue        string                             `json:"bottom_queue"`
	ProgramWaitUpdate  string                             `json:"program_wait_update"`
	Mode               string                             `json:"mode"`
	Backend            string                             `json:"backend"`
	Config             ResearchSchedulerConfig            `json:"config"`
	Requests           map[string]*ResearchCallStats      `json:"requests"`
	Programs           map[ProgramID]ResearchProgramStats `json:"programs"`
}
type serviceReceipt struct {
	at            int64
	program       ProgramID
	service, wait int64
}

type ResearchSchedulerRuntime struct {
	Report  ResearchSchedulerReport
	pending []serviceReceipt
}

func NewResearchSchedulerRuntime(mode string, c ResearchSchedulerConfig) (*ResearchSchedulerRuntime, error) {
	if mode != "mlfq" && mode != "atlas" {
		return nil, fmt.Errorf("research scheduler must be mlfq or atlas")
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &ResearchSchedulerRuntime{Report: ResearchSchedulerReport{Mode: mode, Backend: "kv-recompute", BottomQueue: "fifo-renewal", ProgramWaitUpdate: "max-inherited-plus-call", PrefixRegistration: "after-batch-execution", Config: c, Requests: map[string]*ResearchCallStats{}, Programs: map[ProgramID]ResearchProgramStats{}}}, nil
}
func (r *ResearchSchedulerRuntime) flush(now int64) {
	sort.SliceStable(r.pending, func(i, j int) bool { return r.pending[i].at < r.pending[j].at })
	n := 0
	for _, p := range r.pending {
		if p.at > now {
			break
		}
		old := r.Report.Programs[p.program]
		old.ServiceUs = max(old.ServiceUs, p.service)
		old.WaitUs = max(old.WaitUs, p.wait)
		r.Report.Programs[p.program] = old
		n++
	}
	r.pending = append(r.pending[:0], r.pending[n:]...)
}
func (r *ResearchSchedulerRuntime) Snapshot() *ResearchSchedulerReport {
	if r == nil {
		return nil
	}
	r.flush(math.MaxInt64)
	return &r.Report
}

type researchCall struct {
	stats                                       *ResearchCallStats
	level                                       int
	order                                       uint64
	remaining, model, wait, inheritedWait, last int64
	active                                      bool
	everScheduled                               bool
	recomputeEnd                                int64
}
type researchScheduler struct {
	runtime *ResearchSchedulerRuntime
	calls   map[string]*researchCall
	seq     uint64
	quanta  []int64
}

func newResearchScheduler(r *ResearchSchedulerRuntime) *researchScheduler {
	s := &researchScheduler{runtime: r, calls: map[string]*researchCall{}}
	for _, q := range r.Report.Config.QuantaMS {
		s.quanta = append(s.quanta, int64(math.Round(q*1000)))
	}
	return s
}
func (s *researchScheduler) admit(req *Request, now int64) {
	if _, ok := s.calls[req.ID]; ok {
		return
	}
	s.runtime.flush(now)
	inherited := ResearchProgramStats{}
	if s.runtime.Report.Mode == "atlas" {
		if req.ProgramID == "" {
			panic("ATLAS requires ProgramID")
		}
		inherited = s.runtime.Report.Programs[req.ProgramID]
	}
	level := 0
	bound := s.quanta[0]
	for level < len(s.quanta)-1 && inherited.ServiceUs >= bound {
		level++
		bound += s.quanta[level]
	}
	s.seq++
	stats := &ResearchCallStats{ProgramID: req.ProgramID, InheritedServiceUs: inherited.ServiceUs}
	s.calls[req.ID] = &researchCall{stats: stats, level: level, order: s.seq, remaining: s.quanta[level], last: now, inheritedWait: inherited.WaitUs}
	s.runtime.Report.Requests[req.ID] = stats
}
func (s *researchScheduler) account(c *researchCall, now int64) {
	if !c.active && now > c.last {
		d := now - c.last
		c.wait += d
		c.stats.QueueWaitUs += d
	}
	c.last = now
}
func (s *researchScheduler) rank(reqs []*Request, now int64) {
	s.runtime.flush(now)
	for _, r := range reqs {
		c := s.calls[r.ID]
		s.account(c, now)
		if c.remaining <= 0 {
			if c.level < len(s.quanta)-1 {
				c.level++
				c.stats.Demotions++
				s.seq++
				c.order = s.seq
			}
			c.remaining = s.quanta[c.level]
		}
		service, wait := c.model, c.wait
		if s.runtime.Report.Mode == "atlas" {
			p := s.runtime.Report.Programs[r.ProgramID]
			service += p.ServiceUs
			wait += p.WaitUs
		}
		if c.level > 0 && wait > 0 && (service == 0 || float64(wait)/float64(service) >= s.runtime.Report.Config.StarvationRatio) {
			c.level = 0
			c.remaining = s.quanta[0]
			c.model = 0
			c.wait = 0
			c.stats.Promotions++
			s.seq++
			c.order = s.seq
		}
	}
	sort.SliceStable(reqs, func(i, j int) bool {
		a, b := s.calls[reqs[i].ID], s.calls[reqs[j].ID]
		if a.level != b.level {
			return a.level < b.level
		}
		return a.order < b.order
	})
}
func (s *researchScheduler) form(ctx BatchContext, base BatchFormation) BatchResult {
	var running []*Request
	if ctx.RunningBatch != nil {
		running = ctx.RunningBatch.Requests
	}
	all := append(append([]*Request{}, running...), ctx.WaitQ.Items()...)
	s.rank(all, ctx.Now)
	selected := map[*Request]bool{}
	budget := ctx.MaxNumBatchedTokens
	cap := ctx.PrefillTokenThreshold
	if len(all) <= 1 {
		cap = 0
	}
	for _, r := range all {
		if int64(len(selected)) >= ctx.MaxNumSeqs || budget <= 0 {
			break
		}
		tokens := int64(1)
		start := r.ProgressIndex
		if r.State == StateQueued {
			start = int64(len(admissionCachedPrefix(r, ctx.KVCache, ctx.PrefixCachingDisabled))) * ctx.KVCache.BlockSize()
		}
		if n := r.PrefillEnd() - start; n > 0 {
			chunk := func(from int64) int64 {
				n := r.PrefillEnd() - from
				if cap > 0 {
					n = min(n, cap)
				}
				return min(n, budget)
			}
			tokens = chunk(start)
			if r.State == StateQueued {
				if rc, ok := ctx.KVCache.(PrefixHitReconciler); ok {
					replay := min(rc.PrefixReplayTokens(), start)
					if replay > 0 {
						if n := chunk(start - replay); n > replay {
							tokens = n
						}
					}
				}
			}
		}
		tokens = min(tokens, budget)
		selected[r] = true
		budget -= tokens
	}
	var displaced []PreemptedRequest
	kept := &Batch{}
	for _, r := range running {
		if selected[r] {
			continue
		}
		c := s.calls[r.ID]
		c.recomputeEnd = max(c.recomputeEnd, r.ProgressIndex)
		resetPreemptedRequest(r, ctx)
		c.stats.Displacements++
		c.active = false
		displaced = append(displaced, PreemptedRequest{Request: r})
	}
	chosen := &WaitQueue{}
	progress := make(map[*Request]int64, len(all))
	for _, r := range all {
		progress[r] = r.ProgressIndex
		if selected[r] && r.State == StateRunning {
			kept.Requests = append(kept.Requests, r)
		}
		if selected[r] && r.State == StateQueued {
			chosen.Enqueue(r)
		}
	}
	ctx.RunningBatch = kept
	original := ctx.WaitQ
	ctx.WaitQ = chosen
	ctx.PreservePrefillCap = true
	ctx.DeferKVRegistration = true
	ctx.PrefillTokenThreshold = cap
	ranks := make(map[*Request]int, len(all))
	for i, r := range all {
		ranks[r] = i
	}
	admissionDisplacements := map[*Request]bool{}
	ctx.AdmissionVictim = func(queued *Request, running []*Request) int {
		victim := -1
		for i, r := range running {
			if ranks[r] > ranks[queued] && (victim < 0 || ranks[r] > ranks[running[victim]]) {
				victim = i
			}
		}
		if victim >= 0 {
			r := running[victim]
			c := s.calls[r.ID]
			c.recomputeEnd = max(c.recomputeEnd, r.ProgressIndex)
			c.stats.Displacements++
			c.active = false
			admissionDisplacements[r] = true
		}
		return victim
	}
	result := base.FormBatch(ctx)
	live := map[*Request]bool{}
	for _, r := range result.RunningBatch.Requests {
		live[r] = true
	}
	*original = WaitQueue{}
	for _, r := range all {
		if !live[r] {
			r.State = StateQueued
			original.Enqueue(r)
		}
	}
	for _, p := range result.Preempted {
		c := s.calls[p.Request.ID]
		if !admissionDisplacements[p.Request] {
			c.stats.MemoryPreemptions++
		}
		c.recomputeEnd = max(c.recomputeEnd, progress[p.Request])
	}
	result.Preempted = append(displaced, result.Preempted...)
	for _, r := range all {
		c := s.calls[r.ID]
		active := live[r] && r.NumNewTokens > 0
		if active && !c.active && c.everScheduled {
			c.stats.Resumes++
		}
		c.active = active
		if active {
			c.everScheduled = true
		}
	}
	return result
}
func (s *researchScheduler) executed(reqs []*Request, end, model int64, computed map[string]int64) {
	for _, r := range reqs {
		c := s.calls[r.ID]
		c.model += model
		c.stats.ModelServiceUs += model
		c.remaining -= model
		c.last = end
		start := max(r.ProgressIndex, computed[r.ID]-int64(r.NumNewTokens))
		if c.recomputeEnd > start {
			c.stats.RecomputedTokens += min(int64(r.NumNewTokens), c.recomputeEnd-start)
		}
	}
}
func (s *researchScheduler) completed(r *Request, end int64) {
	c := s.calls[r.ID]
	c.stats.Completed = true
	if s.runtime.Report.Mode == "atlas" {
		s.runtime.pending = append(s.runtime.pending, serviceReceipt{end, r.ProgramID, c.stats.InheritedServiceUs + c.model, c.inheritedWait + c.wait})
	}
	delete(s.calls, r.ID)
}
