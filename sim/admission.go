package sim

import "fmt"

// AdmissionSource holds prepared requests outside an engine. Admit transfers ownership
// only after the native batcher successfully allocates the first chunk.
type AdmissionSource interface {
	Peek() *Request
	Len() int
	Admit(req *Request, at int64, evidence AdmissionEvidence)
}

type AdmissionEvidence struct {
	BatchSize        int   `json:"batch_size"`
	ScheduledTokens  int64 `json:"scheduled_tokens"`
	UsedKVBlocks     int64 `json:"used_kv_blocks"`
	PromisedKVBlocks int64 `json:"promised_kv_blocks"`
	TotalKVBlocks    int64 `json:"total_kv_blocks"`
}

// LifecycleKVStore guarantees that every admitted request can finish without eviction.
// Promises constrain admission but do not allocate physical blocks or compute tokens.
type LifecycleKVStore interface {
	EnableLifecycleReservations() error
	LifecycleBlocks(req *Request) int64
	PromisedBlocks() int64
	LifecycleStats() LifecycleReservationStats
}

type LifecycleReservationStats struct {
	FinalPromisedBlocks int64 `json:"final_promised_blocks"`
	TotalKVBlocks       int64 `json:"total_kv_blocks"`
	AdmissionRefusals   int64 `json:"admission_refusals"`
	PeakPromisedBlocks  int64 `json:"peak_promised_blocks"`
}

type registeredAdmission struct {
	sim    *Simulator
	source AdmissionSource
}

func (a *registeredAdmission) Peek() *Request { return a.source.Peek() }
func (a *registeredAdmission) Len() int       { return a.source.Len() }
func (a *registeredAdmission) Admit(req *Request, at int64, evidence AdmissionEvidence) {
	s := a.sim
	if _, exists := s.Metrics.Requests[req.ID]; exists {
		panic("central admission registered twice: " + req.ID)
	}
	a.source.Admit(req, at, evidence)
	rm := NewRequestMetrics(req, float64(req.ArrivalTime)/1e6)
	rm.GatewayQueueDelay = float64(at-req.GatewayEnqueueTime) / 1000
	s.Metrics.Requests[req.ID] = rm
	s.Metrics.TotalInputTokens += int(req.InputLen())
	req.Priority = float64(s.sloMap.InvertForVLLM(req.SLOClass))
}

func (s *Simulator) SetAdmissionSource(source AdmissionSource) error {
	if s.stepCount != 0 || s.WaitQ.Len() != 0 || (s.RunningBatch != nil && len(s.RunningBatch.Requests) != 0) || len(s.Metrics.Requests) != 0 || s.HasPendingEvents() {
		return fmt.Errorf("central admission requires a fresh engine")
	}
	if source == nil || s.admissionSource != nil {
		return fmt.Errorf("central admission requires one non-nil source")
	}
	if s.renderer != nil && s.renderer.Rendered() != 0 {
		return fmt.Errorf("central admission requires a fresh renderer")
	}
	if !s.prefixCachingDisabled || s.researchScheduler != nil || s.specEnabled || s.residentAdapters != nil || s.lockstep != nil || s.remoteKVAdmission != nil {
		return fmt.Errorf("central admission requires cache-off independent engines without research scheduling, speculation, adapters or P/D")
	}
	if _, ok := s.scheduler.(*FCFSScheduler); !ok {
		return fmt.Errorf("central admission requires FCFS")
	}
	store, ok := s.KVCache.(LifecycleKVStore)
	if !ok {
		return fmt.Errorf("central admission requires single-tier lifecycle KV reservations")
	}
	if err := store.EnableLifecycleReservations(); err != nil {
		return err
	}
	s.admissionSource = &registeredAdmission{sim: s, source: source}
	return nil
}

// PrepareCentralAdmission charges the same uncontended host preparation as ArrivalEvent.
// The caller retains ownership and schedules the returned delay before publishing readiness.
func (s *Simulator) PrepareCentralAdmission(req *Request) (int64, error) {
	if s.admissionSource == nil {
		return 0, fmt.Errorf("central admission is not enabled")
	}
	if req.Deadline != 0 || req.Adapter != "" || req.IsDecodeSubRequest || req.RetainKVOnFinish || req.ProgressIndex != 0 {
		return 0, fmt.Errorf("central admission does not support deadlines, adapters, P/D or resumed requests: %s", req.ID)
	}
	if why := normalizeAdmission(req, s.maxModelLen, s.KVCache.BlockSize(), s.KVCache.TotalCapacity()); why != nil {
		return 0, fmt.Errorf("%s", why.reason)
	}
	if req.MaxOutputLen <= 0 {
		return 0, fmt.Errorf("central admission requires a bounded output budget: %s", req.ID)
	}
	if blocks := s.KVCache.(LifecycleKVStore).LifecycleBlocks(req); blocks > s.KVCache.TotalCapacity() {
		return 0, fmt.Errorf("central admission request %s requires %d lifecycle blocks, capacity %d", req.ID, blocks, s.KVCache.TotalCapacity())
	}
	return s.latencyModel.QueueingTime(req), nil
}

func (s *Simulator) NotifyAdmissionReady(at int64) {
	if s.admissionSource != nil && s.admissionSource.Len() > 0 {
		s.scheduleStepBy(at)
	}
}
func (s *Simulator) hasAdmissionReady() bool {
	return s.admissionSource != nil && s.admissionSource.Len() > 0
}

type admissionRefusal struct {
	reason          string
	contextOverflow bool
}

func normalizeAdmission(r *Request, maxModelLen, blockSize, capacity int64) *admissionRefusal {
	if r.MaxOutputLen == 0 && maxModelLen > 0 && r.InputLen() < maxModelLen {
		r.MaxOutputLen = int(maxModelLen - r.InputLen())
	}
	if r.MaxOutputLen < 0 {
		return &admissionRefusal{fmt.Sprintf("dropping request %s: MaxOutputLen %d is negative", r.ID, r.MaxOutputLen), false}
	}
	if maxModelLen > 0 {
		if r.InputLen() >= maxModelLen {
			return &admissionRefusal{fmt.Sprintf("dropping request %s: input length %d >= MaxModelLen %d (no room for output)", r.ID, r.InputLen(), maxModelLen), true}
		}
		if r.MaxOutputLen > 0 && r.InputLen()+int64(r.MaxOutputLen) > maxModelLen {
			return &admissionRefusal{fmt.Sprintf("dropping request %s: total sequence length %d (input=%d + budget=%d) exceeds MaxModelLen %d", r.ID, r.InputLen()+int64(r.MaxOutputLen), r.InputLen(), r.MaxOutputLen, maxModelLen), true}
		}
	}
	if blocks := (r.InputLen() + blockSize - 1) / blockSize; blocks > capacity {
		return &admissionRefusal{fmt.Sprintf("dropping request %s: input requires %d KV blocks but cache has only %d total", r.ID, blocks, capacity), false}
	}
	return nil
}
