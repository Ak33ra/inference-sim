package cluster

import (
	"encoding/json"
	"fmt"
	"math"
	"os"

	"github.com/inference-sim/inference-sim/sim"
)

type splitJob struct {
	priorityUs    int64
	jobKey        string
	predecessorID string
	refRun        string
}

type splitEntry struct {
	req                  *sim.Request
	job                  splitJob
	preparedAt           int64
	preparationInstance  InstanceID
	preparationServiceUs int64
	preparationWaitUs    int64
}
type splitActive struct {
	entry      *splitEntry
	terminalAt int64
}

type SplitEvent struct {
	Kind                 string                `json:"kind"`
	AtUs                 int64                 `json:"at_us"`
	RequestID            string                `json:"request_id"`
	SessionID            string                `json:"session_id"`
	RemainingUs          int64                 `json:"remaining_us,omitempty"`
	ServiceUs            int64                 `json:"service_us,omitempty"`
	ReferenceRun         string                `json:"reference_run,omitempty"`
	ArrivalUs            int64                 `json:"arrival_us"`
	PreparedUs           int64                 `json:"prepared_us"`
	PreparationInstance  InstanceID            `json:"preparation_instance"`
	PreparationServiceUs int64                 `json:"preparation_service_us"`
	PreparationWaitUs    int64                 `json:"preparation_wait_us"`
	Instance             InstanceID            `json:"instance,omitempty"`
	LargestFirst         bool                  `json:"largest_first"`
	ReadyCount           int                   `json:"ready_count"`
	Evidence             sim.AdmissionEvidence `json:"capacity"`
}

type splitGateway struct {
	policy          string
	lookup          func(*sim.Request) (splitJob, error)
	latencyUs       int64
	host            *ClusterSimulator
	ready           map[string]*splitEntry
	preparing       map[string]*splitEntry
	active          map[string]splitActive
	completed       map[string]int64
	seen            map[string]bool
	events          []SplitEvent
	nextPreparation int
}

func validateSplitDeployment(c DeploymentConfig, policy string) error {
	if policy == PolicyRequestSplit && c.NumInstances != 8 {
		return fmt.Errorf("request-split requires exactly eight replicas")
	}
	if c.Offload.IsEnabled() || c.KVCPUBlocks != 0 || c.SpeculativeConfig.IsEnabled() || c.HasAdapters() {
		return fmt.Errorf("%s does not support offload, speculation or adapters", policy)
	}
	if c.PreemptionPolicy != "" && c.PreemptionPolicy != "fcfs" {
		return fmt.Errorf("%s requires FCFS preemption configuration", policy)
	}
	if !c.PrefixCachingDisabled || c.Scheduler != "" && c.Scheduler != "fcfs" || c.ResearchScheduler != nil || c.Horizon != math.MaxInt64 {
		return fmt.Errorf("%s requires cache off, FCFS and a complete-drain horizon", policy)
	}
	if c.DPLockstepRanks > 1 || len(c.NodePools) > 0 || c.ModelAutoscalerIntervalUs != 0 || c.PrefillInstances != 0 || c.DecodeInstances != 0 || c.SharedInstances != 0 || c.EncodeInstances != 0 {
		return fmt.Errorf("%s requires homogeneous independent colocated engines without autoscaling", policy)
	}
	l := c.InstanceLifecycle
	if l.LoadingDelay.Mean != 0 || l.LoadingDelay.Stddev != 0 || l.WarmUpRequestCount != 0 || l.DrainPolicy != "" {
		return fmt.Errorf("%s does not support instance lifecycle changes", policy)
	}
	return nil
}

func (q *splitGateway) Start(host GatewayHost) error {
	cs, ok := host.(*ClusterSimulator)
	if !ok {
		return fmt.Errorf("%s requires the native cluster host", q.policy)
	}
	if err := validateSplitDeployment(cs.config, q.policy); err != nil {
		return err
	}
	if cs.instanceByID("instance_0") == nil {
		return fmt.Errorf("%s requires instance_0", q.policy)
	}
	q.host = cs
	for _, inst := range cs.instances {
		if err := inst.sim.SetAdmissionSource(&splitSource{queue: q, instance: inst}); err != nil {
			return fmt.Errorf("%s %s: %w", q.policy, inst.ID(), err)
		}
	}
	return nil
}

func (q *splitGateway) OnArrival(req *sim.Request) {
	job, err := q.lookup(req)
	if err != nil {
		q.host.Abort(err)
		return
	}
	if q.seen[req.ID] {
		q.host.Abort(fmt.Errorf("%s duplicate request identity %q", q.policy, req.ID))
		return
	}
	q.seen[req.ID] = true
	req.GatewayEnqueueTime = q.host.Now()
	endpoint := q.host.instances[q.nextPreparation]
	q.nextPreparation = (q.nextPreparation + 1) % len(q.host.instances)
	delay, err := endpoint.sim.PrepareCentralAdmission(req)
	if err != nil {
		q.host.Abort(err)
		return
	}
	arrival := q.host.Now() + q.latencyUs
	done := arrival + delay
	if renderer := endpoint.sim.Renderer(); renderer != nil {
		done = renderer.Admit(arrival, delay)
	}
	e := &splitEntry{req: req, job: job, preparedAt: done, preparationInstance: endpoint.ID(), preparationServiceUs: delay, preparationWaitUs: done - arrival - delay}
	q.preparing[req.ID] = e
	q.record("prepare", e, q.host.Now(), "", sim.AdmissionEvidence{})
	q.host.Schedule(e.preparedAt, PriorityGatewayRequest, false, func() {
		if job.predecessorID != "" {
			at, done := q.completed[job.predecessorID]
			if !done || at > req.ArrivalTime {
				q.host.Abort(fmt.Errorf("%s predecessor was not complete at ready time for %s", q.policy, req.ID))
				return
			}
		}
		if _, busy := q.active[job.jobKey]; busy {
			q.host.Abort(fmt.Errorf("%s overlapping session %s", q.policy, req.SessionID))
			return
		}
		for _, other := range q.ready {
			if other.job.jobKey == job.jobKey {
				q.host.Abort(fmt.Errorf("%s concurrent ready turns for %s", q.policy, req.SessionID))
				return
			}
		}
		delete(q.preparing, req.ID)
		q.ready[req.ID] = e
		q.record("ready", e, q.host.Now(), "", sim.AdmissionEvidence{})
		for _, inst := range q.host.instances {
			inst.sim.NotifyAdmissionReady(q.host.Now())
		}
	})
}

func (q *splitGateway) OnDispatched(*sim.Request, int64) {}
func (q *splitGateway) OnFirstToken(*sim.Request, int64) {}
func (q *splitGateway) OnDone(req *sim.Request, at int64, outcome DoneOutcome) {
	job, err := q.lookup(req)
	if err != nil {
		q.host.Abort(err)
		return
	}
	a, ok := q.active[job.jobKey]
	if !ok || a.entry.req != req || a.terminalAt != 0 || outcome != DoneCompleted {
		q.host.Abort(fmt.Errorf("%s unexpected terminal request %s", q.policy, req.ID))
		return
	}
	a.terminalAt = at
	q.active[job.jobKey] = a
	q.host.Schedule(at, PriorityGatewayRequest, false, func() {
		delete(q.active, job.jobKey)
		q.completed[req.ID] = at
		q.record("complete", a.entry, at, InstanceID(req.AssignedInstance), sim.AdmissionEvidence{})
	})
}
func (q *splitGateway) OnInstanceChange(*InstanceSimulator) {
	q.host.Abort(fmt.Errorf("%s does not support instance changes", q.policy))
}
func (q *splitGateway) Pending() int { return len(q.ready) + len(q.preparing) }
func (q *splitGateway) Stop()        {}

type splitReservationStats struct {
	FinalUsedKVBlocks int64 `json:"final_used_kv_blocks"`
	sim.LifecycleReservationStats
	Preemptions        int64 `json:"preemptions"`
	AllocationFailures int64 `json:"allocation_failures"`
}

type splitRendererStats struct {
	Instance   InstanceID `json:"instance"`
	APIServers int        `json:"api_servers"`
	Workers    int        `json:"workers"`
	Rendered   int64      `json:"rendered"`
	WaitUs     int64      `json:"wait_us"`
}

func (q *splitGateway) WriteAudit(path string) error {
	stats := map[InstanceID]splitReservationStats{}
	renderers := []splitRendererStats{}
	if q.host != nil {
		for _, inst := range q.host.instances {
			stats[inst.ID()] = splitReservationStats{FinalUsedKVBlocks: inst.sim.KVCache.UsedBlocks(), LifecycleReservationStats: inst.sim.KVCache.(sim.LifecycleKVStore).LifecycleStats(), Preemptions: inst.sim.Metrics.PreemptionCount, AllocationFailures: inst.sim.Metrics.KVAllocationFailures}
			r := splitRendererStats{Instance: inst.ID(), APIServers: max(1, q.host.config.Frontend.APIServerCount), Workers: q.host.config.Frontend.RendererNumWorkers}
			if renderer := inst.sim.Renderer(); renderer != nil {
				r.Rendered, r.WaitUs = renderer.Rendered(), renderer.WaitTicks()
			}
			renderers = append(renderers, r)
		}
	}
	b, err := json.MarshalIndent(struct {
		FormatVersion   int                                  `json:"format_version"`
		Policy          string                               `json:"policy"`
		Events          []SplitEvent                         `json:"events"`
		Reservations    map[InstanceID]splitReservationStats `json:"reservations"`
		RouterLatencyUs int64                                `json:"router_latency_us"`
		Renderers       []splitRendererStats                 `json:"renderers"`
	}{1, q.policy, q.events, stats, q.latencyUs, renderers}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}

func (q *splitGateway) record(kind string, e *splitEntry, at int64, id InstanceID, ev sim.AdmissionEvidence) {
	event := SplitEvent{Kind: kind, AtUs: at, RequestID: e.req.ID, SessionID: e.req.SessionID, ReferenceRun: e.job.refRun, ArrivalUs: e.req.ArrivalTime, PreparedUs: e.preparedAt, PreparationInstance: e.preparationInstance, PreparationServiceUs: e.preparationServiceUs, PreparationWaitUs: e.preparationWaitUs, Instance: id, LargestFirst: id == "instance_0", ReadyCount: len(q.ready), Evidence: ev}
	if q.policy == PolicySessionSplit {
		event.RemainingUs = e.job.priorityUs
	} else {
		event.ServiceUs = e.job.priorityUs
	}
	q.events = append(q.events, event)
}

type splitSource struct {
	queue    *splitGateway
	instance *InstanceSimulator
}

func (s *splitSource) Len() int { return len(s.queue.ready) }
func (s *splitSource) Peek() *sim.Request {
	var best *splitEntry
	for _, e := range s.queue.ready {
		if best == nil || splitBefore(e, best, s.instance.ID() == "instance_0") {
			best = e
		}
	}
	if best == nil {
		return nil
	}
	return best.req
}
func splitBefore(a, b *splitEntry, largest bool) bool {
	if a.job.priorityUs != b.job.priorityUs {
		if largest {
			return a.job.priorityUs > b.job.priorityUs
		}
		return a.job.priorityUs < b.job.priorityUs
	}
	if a.req.ArrivalTime != b.req.ArrivalTime {
		return a.req.ArrivalTime < b.req.ArrivalTime
	}
	return a.req.ID < b.req.ID
}
func (s *splitSource) Admit(req *sim.Request, at int64, ev sim.AdmissionEvidence) {
	q := s.queue
	e := q.ready[req.ID]
	if e == nil || e.req != req {
		panic("central claim lost ownership: " + req.ID)
	}
	q.record("claim", e, at, s.instance.ID(), ev)
	delete(q.ready, req.ID)
	q.active[e.job.jobKey] = splitActive{entry: e}
	req.GatewayDispatchTime = at
	req.AssignedInstance = string(s.instance.ID())
	q.host.recordRouting(req, Placement{Decode: s.instance.ID()})
	q.host.inFlightRequests[req.AssignedInstance]++
}

func newSplitGateway(policy string, latencyUs int64, lookup func(*sim.Request) (splitJob, error)) *splitGateway {
	return &splitGateway{policy: policy, lookup: lookup, latencyUs: latencyUs, ready: map[string]*splitEntry{}, preparing: map[string]*splitEntry{}, active: map[string]splitActive{}, completed: map[string]int64{}, seen: map[string]bool{}}
}
