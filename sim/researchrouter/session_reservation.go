package researchrouter

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/inference-sim/inference-sim/sim"
	"github.com/inference-sim/inference-sim/sim/cluster"
)

type sessionReference struct {
	TraceID   string `json:"trace_id"`
	SessionID string `json:"session_id"`
	Class     string `json:"class"`
	ElapsedUs int64  `json:"reference_elapsed_us"`
	Run       string `json:"reference_run"`
}

type sessionClassManifest struct {
	FormatVersion int    `json:"format_version"`
	TraceSHA256   string `json:"trace_sha256"`
	Ranking       struct {
		Metric            string `json:"metric"`
		ReferenceLabel    string `json:"reference_label"`
		ReferenceRunCount int    `json:"reference_run_count"`
		ShortCount        int    `json:"short_count"`
		LongCount         int    `json:"long_count"`
		TiePolicy         string `json:"tie_policy"`
	} `json:"ranking"`
	Sessions map[string]sessionReference `json:"sessions"`
	Families map[string]struct {
		ElapsedUs int64  `json:"reference_elapsed_us"`
		Run       string `json:"reference_run"`
	} `json:"families"`
}

type sessionReservation struct {
	sessions map[string]sessionReference
	reserved map[cluster.InstanceID]bool
}

func (c Config) validateSessionReservation() error {
	if c.Policy != PolicySessionReservation {
		if c.SessionClassManifest != "" || len(c.ReservedInstances) != 0 {
			return fmt.Errorf("session_class_manifest and reserved_instances require session-reservation")
		}
		return nil
	}
	if c.SessionClassManifest == "" {
		return fmt.Errorf("session-reservation requires session_class_manifest")
	}
	if len(c.ReservedInstances) != 2 || !((c.ReservedInstances[0] == "instance_0" && c.ReservedInstances[1] == "instance_1") || (c.ReservedInstances[0] == "instance_1" && c.ReservedInstances[1] == "instance_0")) {
		return fmt.Errorf("session-reservation requires reserved_instances instance_0 and instance_1")
	}
	return nil
}

func loadSessionClassManifest(path string) (map[string]sessionReference, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var manifest sessionClassManifest
	if err := dec.Decode(&manifest); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("session manifest must contain one JSON object")
	}
	if manifest.FormatVersion != 1 || len(manifest.TraceSHA256) != 64 || len(manifest.Sessions) == 0 {
		return nil, fmt.Errorf("invalid session manifest version, trace hash, or empty sessions")
	}
	if _, err := hex.DecodeString(manifest.TraceSHA256); err != nil {
		return nil, fmt.Errorf("invalid session manifest trace hash")
	}
	rank := manifest.Ranking
	if rank.Metric != "minimum_session_elapsed_us_across_cache_off_isolated_family_runs" || rank.ReferenceRunCount < 1 || rank.ReferenceLabel == "" || rank.TiePolicy != "reference_elapsed_us_then_runtime_session_id" || rank.ShortCount < 1 || rank.LongCount < 1 || rank.ShortCount+rank.LongCount != len(manifest.Sessions) {
		return nil, fmt.Errorf("invalid session manifest ranking metadata")
	}
	ids := make([]string, 0, len(manifest.Sessions))
	for id, ref := range manifest.Sessions {
		if ref.TraceID == "" || ref.SessionID == "" || id != ref.TraceID+"/"+ref.SessionID || ref.ElapsedUs <= 0 || ref.Run == "" {
			return nil, fmt.Errorf("invalid session reference %q", id)
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := manifest.Sessions[ids[i]], manifest.Sessions[ids[j]]
		if a.ElapsedUs != b.ElapsedUs {
			return a.ElapsedUs < b.ElapsedUs
		}
		return ids[i] < ids[j]
	})
	for i, id := range ids {
		want := "long"
		if i < rank.ShortCount {
			want = "short"
		}
		if manifest.Sessions[id].Class != want {
			return nil, fmt.Errorf("session %q class disagrees with reference rank", id)
		}
	}
	return manifest.Sessions, nil
}

func (g *Gateway) startSessionReservation() error {
	sessions, err := loadSessionClassManifest(g.cfg.SessionClassManifest)
	if err != nil {
		return fmt.Errorf("session-reservation manifest: %w", err)
	}
	present := make(map[cluster.InstanceID]bool)
	for _, inst := range g.host.Instances() {
		present[inst.ID()] = true
	}
	reserved := make(map[cluster.InstanceID]bool)
	for _, id := range g.cfg.ReservedInstances {
		if !present[id] {
			return fmt.Errorf("session-reservation reserved instance %q does not exist", id)
		}
		reserved[id] = true
	}
	if len(present) <= len(reserved) {
		return fmt.Errorf("session-reservation requires a nonreserved instance")
	}
	g.reservation = &sessionReservation{sessions: sessions, reserved: reserved}
	return nil
}

func (g *Gateway) sessionReservation(req *sim.Request, hashes []string, ids []cluster.InstanceID) (cluster.InstanceID, string, int64, float64, map[string]float64) {
	ref, ok := g.reservation.sessions[string(req.SessionID)]
	if !ok || (ref.Class != "short" && ref.Class != "long") {
		g.host.Abort(fmt.Errorf("session-reservation request %q has unknown session class for %q", req.ID, req.SessionID))
		return "", "", 0, 0, nil
	}
	eligible := make([]cluster.InstanceID, 0, len(ids))
	for _, id := range ids {
		if ref.Class == "short" || !g.reservation.reserved[id] {
			eligible = append(eligible, id)
		}
	}
	if len(eligible) == 0 {
		g.host.Abort(fmt.Errorf("session-reservation session %q has no eligible routable instance", req.SessionID))
		return "", "", 0, 0, nil
	}
	id := g.minLoad(eligible, false)
	return id, "session-reservation-" + ref.Class, g.cachedTokens(id, hashes), float64(g.loads[id]), g.loadScores(eligible, false)
}
