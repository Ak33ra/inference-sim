package cluster

import (
	"fmt"
	"github.com/inference-sim/inference-sim/sim"
)

const PolicySessionSplit = "session-split"

type SessionSplitGateway struct {
	*splitGateway
	manifest splitManifest
}

func NewSessionSplitGateway(manifestPath string, latencyUs int64) (*SessionSplitGateway, error) {
	if latencyUs < 0 {
		return nil, fmt.Errorf("session-split latency must be nonnegative")
	}
	m, err := loadSplitManifest(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("session-split manifest: %w", err)
	}
	return newSessionSplitGateway(m, latencyUs), nil
}

func newSessionSplitGateway(m splitManifest, latencyUs int64) *SessionSplitGateway {
	q := &SessionSplitGateway{manifest: m}
	q.splitGateway = newSplitGateway(PolicySessionSplit, latencyUs, func(req *sim.Request) (splitJob, error) {
		r, ok := q.manifest.Requests[req.ID]
		if !ok || r.RuntimeSessionID != req.SessionID || r.Step != req.RoundIndex {
			return splitJob{}, fmt.Errorf("session-split unknown request identity %q", req.ID)
		}
		return splitJob{priorityUs: r.RemainingUs, jobKey: req.SessionID, predecessorID: r.PreviousRequestID, refRun: r.ReferenceRun}, nil
	})
	return q
}

func ValidateSessionSplitDeployment(c DeploymentConfig) error {
	return validateSplitDeployment(c, PolicySessionSplit)
}
