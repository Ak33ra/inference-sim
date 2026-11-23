package cluster

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/inference-sim/inference-sim/sim"
)

const PolicyRequestSplit = "request-split"

type requestServiceShape struct {
	InputTokens     int64  `json:"input_tokens"`
	OutputTokens    int    `json:"output_tokens"`
	ServiceUs       int64  `json:"service_us"`
	PreparationUs   int64  `json:"preparation_us"`
	OracleRequestID string `json:"oracle_request_id"`
}

type requestServiceReference struct {
	TraceID          string `json:"trace_id"`
	SessionID        string `json:"session_id"`
	RuntimeSessionID string `json:"runtime_session_id"`
	Step             int    `json:"step"`
	Shape            string `json:"shape"`
}

type requestServiceManifest struct {
	FormatVersion  int                                `json:"format_version"`
	TraceSHA256    string                             `json:"trace_sha256"`
	BinarySHA256   string                             `json:"binary_sha256"`
	EngineSHA256   string                             `json:"engine_sha256"`
	ReferenceLabel string                             `json:"reference_label"`
	SourceHashes   map[string]string                  `json:"source_hashes"`
	Shapes         map[string]requestServiceShape     `json:"shapes"`
	Requests       map[string]requestServiceReference `json:"requests"`
}

type RequestSplitGateway struct{ *splitGateway }

func NewRequestSplitGateway(manifestPath string, latencyUs int64) (*RequestSplitGateway, error) {
	if latencyUs < 0 {
		return nil, fmt.Errorf("request-split latency must be nonnegative")
	}
	m, err := loadRequestServiceManifest(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("request-split manifest: %w", err)
	}
	return newRequestSplitGateway(m, latencyUs), nil
}

func newRequestSplitGateway(m requestServiceManifest, latencyUs int64) *RequestSplitGateway {
	return &RequestSplitGateway{newSplitGateway(PolicyRequestSplit, latencyUs, func(req *sim.Request) (splitJob, error) {
		r, ok := m.Requests[req.ID]
		if !ok || r.RuntimeSessionID != req.SessionID || r.Step != req.RoundIndex {
			return splitJob{}, fmt.Errorf("request-split unknown request identity %q", req.ID)
		}
		shape := m.Shapes[r.Shape]
		if req.InputLen() != shape.InputTokens || len(req.OutputTokens) != shape.OutputTokens || req.MaxOutputLen != max(1, shape.OutputTokens) {
			return splitJob{}, fmt.Errorf("request-split request %q differs from its calibrated input/output shape or output budget", req.ID)
		}
		return splitJob{priorityUs: shape.ServiceUs, jobKey: req.ID}, nil
	})}
}

func ValidateRequestSplitDeployment(c DeploymentConfig) error {
	return validateSplitDeployment(c, PolicyRequestSplit)
}

func loadRequestServiceManifest(path string) (requestServiceManifest, error) {
	var m requestServiceManifest
	f, err := os.Open(path)
	if err != nil {
		return m, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return m, err
	}
	if d.Decode(new(any)) != io.EOF {
		return m, fmt.Errorf("service manifest requires one JSON object")
	}
	if m.FormatVersion != 1 || m.ReferenceLabel == "" || len(m.Requests) == 0 || len(m.Shapes) == 0 || len(m.SourceHashes) == 0 {
		return m, fmt.Errorf("invalid service manifest metadata")
	}
	hashes := []string{m.TraceSHA256, m.BinarySHA256, m.EngineSHA256}
	for path, hash := range m.SourceHashes {
		if path == "" {
			return m, fmt.Errorf("empty service manifest source path")
		}
		hashes = append(hashes, hash)
	}
	for _, hash := range hashes {
		if _, err := hex.DecodeString(hash); err != nil || len(hash) != 64 {
			return m, fmt.Errorf("invalid service manifest hash")
		}
	}
	for key, shape := range m.Shapes {
		if shape.InputTokens <= 0 || shape.OutputTokens < 0 || shape.ServiceUs <= 0 || shape.PreparationUs < 0 || shape.OracleRequestID == "" || key != fmt.Sprintf("%d/%d", shape.InputTokens, shape.OutputTokens) {
			return m, fmt.Errorf("invalid service shape %q", key)
		}
	}
	identities := map[struct {
		session string
		step    int
	}]bool{}
	used := map[string]bool{}
	for id, r := range m.Requests {
		_, exists := m.Shapes[r.Shape]
		key := struct {
			session string
			step    int
		}{r.RuntimeSessionID, r.Step}
		if id == "" || r.TraceID == "" || r.SessionID == "" || r.RuntimeSessionID != r.TraceID+"/"+r.SessionID || r.Step < 0 || !exists || identities[key] {
			return m, fmt.Errorf("invalid service request reference %q", id)
		}
		identities[key], used[r.Shape] = true, true
	}
	if len(used) != len(m.Shapes) {
		return m, fmt.Errorf("service manifest contains unused shapes")
	}
	return m, nil
}
