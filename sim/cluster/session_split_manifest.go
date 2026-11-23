package cluster

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

type splitReference struct {
	ReferenceArrivalUs           int64  `json:"reference_arrival_us"`
	ReferenceSessionCompletionUs int64  `json:"reference_session_completion_us"`
	TraceID                      string `json:"trace_id"`
	SessionID                    string `json:"session_id"`
	RuntimeSessionID             string `json:"runtime_session_id"`
	Step                         int    `json:"step"`
	RemainingUs                  int64  `json:"remaining_us"`
	ReferenceRun                 string `json:"reference_run"`
	ReferenceElapsedUs           int64  `json:"reference_elapsed_us"`
	PreviousRequestID            string `json:"previous_request_id"`
}

type splitManifest struct {
	FormatVersion  int                       `json:"format_version"`
	TraceSHA256    string                    `json:"trace_sha256"`
	ReferenceLabel string                    `json:"reference_label"`
	SourceHashes   map[string]string         `json:"source_hashes"`
	Requests       map[string]splitReference `json:"requests"`
}

func loadSplitManifest(path string) (splitManifest, error) {
	var m splitManifest
	f, err := os.Open(path)
	if err != nil {
		return m, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err = d.Decode(&m); err != nil {
		return m, err
	}
	if d.Decode(new(any)) != io.EOF {
		return m, fmt.Errorf("remaining manifest requires one JSON object")
	}
	if m.FormatVersion != 1 || len(m.TraceSHA256) != 64 || m.ReferenceLabel == "" || len(m.Requests) == 0 {
		return m, fmt.Errorf("invalid remaining manifest metadata")
	}
	if _, err = hex.DecodeString(m.TraceSHA256); err != nil {
		return m, err
	}
	bySession := map[string][]string{}
	for id, r := range m.Requests {
		if id == "" || r.TraceID == "" || r.SessionID == "" || r.RuntimeSessionID != r.TraceID+"/"+r.SessionID || r.Step < 0 || r.RemainingUs <= 0 || r.ReferenceElapsedUs <= 0 || r.ReferenceRun == "" || r.ReferenceSessionCompletionUs-r.ReferenceArrivalUs != r.RemainingUs {
			return m, fmt.Errorf("invalid remaining reference %q", id)
		}
		bySession[r.RuntimeSessionID] = append(bySession[r.RuntimeSessionID], id)
	}
	for session, ids := range bySession {
		sort.Slice(ids, func(i, j int) bool { return m.Requests[ids[i]].Step < m.Requests[ids[j]].Step })
		for i, id := range ids {
			r := m.Requests[id]
			if i == 0 {
				if r.PreviousRequestID != "" || r.RemainingUs != r.ReferenceElapsedUs {
					return m, fmt.Errorf("first remaining reference differs for %s", session)
				}
			} else {
				p := m.Requests[ids[i-1]]
				if r.PreviousRequestID != ids[i-1] || r.Step <= p.Step || r.RemainingUs > p.RemainingUs || r.ReferenceRun != p.ReferenceRun || r.ReferenceElapsedUs != p.ReferenceElapsedUs || r.ReferenceSessionCompletionUs != p.ReferenceSessionCompletionUs {
					return m, fmt.Errorf("nonsequential remaining references for %s", session)
				}
			}
		}
	}
	return m, nil
}
