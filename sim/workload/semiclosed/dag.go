package semiclosed

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"os/exec"
	"strings"

	"github.com/inference-sim/inference-sim/sim"
)

type dagTrace struct {
	ID          string    `json:"id"`
	BlockSize   int       `json:"block_size"`
	HashIDScope string    `json:"hash_id_scope"`
	Models      []string  `json:"models"`
	Requests    []dagCall `json:"requests"`
}

type dagCall struct {
	ID        *int      `json:"req_id"`
	Parents   []int     `json:"parent_ids"`
	Type      string    `json:"type"`
	Time      *float64  `json:"t"`
	APITime   *float64  `json:"api_time"`
	ThinkTime float64   `json:"think_time"`
	Input     *int      `json:"in"`
	Output    *int      `json:"out"`
	Model     string    `json:"model"`
	Hashes    []int64   `json:"hash_ids"`
	AgentID   string    `json:"agent_id"`
	Requests  []dagCall `json:"requests"`
}

// LoadInput accepts audited FreeInference directories or explicit-DAG Weka files.
func LoadInput(path string) (*Corpus, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return Load(path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var reader io.Reader = f
	var cmd *exec.Cmd
	if strings.HasSuffix(path, ".zst") {
		cmd = exec.Command("zstd", "-dc")
		cmd.Stdin = f
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("compressed DAG input requires zstd: %w", err)
		}
		reader = pipe
		defer pipe.Close()
	}
	c, readErr := readDAG(reader)
	if cmd != nil {
		if readErr != nil {
			_ = cmd.Process.Kill()
		}
		if err := cmd.Wait(); err != nil && readErr == nil {
			return nil, fmt.Errorf("decompress DAG input: %w", err)
		}
	}
	return c, readErr
}

func readDAG(reader io.Reader) (*Corpus, error) {
	hash := sha256.New()
	dec := json.NewDecoder(io.TeeReader(reader, hash))
	c := &Corpus{explicitDeps: []dependency{}, rootOffsetsUs: make(map[int]int64)}
	seen := make(map[string]bool)
	nextToken := int64(1)
	model := ""
	for {
		var tr dagTrace
		if err := dec.Decode(&tr); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("DAG row %d: %w", len(c.families)+1, err)
		}
		if tr.ID == "" || seen[tr.ID] || tr.BlockSize != 64 || tr.HashIDScope != "local" || len(tr.Models) != 1 || tr.Models[0] == "" {
			return nil, fmt.Errorf("DAG row %d requires a unique id, local 64-token blocks, and one model", len(c.families)+1)
		}
		seen[tr.ID] = true
		if model != "" && model != tr.Models[0] {
			return nil, fmt.Errorf("DAG input contains multiple models; current deployment serves one model")
		}
		model = tr.Models[0]
		if err := c.addDAGFamily(tr, &nextToken); err != nil {
			return nil, fmt.Errorf("family %s: %w", tr.ID, err)
		}
	}
	if len(c.requests) == 0 {
		return nil, fmt.Errorf("DAG input has no requests")
	}
	c.totals.Families, c.totals.Sessions, c.totals.Requests = len(c.families), len(c.sessions), len(c.requests)
	c.manifest, _ = json.Marshal(map[string]any{
		"format_version": 1, "source": map[string]any{"format": "weka-dag", "sha256": hex.EncodeToString(hash.Sum(nil)), "digest_encoding": "uncompressed"},
		"selection": map[string]any{"model": model}, "included": c.totals,
	})
	digest := sha256.Sum256(c.manifest)
	c.manifestSHA256 = hex.EncodeToString(digest[:])
	return c, nil
}

func (c *Corpus) addDAGFamily(tr dagTrace, nextToken *int64) error {
	f := family{id: tr.ID, root: len(c.sessions)}
	first := len(c.requests)
	byID := make(map[int]int)
	parents := make(map[int][]int)
	starts := make(map[int]int64)
	ends := make(map[int]int64)
	blocks := make(map[int64]sim.TokenID)
	sessions := make(map[string]bool)
	var addSession func(string, []dagCall) error
	addSession = func(id string, calls []dagCall) error {
		if id == "" || sessions[id] {
			return fmt.Errorf("empty or duplicate session id %q", id)
		}
		sessions[id] = true
		si := len(c.sessions)
		c.sessions = append(c.sessions, session{id: id, family: len(c.families), spawn: -1, join: -1})
		f.sessions = append(f.sessions, si)
		for _, call := range calls {
			if call.Type == "subagent" {
				if err := addSession(call.AgentID, call.Requests); err != nil {
					return err
				}
				continue
			}
			if call.ID == nil || *call.ID < 0 || call.Parents == nil || call.Input == nil || call.Output == nil || call.Time == nil || call.APITime == nil {
				return fmt.Errorf("session %s requests require req_id, parent_ids, in, out, t, and api_time", id)
			}
			if _, ok := byID[*call.ID]; ok {
				return fmt.Errorf("duplicate req_id %d", *call.ID)
			}
			if (call.Type != "n" && call.Type != "s") || len(call.Requests) != 0 || call.Model != tr.Models[0] || *call.Input <= 0 || *call.Input > math.MaxInt32 || *call.Output < 0 || *call.Output >= math.MaxInt32 || len(call.Hashes) > *call.Input/64 {
				return fmt.Errorf("req_id %d has invalid type, model, token counts, or prefix coverage", *call.ID)
			}
			start, err := secondsUs(*call.Time)
			if err != nil {
				return err
			}
			if _, err := secondsUs(*call.APITime); err != nil {
				return fmt.Errorf("req_id %d has invalid api_time", *call.ID)
			}
			end, err := secondsUs(*call.Time + *call.APITime)
			if err != nil {
				return fmt.Errorf("req_id %d end: %w", *call.ID, err)
			}
			_, err = secondsUs(call.ThinkTime)
			if err != nil {
				return fmt.Errorf("req_id %d think_time: %w", *call.ID, err)
			}
			ri := len(c.requests)
			byID[*call.ID], parents[ri], starts[ri], ends[ri] = ri, call.Parents, start, end
			r := request{id: url.PathEscape(tr.ID) + fmt.Sprintf("/%d", *call.ID), session: si, step: len(c.sessions[si].requests), startMs: start / 1000, endMs: end / 1000, prompt: *call.Input, output: *call.Output, model: call.Model, stream: call.Type == "s"}
			for _, h := range call.Hashes {
				if h < 0 {
					return fmt.Errorf("req_id %d has negative prefix identity", *call.ID)
				}
				if _, ok := blocks[h]; !ok {
					blocks[h] = sim.TokenID(*nextToken)
					*nextToken++
				}
				r.blocks = append(r.blocks, blocks[h])
			}
			r.suffixToken, r.outputToken = sim.TokenID(*nextToken), sim.TokenID(*nextToken+1)
			*nextToken += 2
			if *nextToken > math.MaxInt32 {
				return fmt.Errorf("DAG input exhausts positive token identity space")
			}
			c.sessions[si].requests = append(c.sessions[si].requests, ri)
			c.requests = append(c.requests, r)
			c.explicitDeps = append(c.explicitDeps, dependency{})
			c.totals.PromptTokens += int64(r.prompt)
			c.totals.OutputTokens += int64(r.output)
			if r.output == 0 {
				c.totals.ZeroOutputRequests++
			}
		}
		if len(c.sessions[si].requests) == 0 {
			return fmt.Errorf("session %s has no requests", id)
		}
		return nil
	}
	if err := addSession(tr.ID, tr.Requests); err != nil {
		return err
	}
	indegree := make(map[int]int)
	var roots []int
	origin := int64(math.MaxInt64)
	for ri := first; ri < len(c.requests); ri++ {
		seen := make(map[int]bool)
		anchor := int64(0)
		for _, parent := range parents[ri] {
			pi, ok := byID[parent]
			if !ok || pi == ri || seen[parent] {
				return fmt.Errorf("request %s has missing, self, or duplicate parent %d", c.requests[ri].id, parent)
			}
			seen[parent] = true
			anchor = max(anchor, ends[pi])
			c.explicitDeps[ri].prerequisites = append(c.explicitDeps[ri].prerequisites, prerequisite{request: pi})
			c.explicitDeps[pi].dependents = append(c.explicitDeps[pi].dependents, ri)
		}
		indegree[ri] = len(parents[ri])
		if indegree[ri] == 0 {
			roots = append(roots, ri)
			origin = min(origin, starts[ri])
		} else {
			if starts[ri] < anchor {
				return fmt.Errorf("request %s starts before a parent completes", c.requests[ri].id)
			}
			// think_time can include a join wait already represented by parent_ids.
			c.explicitDeps[ri].delayUs = starts[ri] - anchor
		}
	}
	queue := append([]int(nil), roots...)
	for pos := 0; pos < len(queue); pos++ {
		for _, dependent := range c.explicitDeps[queue[pos]].dependents {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}
	if len(queue) != len(c.requests)-first {
		return fmt.Errorf("parent_ids contain a cycle")
	}
	for _, ri := range roots {
		c.rootOffsetsUs[ri] = starts[ri] - origin
	}
	f.originMs = origin / 1000
	c.arrivalUs = append(c.arrivalUs, origin)
	c.families = append(c.families, f)
	return nil
}
