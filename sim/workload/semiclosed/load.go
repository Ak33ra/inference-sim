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
	"path/filepath"
	"sort"

	"github.com/inference-sim/inference-sim/sim"
)

// Load verifies a freeinference_trace conversion directory before compiling its
// finite families. Excluded audit rows remain provenance, never replay requests.
func Load(dir string) (*Corpus, error) {
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	if m.FormatVersion != 1 || m.Selection.Model == "" || len(m.Selection.Weeks) == 0 {
		return nil, fmt.Errorf("unsupported or incomplete conversion manifest")
	}
	files := make(map[string]*os.File)
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	for _, name := range []string{"corpus.jsonl", "audit.jsonl"} {
		artifact, ok := m.Artifacts[name]
		if !ok || artifact.Bytes <= 0 {
			return nil, fmt.Errorf("missing artifact metadata for %s", name)
		}
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		files[name] = f
		hash := sha256.New()
		n, err := io.Copy(hash, f)
		if err != nil {
			return nil, err
		}
		if n != artifact.Bytes || hex.EncodeToString(hash.Sum(nil)) != artifact.SHA256 {
			return nil, fmt.Errorf("%s size or sha256 disagrees with manifest", name)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
	}
	byID := make(map[string]audit)
	seen := make(map[string]bool)
	stats := make(map[string]int64)
	var excludedFamilies, excludedRequests int64
	dec := json.NewDecoder(files["audit.jsonl"])
	for {
		var a audit
		if err := dec.Decode(&a); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("audit: %w", err)
		}
		if a.TraceID == "" || seen[a.TraceID] {
			return nil, fmt.Errorf("empty or duplicate audit trace %q", a.TraceID)
		}
		seen[a.TraceID] = true
		switch a.Status {
		case "included":
			byID[a.TraceID] = a
			for k, v := range a.Stats {
				stats[k] += v
			}
		case "excluded":
			var n int64
			if err := json.Unmarshal(a.Requests, &n); err != nil || n <= 0 {
				return nil, fmt.Errorf("invalid excluded request count for %s", a.TraceID)
			}
			excludedFamilies++
			excludedRequests += n
		default:
			return nil, fmt.Errorf("invalid audit status %q", a.Status)
		}
	}
	if len(byID) == 0 {
		return nil, fmt.Errorf("conversion has no included families")
	}
	if err := equalCounts("manifest included", stats, m.Included); err != nil {
		return nil, err
	}
	counts := map[string]int64{"excluded_families": excludedFamilies, "excluded_requests": excludedRequests,
		"source_families": excludedFamilies + int64(len(byID)), "source_requests": excludedRequests + stats["requests"]}
	if err := equalCounts("manifest counts", counts, m.Counts); err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	c := &Corpus{manifest: append(json.RawMessage(nil), data...), manifestSHA256: hex.EncodeToString(hash[:]), originMs: math.MaxInt64}
	dec = json.NewDecoder(files["corpus.jsonl"])
	nextToken := int64(1)
	seenSessions := make(map[string]bool)
	for {
		var tr trace
		if err := dec.Decode(&tr); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("corpus: %w", err)
		}
		a, ok := byID[tr.ID]
		if !ok {
			return nil, fmt.Errorf("trace %q has no unique included audit", tr.ID)
		}
		if err := c.addFamily(tr, a, m, &nextToken, seenSessions); err != nil {
			return nil, fmt.Errorf("%s: %w", tr.ID, err)
		}
		delete(byID, tr.ID)
	}
	if len(byID) != 0 {
		return nil, fmt.Errorf("%d included audit families missing from corpus", len(byID))
	}
	c.totals.Families, c.totals.Sessions, c.totals.Requests = len(c.families), len(c.sessions), len(c.requests)
	for _, r := range c.requests {
		c.totals.PromptTokens += int64(r.prompt)
		c.totals.OutputTokens += int64(r.output)
		if r.output == 0 {
			c.totals.ZeroOutputRequests++
		}
		if r.endMs-c.originMs > math.MaxInt64/1000 {
			return nil, fmt.Errorf("recorded time range exceeds microsecond clock")
		}
	}
	return c, nil
}

func equalCounts(label string, got, want map[string]int64) error {
	keys := make(map[string]bool)
	for k := range got {
		keys[k] = true
	}
	for k := range want {
		keys[k] = true
	}
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)
	for _, k := range ordered {
		if got[k] != want[k] {
			return fmt.Errorf("%s %s: computed %d, recorded %d", label, k, got[k], want[k])
		}
	}
	return nil
}

func (c *Corpus) addFamily(tr trace, a audit, m manifest, nextToken *int64, seenSessions map[string]bool) error {
	if tr.Version != 7 || tr.BlockSize != 64 || tr.HashIDScope != "local" || len(tr.Models) != 1 || tr.Models[0] != m.Selection.Model {
		return fmt.Errorf("expected Weka v7, local 64-token blocks, and selected model")
	}
	weekOK := false
	for _, week := range m.Selection.Weeks {
		weekOK = weekOK || a.Week == week
	}
	if !weekOK || a.RootID == "" || tr.ID != a.Week+"/"+a.RootID || a.OriginMs < 0 || a.OriginMs > math.MaxInt64/1000 {
		return fmt.Errorf("invalid family identity or origin")
	}
	if m.Selection.StartMs != nil && a.OriginMs < *m.Selection.StartMs || m.Selection.EndMs != nil && a.OriginMs >= *m.Selection.EndMs {
		return fmt.Errorf("root origin outside selected window")
	}
	var rows []requestAudit
	if err := json.Unmarshal(a.Requests, &rows); err != nil {
		return err
	}
	bySession := make(map[string][]requestAudit)
	for _, r := range rows {
		bySession[r.SessionID] = append(bySession[r.SessionID], r)
	}
	rootCalls := []call{}
	childCalls := make(map[string]call)
	childSpawns := make(map[string]int)
	var childOrder []string
	for _, r := range tr.Requests {
		if r.Type == "subagent" {
			if r.AgentID == "" || r.AgentID == a.RootID || len(rootCalls) == 0 {
				return fmt.Errorf("invalid child placement")
			}
			if _, ok := childCalls[r.AgentID]; ok {
				return fmt.Errorf("duplicate child %s", r.AgentID)
			}
			childCalls[r.AgentID], childSpawns[r.AgentID] = r, len(rootCalls)-1
			childOrder = append(childOrder, r.AgentID)
		} else {
			rootCalls = append(rootCalls, r)
		}
	}
	if len(rootCalls) == 0 {
		return fmt.Errorf("family has no root requests")
	}
	if len(bySession) != len(childCalls)+1 || len(childCalls) != len(a.Children) {
		return fmt.Errorf("session counts disagree")
	}
	f := family{id: tr.ID, originMs: a.OriginMs, root: len(c.sessions)}
	stats := map[string]int64{"families": 1, "sessions": int64(len(bySession)), "child_sessions": int64(len(childCalls))}
	blockIDs := make(map[int64]sim.TokenID)
	sids := append([]string{a.RootID}, childOrder...)
	for _, sid := range sids {
		key := a.Week + "/" + sid
		if sid == "" || seenSessions[key] {
			return fmt.Errorf("empty or duplicate source session %q", sid)
		}
		seenSessions[key] = true
		sr := bySession[sid]
		sort.Slice(sr, func(i, j int) bool { return sr[i].Step < sr[j].Step })
		calls := rootCalls
		if sid != a.RootID {
			calls = childCalls[sid].Requests
		}
		if len(sr) == 0 || len(sr) != len(calls) {
			return fmt.Errorf("%s request count disagrees", sid)
		}
		s := session{id: sid, family: len(c.families), spawn: -1, join: -1}
		for step, r := range sr {
			call := calls[step]
			if err := validateRequest(r, call, a.OriginMs, step, m.Selection.Model); err != nil {
				return fmt.Errorf("%s step %d: %w", sid, step, err)
			}
			if step == 0 && r.GapMs != nil || step > 0 && (r.GapMs == nil || *r.GapMs != r.StartMs-sr[step-1].EndMs || *r.GapMs < 0) {
				return fmt.Errorf("%s step %d: invalid sequential gap", sid, step)
			}
			if step == 0 && (r.SourceShared != 0 || r.MappedShared != 0) {
				return fmt.Errorf("first request claims previous prefix sharing")
			}
			if step > 0 {
				previous := calls[step-1]
				shared := 0
				for shared < len(previous.Hashes) && shared < len(call.Hashes) && previous.Hashes[shared] == call.Hashes[shared] {
					shared++
				}
				if r.MappedShared != int64(shared*64) || r.SourceShared < r.MappedShared || r.SourceShared%16 != 0 || r.SourceShared > 16*min(r.SourceBlocks, sr[step-1].SourceBlocks) {
					return fmt.Errorf("invalid previous prefix coverage")
				}
				stats["sequential_gaps"]++
				stats["sequential_gap_ms"] += *r.GapMs
				if *r.GapMs > 10000 {
					stats["sequential_gaps_over_10s"]++
				}
				if r.Prompt < sr[step-1].Prompt {
					stats["prompt_contractions"]++
				}
				stats["source_shared_with_previous_tokens"] += r.SourceShared
				stats["mapped_shared_with_previous_tokens"] += r.MappedShared
			}
			req := request{id: tr.ID + "/" + url.PathEscape(sid) + fmt.Sprintf("/%d", step), session: len(c.sessions), step: step,
				startMs: r.StartMs, endMs: r.EndMs, prompt: r.Prompt, output: r.Output, model: call.Model, stream: call.Type == "s"}
			for _, h := range call.Hashes {
				id, ok := blockIDs[h]
				if !ok {
					id = sim.TokenID(*nextToken)
					*nextToken++
					blockIDs[h] = id
				}
				req.blocks = append(req.blocks, id)
			}
			req.suffixToken = sim.TokenID(*nextToken)
			*nextToken++
			req.outputToken = sim.TokenID(*nextToken)
			*nextToken++
			if *nextToken > math.MaxInt32 {
				return fmt.Errorf("corpus exhausts positive token identity space")
			}
			s.requests = append(s.requests, len(c.requests))
			c.requests = append(c.requests, req)
			stats["requests"]++
			stats["prompt_tokens"] += int64(r.Prompt)
			stats["completion_tokens"] += int64(r.Output)
			stats["source_blocks"] += r.SourceBlocks
			stats["mapped_blocks"] += r.MappedBlocks
			stats["unmapped_prompt_tokens"] += int64(r.Prompt) - 64*r.MappedBlocks
			stats["source_groups_clipped_to_prompt"] += r.SourceBlocks/4 - r.MappedBlocks
			stats["source_trailing_blocks"] += r.SourceBlocks % 4
			if r.Output == 0 {
				stats["zero_output_requests"]++
			}
			if r.MappedBlocks == 0 {
				stats["requests_without_mapped_blocks"]++
			}
		}
		f.sessions = append(f.sessions, len(c.sessions))
		c.sessions = append(c.sessions, s)
	}
	if c.requests[c.sessions[f.root].requests[0]].startMs != a.OriginMs {
		return fmt.Errorf("root start disagrees with family origin")
	}
	if err := c.validateChildren(f, a, childCalls, childSpawns, childOrder, stats); err != nil {
		return err
	}
	if err := equalCounts("family stats", stats, a.Stats); err != nil {
		return err
	}
	c.families = append(c.families, f)
	c.originMs = min(c.originMs, f.originMs)
	return nil
}

func validateRequest(r requestAudit, c call, origin int64, step int, model string) error {
	if r.Step != step || r.StartMs < origin || r.EndMs < r.StartMs || r.EndMs > math.MaxInt64/1000 || r.Prompt <= 0 || r.Prompt > math.MaxInt32 || r.Output < 0 || r.Output >= math.MaxInt32 {
		return fmt.Errorf("invalid step, time, or token count")
	}
	if c.Type != "s" && c.Type != "n" || len(c.Requests) != 0 || c.Model != model || c.Input != r.Prompt || c.Output != r.Output {
		return fmt.Errorf("corpus request disagrees with audit")
	}
	if !secondsMatch(c.Time, r.StartMs-origin) || !secondsMatch(c.APITime, r.EndMs-r.StartMs) {
		return fmt.Errorf("corpus time disagrees with audit")
	}
	if r.SourceBlocks < 0 || r.MappedBlocks != int64(len(c.Hashes)) || r.MappedBlocks != min(r.SourceBlocks/4, int64(r.Prompt/64)) {
		return fmt.Errorf("invalid mapped prefix coverage")
	}
	for _, h := range c.Hashes {
		if h < 0 {
			return fmt.Errorf("negative prefix identity")
		}
	}
	return nil
}

func secondsMatch(seconds float64, ms int64) bool {
	return !math.IsNaN(seconds) && !math.IsInf(seconds, 0) && math.Abs(seconds-float64(ms)/1000) < 1e-8
}
