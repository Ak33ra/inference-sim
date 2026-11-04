package semiclosed

import (
	"fmt"
	"sort"
)

func (c *Corpus) validateChildren(f family, a audit, calls map[string]call, spawns map[string]int, order []string, stats map[string]int64) error {
	ordered := append([]childAudit(nil), a.Children...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].SpawnStep != ordered[j].SpawnStep {
			return ordered[i].SpawnStep < ordered[j].SpawnStep
		}
		if ordered[i].CallIndex != ordered[j].CallIndex {
			return ordered[i].CallIndex < ordered[j].CallIndex
		}
		return ordered[i].SessionID < ordered[j].SessionID
	})
	for i, child := range ordered {
		if order[i] != child.SessionID {
			return fmt.Errorf("child order disagrees with audited spawn call indices")
		}
	}
	root := c.sessions[f.root].requests
	sessions := make(map[string]int)
	for _, si := range f.sessions[1:] {
		sessions[c.sessions[si].id] = si
	}
	type groupKey struct{ spawn, join int }
	groups := make(map[groupKey][]int)
	seen := make(map[string]bool)
	for _, child := range a.Children {
		si, ok := sessions[child.SessionID]
		if !ok || seen[child.SessionID] || child.SpawnStep < 0 || child.SpawnStep >= len(root) || child.CallIndex < 0 || spawns[child.SessionID] != child.SpawnStep {
			return fmt.Errorf("invalid child topology for %q", child.SessionID)
		}
		seen[child.SessionID] = true
		s := &c.sessions[si]
		first, last := c.requests[s.requests[0]], c.requests[s.requests[len(s.requests)-1]]
		parent := c.requests[root[child.SpawnStep]]
		call := calls[child.SessionID]
		if child.FirstMs != first.startMs || child.LastMs != last.endMs || child.SpawnGapMs != first.startMs-parent.endMs || first.startMs < parent.startMs || call.DurationMs != last.endMs-first.startMs || !secondsMatch(call.Time, first.startMs-a.OriginMs) {
			return fmt.Errorf("child %s timing disagrees with requests", child.SessionID)
		}
		join := -1
		for step := child.SpawnStep + 1; step < len(root); step++ {
			if c.requests[root[step]].startMs >= last.endMs {
				join = step
				break
			}
		}
		if join < 0 {
			if child.JoinStep != nil || child.JoinResidual != nil {
				return fmt.Errorf("background child has a join")
			}
			stats["background_child_sessions"]++
		} else if child.JoinStep == nil || *child.JoinStep != join || child.JoinResidual == nil || *child.JoinResidual != c.requests[root[join]].startMs-last.endMs {
			return fmt.Errorf("child %s inferred join disagrees with recorded times", child.SessionID)
		}
		s.spawn = root[child.SpawnStep]
		if join >= 0 {
			s.join = root[join]
		}
		if child.SpawnGapMs < 0 {
			stats["children_starting_before_parent_end"]++
		} else {
			stats["positive_spawn_gap_ms"] += child.SpawnGapMs
		}
		key := groupKey{child.SpawnStep, join}
		groups[key] = append(groups[key], si)
	}
	stats["spawn_groups"] = int64(len(groups))
	if len(a.Groups) != len(groups) {
		return fmt.Errorf("inferred group count disagrees")
	}
	for _, g := range a.Groups {
		join := -1
		if g.JoinStep != nil {
			join = *g.JoinStep
		}
		key := groupKey{g.SpawnStep, join}
		members, ok := groups[key]
		if !ok || len(members) != len(g.Children) {
			return fmt.Errorf("invalid inferred group")
		}
		children := make(map[string]bool)
		last := int64(0)
		for _, si := range members {
			s := c.sessions[si]
			children[s.id] = true
			last = max(last, c.requests[s.requests[len(s.requests)-1]].endMs)
		}
		for _, sid := range g.Children {
			if !children[sid] {
				return fmt.Errorf("inferred group repeats or invents child %s", sid)
			}
			delete(children, sid)
		}
		if g.LastMs != last {
			return fmt.Errorf("inferred group end disagrees")
		}
		if join < 0 {
			if g.ResidualMs != nil {
				return fmt.Errorf("background group has a residual")
			}
		} else {
			residual := c.requests[root[join]].startMs - last
			if g.ResidualMs == nil || *g.ResidualMs != residual {
				return fmt.Errorf("inferred group residual disagrees")
			}
			stats["inferred_group_join_residual_ms"] += residual
		}
		if len(members) >= 2 {
			stats["fanout_groups"]++
		}
		delete(groups, key)
	}
	return nil
}

type prerequisite struct {
	request int
	onSend  bool
}

type dependency struct {
	prerequisites []prerequisite
	dependents    []int
	delayUs       int64
}

func (c *Corpus) dependencies(policy JoinPolicy) []dependency {
	deps := make([]dependency, len(c.requests))
	for _, s := range c.sessions {
		if s.spawn < 0 {
			continue
		}
		ri := s.requests[0]
		r, parent := c.requests[ri], c.requests[s.spawn]
		overlap := r.startMs < parent.endMs
		anchor := parent.endMs
		if overlap {
			anchor = parent.startMs
		}
		deps[ri].prerequisites = append(deps[ri].prerequisites, prerequisite{request: s.spawn, onSend: overlap})
		deps[ri].delayUs = (r.startMs - anchor) * 1000
		deps[s.spawn].dependents = append(deps[s.spawn].dependents, ri)
	}
	for _, s := range c.sessions {
		for step := 1; step < len(s.requests); step++ {
			ri, prev := s.requests[step], s.requests[step-1]
			deps[ri].prerequisites = append(deps[ri].prerequisites, prerequisite{request: prev})
			deps[ri].delayUs = (c.requests[ri].startMs - c.requests[prev].endMs) * 1000
			deps[prev].dependents = append(deps[prev].dependents, ri)
		}
		if policy == TimingInferred && s.join >= 0 {
			last := s.requests[len(s.requests)-1]
			deps[s.join].prerequisites = append(deps[s.join].prerequisites, prerequisite{request: last})
			deps[last].dependents = append(deps[last].dependents, s.join)
		}
	}
	for ri := range deps {
		d := &deps[ri]
		if len(d.prerequisites) > 1 {
			lastPrerequisiteEnd := int64(0)
			for _, p := range d.prerequisites {
				lastPrerequisiteEnd = max(lastPrerequisiteEnd, c.requests[p.request].endMs)
			}
			d.delayUs = (c.requests[ri].startMs - lastPrerequisiteEnd) * 1000
		}
	}
	return deps
}
