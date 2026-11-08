// Package semiclosed replays finite agent workloads with independent root
// arrivals and completion-driven continuations.
package semiclosed

import (
	"encoding/json"

	"github.com/inference-sim/inference-sim/sim"
)

type JoinPolicy string

const (
	TimingInferred JoinPolicy = "timing-inferred"
	NoJoins        JoinPolicy = "none"
	ExplicitDAG    JoinPolicy = "explicit"
)

type Totals struct {
	Families           int   `json:"families"`
	Sessions           int   `json:"sessions"`
	Requests           int   `json:"requests"`
	PromptTokens       int64 `json:"prompt_tokens"`
	OutputTokens       int64 `json:"output_tokens"`
	ZeroOutputRequests int   `json:"zero_output_requests"`
}

// Corpus keeps source metadata and token identities private so replay instances
// can share it without sharing lifecycle state or materialized prompts.
type Corpus struct {
	families       []family
	sessions       []session
	requests       []request
	totals         Totals
	originMs       int64
	manifest       json.RawMessage
	manifestSHA256 string
	explicitDeps   []dependency
	arrivalUs      []int64
	rootOffsetsUs  map[int]int64
}

type family struct {
	id       string
	originMs int64
	root     int
	sessions []int
}

type session struct {
	id       string
	family   int
	requests []int
	spawn    int
	join     int
}

type request struct {
	id          string
	session     int
	step        int
	startMs     int64
	endMs       int64
	prompt      int
	output      int
	model       string
	stream      bool
	blocks      []sim.TokenID
	suffixToken sim.TokenID
	outputToken sim.TokenID
}

type manifest struct {
	FormatVersion int              `json:"format_version"`
	Included      map[string]int64 `json:"included"`
	Counts        map[string]int64 `json:"counts"`
	Artifacts     map[string]struct {
		SHA256 string `json:"sha256"`
		Bytes  int64  `json:"bytes"`
	} `json:"artifacts"`
	Selection struct {
		Model   string   `json:"model"`
		Weeks   []string `json:"weeks"`
		StartMs *int64   `json:"start_ms"`
		EndMs   *int64   `json:"end_ms"`
	} `json:"selection"`
}

type trace struct {
	ID          string   `json:"id"`
	Version     int      `json:"version"`
	BlockSize   int      `json:"block_size"`
	HashIDScope string   `json:"hash_id_scope"`
	Models      []string `json:"models"`
	Requests    []call   `json:"requests"`
}

type call struct {
	Type       string  `json:"type"`
	Time       float64 `json:"t"`
	APITime    float64 `json:"api_time"`
	Input      int     `json:"in"`
	Output     int     `json:"out"`
	Model      string  `json:"model"`
	Hashes     []int64 `json:"hash_ids"`
	AgentID    string  `json:"agent_id"`
	DurationMs int64   `json:"duration_ms"`
	Requests   []call  `json:"requests"`
}

type audit struct {
	TraceID  string           `json:"trace_id"`
	Status   string           `json:"status"`
	Week     string           `json:"week"`
	RootID   string           `json:"root_session_id"`
	OriginMs int64            `json:"origin_ms"`
	Stats    map[string]int64 `json:"stats"`
	Requests json.RawMessage  `json:"requests"`
	Children []childAudit     `json:"children"`
	Groups   []groupAudit     `json:"inferred_groups"`
}

type requestAudit struct {
	SessionID    string `json:"session_id"`
	Step         int    `json:"step"`
	StartMs      int64  `json:"start_ms"`
	EndMs        int64  `json:"end_ms"`
	Prompt       int    `json:"prompt_tokens"`
	Output       int    `json:"completion_tokens"`
	GapMs        *int64 `json:"gap_from_previous_end_ms"`
	SourceBlocks int64  `json:"source_blocks"`
	MappedBlocks int64  `json:"mapped_blocks"`
	SourceShared int64  `json:"source_shared_with_previous_tokens"`
	MappedShared int64  `json:"mapped_shared_with_previous_tokens"`
}

type childAudit struct {
	SessionID    string `json:"child_session_id"`
	SpawnStep    int    `json:"spawned_by_step"`
	CallIndex    int    `json:"spawned_by_call_index"`
	FirstMs      int64  `json:"first_start_ms"`
	LastMs       int64  `json:"last_end_ms"`
	SpawnGapMs   int64  `json:"spawn_gap_from_parent_end_ms"`
	JoinStep     *int   `json:"inferred_join_step"`
	JoinResidual *int64 `json:"inferred_join_residual_ms"`
}

type groupAudit struct {
	SpawnStep  int      `json:"spawn_step"`
	JoinStep   *int     `json:"inferred_join_step"`
	Children   []string `json:"children"`
	LastMs     int64    `json:"last_child_end_ms"`
	ResidualMs *int64   `json:"residual_ms"`
}
