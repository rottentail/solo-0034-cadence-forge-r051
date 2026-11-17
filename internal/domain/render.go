package domain

import (
	"cadenceforge/internal/event"
	"encoding/json"
	"time"
)

type Attempt struct {
	Number     int        `json:"number"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Outcome    string     `json:"outcome"`
	Detail     string     `json:"detail,omitempty"`
}
type Inspection struct {
	Media           Media  `json:"media"`
	Digest          string `json:"digest"`
	Bytes           int64  `json:"bytes"`
	DurationDeltaMS int64  `json:"duration_delta_ms"`
}
type Artifact struct {
	Filename    string     `json:"filename"`
	CaptionFile string     `json:"caption_file"`
	Inspection  Inspection `json:"inspection"`
}
type Render struct {
	ID          string            `json:"id"`
	Key         string            `json:"key"`
	RequestHash string            `json:"request_hash"`
	Sheet       Sheet             `json:"sheet"`
	Recipe      Recipe            `json:"recipe"`
	Sources     map[string]Source `json:"sources"`
	Plan        Plan              `json:"plan"`
	State       string            `json:"state"`
	Attempts    []Attempt         `json:"attempts"`
	Artifact    *Artifact         `json:"artifact,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// State is the persisted domain document. The event log is embedded so that a
// state mutation and the fact explaining it commit in the same durable update,
// while the event format itself is maintained by the event package. The JSON
// shape remains the existing one: top-level "sequence" and "events".
type State struct {
	event.Log
	Schema  int                 `json:"schema"`
	Sources map[string]Source   `json:"sources"`
	Sheets  map[string][]Sheet  `json:"sheets"`
	Recipes map[string][]Recipe `json:"recipes"`
	Renders map[string]Render   `json:"renders"`
}

func Empty() State {
	return State{Log: event.Empty(), Schema: 1, Sources: map[string]Source{}, Sheets: map[string][]Sheet{}, Recipes: map[string][]Recipe{}, Renders: map[string]Render{}}
}
func Terminal(state string) bool {
	return state == "succeeded" || state == "failed" || state == "cancelled"
}

// StateCodec is the document codec used by the generic journal. It lives in the
// domain package so the journal needs no knowledge of editing or media.
type StateCodec struct{}

func (StateCodec) Empty() State { return Empty() }
func (StateCodec) Decode(b []byte) (State, error) {
	var st State
	err := json.Unmarshal(b, &st)
	return st, err
}
func (StateCodec) Encode(st State) ([]byte, error) {
	return json.MarshalIndent(st, "", "  ")
}
func (StateCodec) Validate(st State) error { return ValidateState(st) }
