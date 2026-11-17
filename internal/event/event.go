// Package event owns the durable business-event format: record shape, the
// catalog of kinds and details, cursor allocation and catch-up paging.
//
// It deliberately has no knowledge of sources, sheets, recipes or renders,
// and knows nothing about storage. Domain state embeds a Log; the journal
// persists the enclosing document without interpreting either events or the
// media domain. That keeps the event format maintainable in one place while
// still allowing a state mutation and its explanatory event to be committed
// by the same durable update.
package event

import (
	"fmt"
	"strconv"
	"time"
)

// Event is the persisted shape returned by the event endpoint.
type Event struct {
	Sequence uint64    `json:"sequence"`
	Subject  string    `json:"subject"`
	Kind     string    `json:"kind"`
	Detail   string    `json:"detail"`
	At       time.Time `json:"at"`
}

// Kind is a stable event kind from the catalog below.
type Kind string

const (
	KindSourceAccepted    Kind = "source.accepted"
	KindSourceRetired     Kind = "source.retired"
	KindSheetRevised      Kind = "sheet.revised"
	KindRecipeRevised     Kind = "recipe.revised"
	KindRenderQueued      Kind = "render.queued"
	KindRenderStarted     Kind = "render.started"
	KindRenderSucceeded   Kind = "render.succeeded"
	KindRenderFailed      Kind = "render.failed"
	KindRenderCancelled   Kind = "render.cancelled"
	KindRenderRetried     Kind = "render.retried"
	KindRenderInterrupted Kind = "render.interrupted"
)

// Fact is an unsequenced business fact. Callers create it through the catalog
// constructors so kind names and detail wording stay owned by this package.
// A Fact only becomes an Event when it is appended to a Log in the same state
// document as the mutation it explains.
type Fact struct {
	Subject string
	Kind    Kind
	Detail  string
}

func New(kind Kind, subject, detail string) Fact {
	return Fact{Subject: subject, Kind: kind, Detail: detail}
}

func SourceAccepted(subject, name string) Fact {
	return New(KindSourceAccepted, subject, name)
}
func SourceRetired(subject string) Fact {
	return New(KindSourceRetired, subject, "content retained for conservative reclamation")
}
func SheetRevised(subject string) Fact {
	return New(KindSheetRevised, subject, "immutable timeline revision saved")
}
func RecipeRevised(subject, format string) Fact {
	return New(KindRecipeRevised, subject, format)
}
func RenderQueued(subject string) Fact {
	return New(KindRenderQueued, subject, "frozen source and recipe revisions")
}
func RenderStarted(subject string, attempt int) Fact {
	return New(KindRenderStarted, subject, strconv.Itoa(attempt))
}
func RenderSucceeded(subject, detail string) Fact {
	return New(KindRenderSucceeded, subject, detail)
}
func RenderFailed(subject, detail string) Fact {
	return New(KindRenderFailed, subject, detail)
}
func RenderCancelled(subject string) Fact {
	return New(KindRenderCancelled, subject, "artifact visibility revoked before acknowledgement")
}
func RenderRetried(subject string) Fact {
	return New(KindRenderRetried, subject, "original frozen inputs retained")
}
func RenderInterrupted(subject string) Fact {
	return New(KindRenderInterrupted, subject, "explicit retry required; frozen input retained")
}

// Log is the embedded event portion of the persisted state document. Its JSON
// fields keep the existing schema: top-level "sequence" and "events".
type Log struct {
	Sequence uint64  `json:"sequence"`
	Events   []Event `json:"events"`
}

func Empty() Log {
	return Log{Events: []Event{}}
}

// Append allocates the next durable cursor value and records the fact. It must
// be called inside the same journal update as the state mutation.
func (l *Log) Append(fact Fact) Event {
	l.Sequence++
	e := Event{
		Sequence: l.Sequence,
		Subject:  fact.Subject,
		Kind:     string(fact.Kind),
		Detail:   fact.Detail,
		At:       time.Now().UTC(),
	}
	l.Events = append(l.Events, e)
	return e
}

// After implements the client catch-up contract: strict successors of the
// cursor, in durable order, at most limit records. It never renumbers or
// synthesizes records.
func (l Log) After(cursor uint64, limit int) []Event {
	out := []Event{}
	if limit <= 0 {
		return out
	}
	for _, e := range l.Events {
		if e.Sequence <= cursor {
			continue
		}
		out = append(out, e)
		if len(out) == limit {
			break
		}
	}
	return out
}

// Validate checks the event-format and cursor invariants only. Domain
// invariants remain in the domain package.
func (l Log) Validate() error {
	var previous uint64
	for _, e := range l.Events {
		if e.Sequence <= previous || e.Sequence > l.Sequence {
			return fmt.Errorf("event sequence is invalid")
		}
		if e.Subject == "" || e.Kind == "" || e.At.IsZero() {
			return fmt.Errorf("event shape is invalid")
		}
		previous = e.Sequence
	}
	return nil
}
