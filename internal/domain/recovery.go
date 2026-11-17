package domain

import (
	"cadenceforge/internal/event"
	"time"
)

// RecoverInterrupted applies the restart rule to a loaded document: a running
// render cannot be reported as delivered after the process ended, so it becomes
// failed with an interrupted attempt while keeping its frozen input. The
// mutation and its explaining event belong to one state document and are
// committed together by the caller's single durable update.
func RecoverInterrupted(st *State) (changed bool) {
	for id, r := range st.Renders {
		if r.State != "running" {
			continue
		}
		r.State = "failed"
		r.UpdatedAt = time.Now().UTC()
		if len(r.Attempts) > 0 {
			a := &r.Attempts[len(r.Attempts)-1]
			a.FinishedAt = &r.UpdatedAt
			a.Outcome = "interrupted"
			a.Detail = "process ended before artifact commit"
		}
		st.Renders[id] = r
		st.Append(event.RenderInterrupted(id))
		changed = true
	}
	return changed
}
