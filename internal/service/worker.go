package service

import (
	"cadenceforge/internal/domain"
	"cadenceforge/internal/encoding"
	"cadenceforge/internal/event"
	"cadenceforge/internal/inspect"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

func (s *Service) Start(ctx context.Context) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	for i := 0; i < s.Config.Workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.loop(ctx) }()
	}
	s.Notify()
	return wg
}
func (s *Service) loop(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		runCtx, cancel := context.WithCancel(ctx)
		r, ok, err := s.claim(cancel)
		if err != nil {
			slog.Error("claim render", "error", err)
		}
		if ok {
			s.execute(runCtx, r)
			cancel()
			s.activeMu.Lock()
			delete(s.active, r.ID)
			s.activeMu.Unlock()
			continue
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		case <-ticker.C:
		}
	}
}
func (s *Service) claim(cancel context.CancelFunc) (domain.Render, bool, error) {
	s.mutation.Lock()
	defer s.mutation.Unlock()
	var out domain.Render
	found := false
	err := s.Store.Update(func(st *domain.State) error {
		ids := []string{}
		for id, r := range st.Renders {
			if r.State == "queued" {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool {
			a, b := st.Renders[ids[i]], st.Renders[ids[j]]
			if a.CreatedAt.Equal(b.CreatedAt) {
				return ids[i] < ids[j]
			}
			return a.CreatedAt.Before(b.CreatedAt)
		})
		if len(ids) == 0 {
			return nil
		}
		r := st.Renders[ids[0]]
		r.State = "running"
		r.UpdatedAt = time.Now().UTC()
		r.Attempts = append(r.Attempts, domain.Attempt{Number: len(r.Attempts) + 1, StartedAt: r.UpdatedAt, Outcome: "running"})
		st.Renders[r.ID] = r
		st.Append(event.RenderStarted(r.ID, len(r.Attempts)))
		out = r
		found = true
		return nil
	})
	if err == nil && found {
		s.activeMu.Lock()
		s.active[out.ID] = cancel
		s.activeMu.Unlock()
	}
	return out, found && err == nil, err
}
func (s *Service) execute(ctx context.Context, r domain.Render) {
	attempt := len(r.Attempts)
	stage, err := s.Blobs.Staging(r.ID, attempt)
	if err != nil {
		s.complete(r, nil, err)
		return
	}
	defer os.RemoveAll(stage)
	for _, source := range r.Sources {
		path, e := s.Blobs.Object(source.Digest)
		if e != nil {
			s.complete(r, nil, e)
			return
		}
		digest, n, e := inspect.Digest(path)
		if e != nil {
			s.complete(r, nil, e)
			return
		}
		if digest != source.Digest || n != source.Bytes {
			s.complete(r, nil, fmt.Errorf("source content integrity failed"))
			return
		}
	}
	runner := encoding.Runner{Binary: s.Config.FFmpeg, Blobs: s.Blobs}
	name, err := runner.Encode(ctx, r, stage)
	if err != nil {
		s.complete(r, nil, err)
		return
	}
	inspection, err := inspect.Artifact(ctx, s.Probe, filepath.Join(stage, name), r)
	if err != nil {
		s.complete(r, nil, err)
		return
	}
	artifact := &domain.Artifact{Filename: name, CaptionFile: "captions.vtt", Inspection: inspection}
	s.mutation.Lock()
	defer s.mutation.Unlock()
	st, err := s.Store.Snapshot()
	if err != nil {
		slog.Error("read before promotion", "error", err)
		return
	}
	current, ok := st.Renders[r.ID]
	if !ok || current.State != "running" || len(current.Attempts) != attempt {
		return
	}
	if err = s.Blobs.Promote(stage, r.ID, attempt); err != nil {
		s.completeLocked(r, nil, err)
		return
	}
	s.completeLocked(r, artifact, nil)
}
func (s *Service) complete(r domain.Render, a *domain.Artifact, cause error) {
	s.mutation.Lock()
	defer s.mutation.Unlock()
	s.completeLocked(r, a, cause)
}
func (s *Service) completeLocked(r domain.Render, a *domain.Artifact, cause error) {
	err := s.Store.Update(func(st *domain.State) error {
		current, ok := st.Renders[r.ID]
		if !ok || current.State != "running" || len(current.Attempts) != len(r.Attempts) {
			return nil
		}
		current.UpdatedAt = time.Now().UTC()
		last := &current.Attempts[len(current.Attempts)-1]
		last.FinishedAt = &current.UpdatedAt
		if cause != nil {
			current.State = "failed"
			last.Outcome = "failed"
			last.Detail = cause.Error()
			if len(last.Detail) > 8192 {
				last.Detail = last.Detail[:8192]
			}
		} else {
			current.State = "succeeded"
			last.Outcome = "succeeded"
			current.Artifact = a
		}
		st.Renders[r.ID] = current
		if cause != nil {
			st.Append(event.RenderFailed(r.ID, last.Detail))
		} else {
			st.Append(event.RenderSucceeded(r.ID, last.Detail))
		}
		return nil
	})
	if err != nil {
		slog.Error("commit render result", "render", r.ID, "error", err)
	}
}
