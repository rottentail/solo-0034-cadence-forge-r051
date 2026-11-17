package service

import (
	"cadenceforge/internal/caption"
	"cadenceforge/internal/domain"
	"cadenceforge/internal/event"
	"cadenceforge/internal/timeline"
	"time"
)

func (s *Service) SaveSheet(id string, expected int, input domain.Sheet) (domain.Sheet, error) {
	s.mutation.Lock()
	defer s.mutation.Unlock()
	if id == "" {
		id = ID()
	}
	var out domain.Sheet
	err := s.Store.Update(func(st *domain.State) error {
		versions := st.Sheets[id]
		if len(versions) != expected {
			return domain.Conflict("sheet revision changed")
		}
		if expected == 0 && len(versions) > 0 {
			return domain.Conflict("sheet already exists")
		}
		if expected > 0 && len(versions) == 0 {
			return domain.Missing("sheet", id)
		}
		if err := domain.ValidateSheet(input, st.Sources); err != nil {
			return err
		}
		input.ID = id
		input.Revision = len(versions) + 1
		input.CreatedAt = time.Now().UTC()
		out = input
		st.Sheets[id] = append(versions, input)
		st.Append(event.SheetRevised(id))
		return nil
	})
	return out, err
}
func (s *Service) Preview(sheetID, recipeID string) (domain.Plan, error) {
	st, err := s.Store.Snapshot()
	if err != nil {
		return domain.Plan{}, err
	}
	sheets := st.Sheets[sheetID]
	recipes := st.Recipes[recipeID]
	if len(sheets) == 0 {
		return domain.Plan{}, domain.Missing("sheet", sheetID)
	}
	if len(recipes) == 0 {
		return domain.Plan{}, domain.Missing("recipe", recipeID)
	}
	sheet := sheets[len(sheets)-1]
	recipe := recipes[len(recipes)-1]
	plan, err := timeline.Build(sheet, recipe, st.Sources)
	if err != nil {
		return plan, err
	}
	plan.VTT = caption.VTT(caption.Map(sheet, plan))
	return plan, nil
}
