package service

import (
	"cadenceforge/internal/domain"
	"cadenceforge/internal/event"
)

func (s *Service) SaveRecipe(id string, expected int, input domain.Recipe) (domain.Recipe, error) {
	if err := domain.ValidateRecipe(input); err != nil {
		return domain.Recipe{}, err
	}
	s.mutation.Lock()
	defer s.mutation.Unlock()
	if id == "" {
		id = ID()
	}
	var out domain.Recipe
	err := s.Store.Update(func(st *domain.State) error {
		versions := st.Recipes[id]
		if len(versions) != expected {
			return domain.Conflict("recipe revision changed")
		}
		if expected > 0 && len(versions) == 0 {
			return domain.Missing("recipe", id)
		}
		input.ID = id
		input.Revision = len(versions) + 1
		out = input
		st.Recipes[id] = append(versions, input)
		st.Append(event.RecipeRevised(id, input.Format))
		return nil
	})
	return out, err
}
