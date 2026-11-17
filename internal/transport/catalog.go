package transport

import (
	"cadenceforge/internal/domain"
	"net/http"
	"strconv"
)

const eventPageLimit = 200

func (a API) health(w http.ResponseWriter, r *http.Request) {
	write(w, 200, map[string]string{"status": "ready", "service": "cadence-forge", "schema": "1"})
}
func (a API) state(w http.ResponseWriter, r *http.Request) {
	st, err := a.Service.Store.Snapshot()
	if err != nil {
		failure(w, err)
		return
	}
	write(w, 200, st)
}
func (a API) ingest(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, a.Service.Config.UploadLimit)
	source, err := a.Service.Ingest(r.Context(), r.URL.Query().Get("name"), r.Body)
	if err != nil {
		failure(w, err)
		return
	}
	write(w, 201, source)
}
func (a API) retire(w http.ResponseWriter, r *http.Request) {
	if err := a.Service.RetireSource(r.PathValue("id")); err != nil {
		failure(w, err)
		return
	}
	w.WriteHeader(204)
}
func (a API) loudness(w http.ResponseWriter, r *http.Request) {
	v, err := a.Service.Loudness(r.Context(), r.PathValue("id"))
	if err != nil {
		failure(w, err)
		return
	}
	write(w, 200, v)
}
func expected(r *http.Request) (int, error) {
	s := r.Header.Get("If-Match")
	if s == "" && r.Method == "POST" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return 0, domain.Invalid("If-Match must be a positive revision")
	}
	return n, nil
}
func (a API) sheet(w http.ResponseWriter, r *http.Request) {
	var input domain.Sheet
	if err := decode(w, r, &input); err != nil {
		failure(w, err)
		return
	}
	rev, err := expected(r)
	if err != nil {
		failure(w, err)
		return
	}
	v, err := a.Service.SaveSheet(r.PathValue("id"), rev, input)
	if err != nil {
		failure(w, err)
		return
	}
	w.Header().Set("ETag", strconv.Itoa(v.Revision))
	write(w, 201, v)
}
func (a API) recipe(w http.ResponseWriter, r *http.Request) {
	var input domain.Recipe
	if err := decode(w, r, &input); err != nil {
		failure(w, err)
		return
	}
	rev, err := expected(r)
	if err != nil {
		failure(w, err)
		return
	}
	v, err := a.Service.SaveRecipe(r.PathValue("id"), rev, input)
	if err != nil {
		failure(w, err)
		return
	}
	w.Header().Set("ETag", strconv.Itoa(v.Revision))
	write(w, 201, v)
}
func (a API) preview(w http.ResponseWriter, r *http.Request) {
	v, err := a.Service.Preview(r.PathValue("id"), r.URL.Query().Get("recipe_id"))
	if err != nil {
		failure(w, err)
		return
	}
	write(w, 200, v)
}
func (a API) events(w http.ResponseWriter, r *http.Request) {
	after := uint64(0)
	if q := r.URL.Query().Get("after"); q != "" {
		var err error
		after, err = strconv.ParseUint(q, 10, 64)
		if err != nil {
			failure(w, domain.Invalid("invalid event cursor"))
			return
		}
	}
	events, err := a.Service.Events(after, eventPageLimit)
	if err != nil {
		failure(w, err)
		return
	}
	write(w, 200, events)
}

func (a API) visual(w http.ResponseWriter, r *http.Request) {
	image, err := a.Service.Visual(r.Context(), r.PathValue("id"), r.URL.Query().Get("mode"))
	if err != nil {
		failure(w, err)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.WriteHeader(200)
	if _, err = w.Write(image); err != nil {
		return
	}
}
