package api

import "net/http"

func NewRouter(deps Dependencies) (http.Handler, error) {
	handler, err := NewHandler(deps)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/tracking/authority", handler.authoritySnapshot)
	mux.HandleFunc("PUT /api/tracking/client-state", handler.replaceClientState)
	mux.HandleFunc("GET /api/tracking/observations", handler.observations)
	return mux, nil
}
