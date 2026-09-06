package adminweb

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var staticFiles embed.FS

func NewHandler() http.Handler {
	files, err := fs.Sub(staticFiles, "static")
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "admin assets unavailable", http.StatusInternalServerError)
		})
	}
	return http.FileServer(http.FS(files))
}
