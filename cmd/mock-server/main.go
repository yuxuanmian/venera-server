package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/comic/", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[len("/comic/"):]
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":         id,
			"title":      "Mock Comic " + id,
			"cover":      "http://127.0.0.1:8765/cover/" + id + ".jpg",
			"updateTime": "2026-07-20",
			"chapters": map[string]string{
				"1": "第1话",
				"2": "第2话",
			},
		})
	})
	fmt.Println("mock server listening on http://127.0.0.1:8765")
	if err := http.ListenAndServe("127.0.0.1:8765", nil); err != nil {
		panic(err)
	}
}
