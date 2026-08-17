package api

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"
)

type gzipResponseWriter struct {
	http.ResponseWriter
	writer *gzip.Writer
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.writer.Write(b)
}

func (w *gzipResponseWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
}

func (w *gzipResponseWriter) Flush() {
	_ = w.writer.Flush()
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

type gzipReadCloser struct {
	io.Reader
	closer io.Closer
}

func (g *gzipReadCloser) Close() error {
	return g.closer.Close()
}

// gzipMiddleware transparently decompresses gzip request bodies and compresses
// responses when the client advertises Accept-Encoding: gzip.
func gzipMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				writeError(w, http.StatusBadRequest, "bad_request", "invalid gzip body")
				return
			}
			r.Body = &gzipReadCloser{Reader: gz, closer: r.Body}
			r.Header.Del("Content-Encoding")
			r.Header.Del("Content-Length")
		}

		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Add("Vary", "Accept-Encoding")
			gz := gzip.NewWriter(w)
			defer gz.Close()
			w = &gzipResponseWriter{ResponseWriter: w, writer: gz}
		}

		next(w, r)
	}
}
