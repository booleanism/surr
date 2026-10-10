package middlewares

import "net/http"

// MaxBodyBytes caps how much of a request body a handler will read.
//
// Every JSON handler decodes with a bare json.NewDecoder(r.Body), so without
// this an unauthenticated caller can stream an unbounded body into memory.
// Exceeding the limit makes the body read fail; the handler then answers 400,
// or 413 when it has not written a status yet.
func MaxBodyBytes(max int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, max)
			}

			next.ServeHTTP(w, r)
		})
	}
}
