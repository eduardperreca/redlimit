package redlimit

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// Middleware limits requests by the key that keyFunc extracts from each request.
//
// Rejected requests get 429 Too Many Requests with a Retry-After header.
// If the limiter fails, the error is logged with slog and the request either
// passes or gets 503 Service Unavailable, depending on the limiter's fail mode.
func Middleware(l Limiter, keyFunc func(*http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			res, err := l.Allow(r.Context(), keyFunc(r))
			if err != nil {
				slog.ErrorContext(r.Context(), "redlimit: limiter failed", "err", err)
				if !res.Allowed {
					http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			h := w.Header()
			h.Set("RateLimit-Limit", strconv.Itoa(res.Limit))
			h.Set("RateLimit-Remaining", strconv.Itoa(res.Remaining))
			h.Set("RateLimit-Reset", seconds(res.ResetIn))
			if !res.Allowed {
				h.Set("Retry-After", seconds(res.RetryAfter))
				http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// seconds rounds d up to whole seconds, so clients never retry too early.
func seconds(d time.Duration) string {
	return strconv.Itoa(int((d + time.Second - 1) / time.Second))
}
