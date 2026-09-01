package v2scan

import (
	"context"
	"time"
)

type Recovery struct {
	Jobs *DemandRepository
}

func NewRecovery(jobs *DemandRepository) *Recovery {
	return &Recovery{Jobs: jobs}
}

func (r *Recovery) RecoverExpiredLeases(ctx context.Context) (int, error) {
	if r == nil || r.Jobs == nil {
		return 0, ErrJobNotFound
	}
	return r.Jobs.RecoverExpiredLeases(ctx)
}

func RetryDelay(attempt int, base, maximum time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if base <= 0 {
		base = time.Minute
	}
	if maximum <= 0 {
		maximum = 24 * time.Hour
	}
	delay := base
	for i := 1; i < attempt && delay < maximum; i++ {
		if delay > maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}
