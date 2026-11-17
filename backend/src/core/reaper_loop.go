package core

import (
	"context"
	"log/slog"
	"time"
)

// RunReaper reaps expired leases until ctx is cancelled: once at start, then
// every interval. When a call handles a full batch there may be more waiting,
// so it goes again straight away instead of sleeping.
func RunReaper(ctx context.Context, db Querier, interval time.Duration, batch int, log *slog.Logger) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if batch < 1 {
		batch = 100
	}
	if log == nil {
		log = slog.Default()
	}

	for ctx.Err() == nil {
		for ctx.Err() == nil {
			reapCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			res, err := Reap(reapCtx, db, batch)
			cancel()

			if err != nil {
				// A cancelled context is a normal shutdown, not an error.
				if ctx.Err() == nil {
					log.Error("reap failed", "error", err)
				}
				break
			}
			if n := res.Requeued + res.Dead; n > 0 {
				log.Info("reaped expired leases", "requeued", res.Requeued, "dead", res.Dead)
				if n >= batch {
					continue // a full batch: there may be more
				}
			}
			break
		}
		sleep(ctx, interval)
	}
}
