package core

import (
	"context"
	"fmt"
)

// CheckOptions says what a finished run is allowed to look like.
type CheckOptions struct {
	TenantID        string // only check jobs of this tenant; empty means all jobs
	ExpectedJobs    int    // fail if the job count differs; 0 means don't check
	AllowReruns     bool   // an effect may have run twice, if an abnormal attempt explains it
	AllowDead       bool   // dead jobs are not a problem
	ExpectedDead    int    // if above 0, require exactly this many dead jobs (and allow them)
	DeadByCrashOnly bool   // every dead job must have used all its attempts, each ended by an expired lease
}

// RunReport holds the counts CheckRun found and the problems they add up to.
type RunReport struct {
	Total      int // jobs in scope
	Succeeded  int
	Dead       int
	Running    int // jobs stuck in running
	Unfinished int // jobs that are not succeeded, dead or cancelled (includes Running)

	Effects        int // rows in the effects table
	RerunJobs      int // effects that ran more than once
	ExtraRuns      int // total repeats: the sum of (runs - 1)
	MissingEffects int // succeeded jobs with no effect row
	OrphanEffects  int // effect rows with no succeeded job

	OpenAttempts         int // attempt rows that were never closed
	LeaseExpiredAttempts int // attempts that ended because a lease expired
	MultiSucceeded       int // jobs with more than one succeeded attempt
	NoSucceededAttempt   int // succeeded jobs with no succeeded attempt row
	DeadNotFromCrashes   int // dead jobs whose history is not max_attempts expired leases

	Problems []string
}

// CheckRun judges a finished load run against the guarantees. Run it after
// every worker has stopped: it reads one consistent snapshot of the jobs,
// effects and job_attempts tables and lists every way the run broke a
// guarantee. An empty Problems list means none was broken.
//
// It expects the load test's effects table to exist.
func CheckRun(ctx context.Context, q Querier, o CheckOptions) (RunReport, error) {
	var r RunReport
	err := q.QueryRow(ctx, `
		WITH scope AS (
			SELECT id, state, max_attempts FROM jobs
			WHERE ($1::text = '' OR tenant_id = $1)
		)
		SELECT
		  (SELECT count(*) FROM scope),
		  (SELECT count(*) FROM scope WHERE state = 'succeeded'),
		  (SELECT count(*) FROM scope WHERE state = 'dead'),
		  (SELECT count(*) FROM scope WHERE state = 'running'),
		  (SELECT count(*) FROM scope WHERE state NOT IN ('succeeded', 'dead', 'cancelled')),
		  (SELECT count(*) FROM effects),
		  (SELECT count(*) FROM effects WHERE runs > 1),
		  (SELECT COALESCE(sum(runs - 1), 0)::bigint FROM effects),
		  (SELECT count(*) FROM scope s
		     WHERE s.state = 'succeeded'
		       AND NOT EXISTS (SELECT 1 FROM effects e WHERE e.job_id = s.id)),
		  (SELECT count(*) FROM effects e
		     WHERE NOT EXISTS (SELECT 1 FROM scope s
		                       WHERE s.id = e.job_id AND s.state = 'succeeded')),
		  (SELECT count(*) FROM job_attempts a
		     JOIN scope s ON s.id = a.job_id WHERE a.outcome IS NULL),
		  (SELECT count(*) FROM job_attempts a
		     JOIN scope s ON s.id = a.job_id WHERE a.outcome = 'lease_expired'),
		  (SELECT count(*) FROM (
		     SELECT a.job_id FROM job_attempts a
		     JOIN scope s ON s.id = a.job_id
		     WHERE a.outcome = 'succeeded'
		     GROUP BY a.job_id HAVING count(*) > 1) multi),
		  (SELECT count(*) FROM scope s
		     WHERE s.state = 'succeeded'
		       AND NOT EXISTS (SELECT 1 FROM job_attempts a
		                       WHERE a.job_id = s.id AND a.outcome = 'succeeded')),
		  (SELECT count(*) FROM scope s
		     WHERE s.state = 'dead'
		       AND NOT (
		         (SELECT count(*) FROM job_attempts a WHERE a.job_id = s.id) = s.max_attempts
		         AND NOT EXISTS (SELECT 1 FROM job_attempts a
		                         WHERE a.job_id = s.id
		                           AND a.outcome IS DISTINCT FROM 'lease_expired')))`,
		o.TenantID).Scan(
		&r.Total, &r.Succeeded, &r.Dead, &r.Running, &r.Unfinished,
		&r.Effects, &r.RerunJobs, &r.ExtraRuns, &r.MissingEffects, &r.OrphanEffects,
		&r.OpenAttempts, &r.LeaseExpiredAttempts, &r.MultiSucceeded, &r.NoSucceededAttempt,
		&r.DeadNotFromCrashes)
	if err != nil {
		return RunReport{}, fmt.Errorf("check run: %w (has the effects table been created?)", err)
	}

	add := func(format string, args ...any) {
		r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
	}

	if r.Total == 0 {
		add("no jobs found")
	}
	if o.ExpectedJobs > 0 && r.Total != o.ExpectedJobs {
		add("expected %d jobs, found %d", o.ExpectedJobs, r.Total)
	}
	if r.Unfinished > 0 {
		add("%d jobs unfinished (%d queued or waiting, %d stuck running)",
			r.Unfinished, r.Unfinished-r.Running, r.Running)
	}
	if o.ExpectedDead > 0 {
		if r.Dead != o.ExpectedDead {
			add("expected %d dead jobs, found %d", o.ExpectedDead, r.Dead)
		}
	} else if r.Dead > 0 && !o.AllowDead {
		add("%d jobs are dead; the handler never fails, so each was killed or lost its lease on every attempt", r.Dead)
	}
	if o.DeadByCrashOnly && r.DeadNotFromCrashes > 0 {
		add("%d dead jobs did not die from expired leases (each must have used all its attempts, every one ended by an expired lease)",
			r.DeadNotFromCrashes)
	}
	if r.MissingEffects > 0 {
		add("%d succeeded jobs without an effect", r.MissingEffects)
	}
	if r.OrphanEffects > 0 {
		add("%d effects without a succeeded job", r.OrphanEffects)
	}
	if !o.AllowReruns {
		if r.RerunJobs > 0 {
			add("%d jobs had their effect run more than once (repeats are not allowed in this run)", r.RerunJobs)
		}
	} else if r.ExtraRuns > r.LeaseExpiredAttempts {
		add("%d repeated effects but only %d attempts ended with an expired lease; some repeat has no explanation",
			r.ExtraRuns, r.LeaseExpiredAttempts)
	}
	if r.OpenAttempts > 0 {
		add("%d attempt rows never closed", r.OpenAttempts)
	}
	if r.MultiSucceeded > 0 {
		add("%d jobs have more than one succeeded attempt (two workers were told they succeeded)", r.MultiSucceeded)
	}
	if r.NoSucceededAttempt > 0 {
		add("%d succeeded jobs have no succeeded attempt row", r.NoSucceededAttempt)
	}
	return r, nil
}
