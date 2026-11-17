package core

import (
	"context"
	"testing"
)

func TestSubmit_InsertsQueuedJob(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id, err := Submit(ctx, pool, SubmitParams{
		TenantID: "t1",
		Type:     "send_email",
		Payload:  []byte(`{"to":"a@example.com"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	var state, queue string
	var attempts int
	err = pool.QueryRow(ctx,
		`SELECT state, queue, attempts FROM jobs WHERE id = $1`, id,
	).Scan(&state, &queue, &attempts)
	if err != nil {
		t.Fatalf("read job: %v", err)
	}
	if state != "queued" || queue != "default" || attempts != 0 {
		t.Fatalf("got state=%q queue=%q attempts=%d", state, queue, attempts)
	}
}

func TestSubmit_RejectsInvalidInput(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	cases := []struct {
		name string
		p    SubmitParams
	}{
		{"missing tenant", SubmitParams{Type: "x", Payload: []byte(`{}`)}},
		{"missing type", SubmitParams{TenantID: "t1", Payload: []byte(`{}`)}},
		{"invalid json", SubmitParams{TenantID: "t1", Type: "x", Payload: []byte(`{oops`)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Submit(ctx, pool, c.p); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("invalid submissions left %d rows behind", n)
	}
}

// The property that makes the transactional-enqueue design work:
// a job submitted inside a transaction vanishes if the transaction rolls back.
func TestSubmit_RolledBackWithTransaction(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Submit(ctx, tx, SubmitParams{
		TenantID: "t1", Type: "send_email", Payload: []byte(`{}`),
	}); err != nil {
		t.Fatalf("Submit in tx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("rolled-back transaction left %d jobs behind", n)
	}
}
