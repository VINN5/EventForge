package core

import (
	"context"
	"testing"
	"time"
)

func ignoreCancel(error) {}

func TestInflight_ExpireUnconfirmed_ReportsEachStaleLeaseOnce(t *testing.T) {
	f := &inflight{m: map[LeaseRef]context.CancelCauseFunc{}}
	base := time.Now()
	old := LeaseRef{ID: "old", Fence: 1}
	fresh := LeaseRef{ID: "fresh", Fence: 1}
	f.addAt(old, ignoreCancel, base)
	f.addAt(fresh, ignoreCancel, base.Add(900*time.Millisecond))

	got := f.expireUnconfirmed(base.Add(time.Second), time.Second)
	if len(got) != 1 || got[0].Ref != old {
		t.Fatalf("at +1s got %+v, want only the old lease", got)
	}
	if got[0].Age != time.Second {
		t.Fatalf("age = %v, want 1s", got[0].Age)
	}

	got = f.expireUnconfirmed(base.Add(2*time.Second), time.Second)
	if len(got) != 1 || got[0].Ref != fresh {
		t.Fatalf("at +2s got %+v, want only the fresh lease (the old one was already reported)", got)
	}

	if got := f.expireUnconfirmed(base.Add(3*time.Second), time.Second); len(got) != 0 {
		t.Fatalf("at +3s got %+v, want nothing: each lease is reported once", got)
	}
}

func TestInflight_Confirm_OnlyMovesForwardAndIgnoresLostAndUnknownLeases(t *testing.T) {
	f := &inflight{m: map[LeaseRef]context.CancelCauseFunc{}}
	base := time.Now()
	a := LeaseRef{ID: "a", Fence: 1}
	b := LeaseRef{ID: "b", Fence: 1}
	c := LeaseRef{ID: "c", Fence: 1} // never added
	f.addAt(a, ignoreCancel, base)
	f.addAt(b, ignoreCancel, base)

	// a is confirmed at +500ms; b was reported lost, so it is not confirmed; c is unknown.
	f.confirm([]LeaseRef{a, b, c}, []LeaseRef{b}, base.Add(500*time.Millisecond))
	// A heartbeat that started earlier must not move a's confirmation backwards.
	f.confirm([]LeaseRef{a}, nil, base.Add(200*time.Millisecond))

	got := f.expireUnconfirmed(base.Add(1400*time.Millisecond), time.Second)
	if len(got) != 1 || got[0].Ref != b {
		t.Fatalf("at +1.4s got %+v, want only b (a was confirmed at +500ms)", got)
	}

	got = f.expireUnconfirmed(base.Add(3*time.Second), time.Second)
	if len(got) != 1 || got[0].Ref != a {
		t.Fatalf("at +3s got %+v, want only a (b was already reported, c was never added)", got)
	}
}

func TestNewWorker_ConfirmWindow(t *testing.T) {
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error { return nil },
	}

	t.Run("default is two thirds of the lease", func(t *testing.T) {
		w, err := NewWorker(nil, WorkerConfig{ID: "w1", Lease: 30 * time.Second}, handlers, quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		if w.cfg.ConfirmWindow != 20*time.Second {
			t.Fatalf("window = %v, want 20s", w.cfg.ConfirmWindow)
		}
	})

	t.Run("default stays between the heartbeat interval and the lease", func(t *testing.T) {
		w, err := NewWorker(nil,
			WorkerConfig{ID: "w1", Lease: time.Second, HeartbeatInterval: 900 * time.Millisecond},
			handlers, quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		if w.cfg.ConfirmWindow <= 900*time.Millisecond || w.cfg.ConfirmWindow >= time.Second {
			t.Fatalf("window = %v, want strictly between 900ms and 1s", w.cfg.ConfirmWindow)
		}
	})

	t.Run("an explicit window not longer than the heartbeat interval is rejected", func(t *testing.T) {
		_, err := NewWorker(nil, WorkerConfig{ID: "w1", Lease: time.Second, ConfirmWindow: 300 * time.Millisecond},
			handlers, quietLogger())
		if err == nil {
			t.Fatal("a window at or below the heartbeat interval (333ms here) should be rejected")
		}
	})

	t.Run("an explicit window not shorter than the lease is rejected", func(t *testing.T) {
		_, err := NewWorker(nil, WorkerConfig{ID: "w1", Lease: time.Second, ConfirmWindow: time.Second},
			handlers, quietLogger())
		if err == nil {
			t.Fatal("a window equal to the lease should be rejected")
		}
	})

	t.Run("an explicit valid window is kept", func(t *testing.T) {
		w, err := NewWorker(nil, WorkerConfig{ID: "w1", Lease: time.Second, ConfirmWindow: 500 * time.Millisecond},
			handlers, quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		if w.cfg.ConfirmWindow != 500*time.Millisecond {
			t.Fatalf("window = %v, want 500ms", w.cfg.ConfirmWindow)
		}
	})
}