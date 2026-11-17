package core

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// partitionProxy sits between a client and Postgres and can freeze the traffic
// in both directions without closing any connection. To both ends that is what
// a network partition looks like: nothing arrives and nothing is refused.
type partitionProxy struct {
	ln     net.Listener
	target string

	mu     sync.Mutex
	cond   *sync.Cond
	frozen bool
	closed bool
	conns  []net.Conn
}

func newPartitionProxy(t *testing.T, target string) *partitionProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &partitionProxy{ln: ln, target: target}
	p.cond = sync.NewCond(&p.mu)
	go p.acceptLoop()
	t.Cleanup(p.Close)
	return p
}

func (p *partitionProxy) Addr() string { return p.ln.Addr().String() }

func (p *partitionProxy) Freeze() {
	p.mu.Lock()
	p.frozen = true
	p.mu.Unlock()
}

func (p *partitionProxy) Thaw() {
	p.mu.Lock()
	p.frozen = false
	p.mu.Unlock()
	p.cond.Broadcast()
}

func (p *partitionProxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	conns := p.conns
	p.mu.Unlock()
	p.cond.Broadcast()
	p.ln.Close()
	for _, c := range conns {
		c.Close()
	}
}

func (p *partitionProxy) acceptLoop() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			client.Close()
			continue
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			client.Close()
			server.Close()
			return
		}
		p.conns = append(p.conns, client, server)
		p.mu.Unlock()
		go p.pipe(server, client)
		go p.pipe(client, server)
	}
}

func (p *partitionProxy) pipe(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if !p.waitUntilThawed() {
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *partitionProxy) waitUntilThawed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.frozen && !p.closed {
		p.cond.Wait()
	}
	return !p.closed
}

func testDBURL(t *testing.T) *url.URL {
	t.Helper()
	raw := os.Getenv("TEST_DATABASE_URL")
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		t.Fatalf("TEST_DATABASE_URL is not a usable URL: %q (%v)", raw, err)
	}
	return u
}

func testDBHostPort(t *testing.T) string {
	t.Helper()
	u := testDBURL(t)
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	return net.JoinHostPort(u.Hostname(), port)
}

// poolThrough returns a pool whose connections go through the proxy.
func poolThrough(t *testing.T, proxyAddr string) *pgxpool.Pool {
	t.Helper()
	u := *testDBURL(t)
	u.Host = proxyAddr
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pool through proxy: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// A worker that is alive but cut off from the database must stop its handler
// before another worker can take the job over. Otherwise the same job runs in
// two places at once, and the fence cannot undo what the first one already did
// outside the database.
func TestPartitionedWorker_HandlerIsCancelledBeforeAnotherWorkerTakesTheJob(t *testing.T) {
	pool := testPool(t) // direct connection, used by worker B and the assertions
	proxy := newPartitionProxy(t, testDBHostPort(t))
	viaProxy := poolThrough(t, proxy.Addr())
	id := submitNoop(t, pool, SubmitParams{MaxAttempts: 8})

	const lease = 2 * time.Second

	var aStarted, aCancelled, bStarted, bRuns atomic.Int64 // unix nanos; 0 = not yet
	var aCauseIsLeaseLost atomic.Bool
	var aCauseText atomic.Value

	handlersA := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			aStarted.Store(time.Now().UnixNano())
			<-ctx.Done()
			cause := context.Cause(ctx)
			aCauseIsLeaseLost.Store(errors.Is(cause, ErrLeaseLost))
			aCauseText.Store(cause.Error())
			aCancelled.Store(time.Now().UnixNano())
			return cause
		},
	}
	handlersB := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			bRuns.Add(1)
			bStarted.Store(time.Now().UnixNano())
			return nil
		},
	}

	cfgA := fastRetryConfig("A")
	cfgA.Lease = lease
	cfgA.ReapInterval = time.Hour // A must not take anything back itself
	cfgB := fastRetryConfig("B")
	cfgB.Lease = lease
	cfgB.ReapInterval = 100 * time.Millisecond

	cancelA, doneA := startWorker(t, viaProxy, cfgA, handlersA)
	cancelB := func() {}
	var doneB <-chan error
	// Cleanups run last-in first: thaw, then stop the workers, then close the pools.
	t.Cleanup(func() { cancelA(); cancelB() })
	t.Cleanup(proxy.Thaw)

	waitFor(t, 5*time.Second, func() bool { return aStarted.Load() != 0 }, "worker A to start the job")

	frozenAt := time.Now()
	proxy.Freeze()
	cancelB, doneB = startWorker(t, pool, cfgB, handlersB)

	waitFor(t, 15*time.Second, func() bool { return bStarted.Load() != 0 }, "worker B to take the job over")

	cancelledAt := aCancelled.Load()
	if cancelledAt == 0 {
		t.Fatal("worker B started the job while A's handler was still running: " +
			"the job ran in two places at once")
	}
	if cancelledAt >= bStarted.Load() {
		t.Fatalf("A's handler was cancelled %v after B started; it must be cancelled before",
			time.Duration(cancelledAt-bStarted.Load()))
	}
	if latency := time.Unix(0, cancelledAt).Sub(frozenAt); latency >= lease {
		t.Fatalf("A's handler was cancelled %v after the partition began; "+
			"that is not before a %v lease can expire", latency, lease)
	}
	if !aCauseIsLeaseLost.Load() {
		t.Fatalf("cause = %v, want it to wrap ErrLeaseLost", aCauseText.Load())
	}
	if text, _ := aCauseText.Load().(string); !strings.Contains(text, "unconfirmed") {
		t.Fatalf("cause text = %q, want it to say the lease was unconfirmed", text)
	}

	proxy.Thaw()
	waitFor(t, 10*time.Second, func() bool {
		return readJob(t, pool, id).State == "succeeded"
	}, "worker B to finish the job")
	stopWorker(t, cancelA, doneA)
	stopWorker(t, cancelB, doneB)

	if got := readJob(t, pool, id); got.Attempts != 2 || got.Fence != 2 {
		t.Fatalf("row %+v, want attempts 2 and fence 2 (A's claim, then B's)", got)
	}
	if bRuns.Load() != 1 {
		t.Fatalf("B ran the handler %d times, want 1", bRuns.Load())
	}
}

// A short blip must not cancel anything. This protects the fix from becoming
// over-eager: a handler longer than the lease, on a worker whose connection
// freezes for less than one heartbeat interval, must run to the end.
func TestPartitionedWorker_ABriefBlipDoesNotCancelTheHandler(t *testing.T) {
	pool := testPool(t)
	proxy := newPartitionProxy(t, testDBHostPort(t))
	viaProxy := poolThrough(t, proxy.Addr())
	id := submitNoop(t, pool, SubmitParams{MaxAttempts: 8})

	var started, cancelled, runs atomic.Int64
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			runs.Add(1)
			started.Store(time.Now().UnixNano())
			select {
			case <-ctx.Done():
				cancelled.Store(time.Now().UnixNano())
				return context.Cause(ctx)
			case <-time.After(2500 * time.Millisecond): // longer than the lease
				return nil
			}
		},
	}
	cfg := fastRetryConfig("A")
	cfg.Lease = 2 * time.Second
	cfg.ReapInterval = 100 * time.Millisecond

	cancel, done := startWorker(t, viaProxy, cfg, handlers)
	t.Cleanup(cancel)
	t.Cleanup(proxy.Thaw)

	waitFor(t, 5*time.Second, func() bool { return started.Load() != 0 }, "the handler to start")
	time.Sleep(300 * time.Millisecond)
	proxy.Freeze()
	time.Sleep(400 * time.Millisecond) // shorter than one heartbeat interval (about 667 ms)
	proxy.Thaw()

	waitFor(t, 10*time.Second, func() bool {
		return readJob(t, pool, id).State == "succeeded"
	}, "the job to succeed")
	stopWorker(t, cancel, done)

	if cancelled.Load() != 0 {
		t.Fatal("the handler was cancelled by a blip shorter than one heartbeat interval")
	}
	if got := readJob(t, pool, id); got.Attempts != 1 || runs.Load() != 1 {
		t.Fatalf("row %+v, handler runs %d; want 1 attempt and 1 run", got, runs.Load())
	}
}