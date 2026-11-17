package core

import (
	"strings"
	"testing"
)

// These tests pin down requestHash, which turns a SubmitParams into the
// fingerprint stored in idempotency_keys.request_hash.
//
// The encoding is a PERSISTENT FORMAT: hashes already stored must keep
// matching after an upgrade. Spec (v1):
//
//	fields, in order: "eventforge/submit/v1", TenantID, Queue, Type,
//	Downstream, canonical payload, Priority, RunAfter (nanoseconds),
//	OrderingKey, MaxAttempts.
//	Integers are written as decimal text. Every field is framed as an
//	8-byte big-endian length followed by its bytes.
//	Canonical payload: object keys sorted, no whitespace, numbers kept as
//	written, no HTML escaping.
//	Result: "v1:" + lowercase hex SHA-256.
//
// A "golden" test compares the output with a literal string written into the
// test. If you change the encoding, the golden test fails on purpose.

// basePerRequest is a fully populated request; the tests below change one
// field at a time.
func baseRequest() SubmitParams {
	return SubmitParams{
		TenantID:    "tenant-1",
		Queue:       "default",
		Type:        "send_email",
		Downstream:  "email_provider",
		Payload:     []byte(`{ "b": 2, "a": [1, "x"] }`),
		Priority:    5,
		RunAfter:    30 * 1e9, // 30 seconds, as a time.Duration in nanoseconds
		OrderingKey: "order-9",
		MaxAttempts: 6,
	}
}

func mustHash(t *testing.T, p SubmitParams) string {
	t.Helper()
	h, err := requestHash(p)
	if err != nil {
		t.Fatalf("requestHash: %v", err)
	}
	return h
}

func TestRequestHash_Golden(t *testing.T) {
	cases := []struct {
		name string
		p    SubmitParams
		want string
	}{
		{
			name: "fully populated request",
			p:    baseRequest(),
			want: "v1:46af7017509d3381ae32e9b22b421414ce7395f5813501b0bfadfd070cd1d17f",
		},
		{
			name: "minimal request, everything else empty or zero",
			p:    SubmitParams{TenantID: "t1", Type: "x", Payload: []byte(`{}`)},
			want: "v1:4bb63dc462413686149f5ae6052d1dcbb6f58644d0cfcf89fd865e55cd9786ae",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := mustHash(t, c.p); got != c.want {
				t.Fatalf("hash encoding changed\n got: %s\nwant: %s", got, c.want)
			}
		})
	}
}

func TestRequestHash_Format(t *testing.T) {
	h := mustHash(t, baseRequest())
	if !strings.HasPrefix(h, "v1:") || len(h) != len("v1:")+64 {
		t.Fatalf("unexpected format: %q", h)
	}
}

func TestRequestHash_Deterministic(t *testing.T) {
	a := mustHash(t, baseRequest())
	b := mustHash(t, baseRequest())
	if a != b {
		t.Fatalf("same request hashed differently: %s vs %s", a, b)
	}
}

// Two spellings of the same JSON document must give the same hash, otherwise
// a client whose library reorders keys would get a false conflict.
func TestRequestHash_PayloadSpellingDoesNotMatter(t *testing.T) {
	want := mustHash(t, baseRequest())

	spellings := map[string]string{
		"compact":       `{"a":[1,"x"],"b":2}`,
		"reordered":     `{"b":2,"a":[1,"x"]}`,
		"lots of space": "{\n\t\"a\" : [ 1 , \"x\" ],\n\t\"b\" : 2\n}",
	}
	for name, payload := range spellings {
		t.Run(name, func(t *testing.T) {
			p := baseRequest()
			p.Payload = []byte(payload)
			if got := mustHash(t, p); got != want {
				t.Fatalf("got %s, want %s", got, want)
			}
		})
	}
}

// "\u003c" and "<" are the same JSON string. Go's default encoder would turn
// "<" back into "\u003c"; the canonical form must not depend on that.
func TestRequestHash_StringEscapingDoesNotMatter(t *testing.T) {
	a := baseRequest()
	a.Payload = []byte(`{"q":"a<b&c>d"}`)
	b := baseRequest()
	b.Payload = []byte(`{"q":"a\u003cb\u0026c\u003ed"}`)
	if mustHash(t, a) != mustHash(t, b) {
		t.Fatal("different spellings of the same string hashed differently")
	}
}

// Every field of the request must be part of the hash, otherwise two
// different requests could share an idempotency key without a conflict.
func TestRequestHash_EveryFieldMatters(t *testing.T) {
	base := mustHash(t, baseRequest())

	mutations := map[string]func(p *SubmitParams){
		"tenant":       func(p *SubmitParams) { p.TenantID = "tenant-2" },
		"queue":        func(p *SubmitParams) { p.Queue = "other" },
		"type":         func(p *SubmitParams) { p.Type = "send_sms" },
		"downstream":   func(p *SubmitParams) { p.Downstream = "sms_provider" },
		"payload":      func(p *SubmitParams) { p.Payload = []byte(`{"a":[1,"y"],"b":2}`) },
		"priority":     func(p *SubmitParams) { p.Priority = 6 },
		"run after":    func(p *SubmitParams) { p.RunAfter += 1 }, // one nanosecond
		"ordering key": func(p *SubmitParams) { p.OrderingKey = "order-10" },
		"max attempts": func(p *SubmitParams) { p.MaxAttempts = 7 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			p := baseRequest()
			mutate(&p)
			if got := mustHash(t, p); got == base {
				t.Fatalf("changing %s did not change the hash", name)
			}
		})
	}
}

// Without length framing, ("ab","c") and ("a","bc") would hash the same.
func TestRequestHash_FieldBoundariesAreUnambiguous(t *testing.T) {
	a := baseRequest()
	a.TenantID, a.Queue = "ab", "c"
	b := baseRequest()
	b.TenantID, b.Queue = "a", "bc"
	if mustHash(t, a) == mustHash(t, b) {
		t.Fatal("field boundaries are ambiguous")
	}
}

// Numbers must keep their exact digits. Converting to float64 would make
// these two integers equal.
func TestRequestHash_LargeIntegersStayDistinct(t *testing.T) {
	a := baseRequest()
	a.Payload = []byte(`{"n":9007199254740993}`)
	b := baseRequest()
	b.Payload = []byte(`{"n":9007199254740992}`)
	if mustHash(t, a) == mustHash(t, b) {
		t.Fatal("distinct large integers hashed the same")
	}
}

// The request is hashed as the client sent it, not after defaults are
// applied. So an omitted queue and an explicit "default" are different
// requests: a false conflict is safe, and changing a default later cannot
// break stored hashes.
func TestRequestHash_DefaultsAreNotApplied(t *testing.T) {
	a := baseRequest()
	a.Queue = ""
	b := baseRequest()
	b.Queue = "default"
	if mustHash(t, a) == mustHash(t, b) {
		t.Fatal("omitted queue and explicit default hashed the same")
	}
}

func TestRequestHash_RejectsInvalidPayload(t *testing.T) {
	bad := map[string]string{
		"not json":         `{oops`,
		"empty":            ``,
		"trailing garbage": `{} {}`,
	}
	for name, payload := range bad {
		t.Run(name, func(t *testing.T) {
			p := baseRequest()
			p.Payload = []byte(payload)
			if _, err := requestHash(p); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}

// goldenPayloadCases pins the canonical payload encoding for the awkward
// inputs. All use the baseRequest fields except the payload.
func TestRequestHash_GoldenPayloads(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    string
	}{
		{
			// é, ✓, an emoji (surrogate pair in the input), control characters
			// U+0001 and U+001F, newline, quote, backslash, U+2028 and DEL.
			// Controls become \u00xx; U+2028 and DEL stay raw.
			name:    "non-ASCII and control characters in a string",
			payload: `{"s":"h\u00e9llo \u2713 \ud83d\ude00 \u0001 \u001f \n \" \\ \u2028 \u007f"}`,
			want:    "v1:95f35a6545ac0e11db98cad4574e9db36935accaae70b0fcc227e439f37178bb",
		},
		{"top-level array", `[1,2]`, "v1:6d35788207cc74f178371afb4f85fe786030e4ebd93147b81f76f56f1ad042f7"},
		{"top-level number", `5`, "v1:860aaaccef9848dd293e3b3d8b1f03cc1eefc42284b4934e0744c216e7c8eaac"},
		{"top-level string", `"x"`, "v1:d08de9f6c0e568f65de1c1cdc66d6faf750be7ed4fff52c80587c567c29b151c"},
		{"top-level null", `null`, "v1:9e980ea1c18ff112b38ff57b6f246e1ace5ffa6bdcf99b88991648cd8fe221ee"},
		{
			// Last value wins, so this must equal the hash of {"a":2} below.
			name:    "duplicate keys, last wins",
			payload: `{"a":1,"a":2}`,
			want:    "v1:bb6a09f0e9303dd2e1950d85f39e628247c7578a0a09698aff33db3bf2ab0f3a",
		},
		{"the single-key object the duplicate collapses to", `{"a":2}`,
			"v1:bb6a09f0e9303dd2e1950d85f39e628247c7578a0a09698aff33db3bf2ab0f3a"},
		{
			// Sorted by UTF-8 bytes: "z" (0x7a) comes before "é" (0xc3 0xa9).
			name:    "non-ASCII key sorts by bytes",
			payload: `{"\u00e9":1,"z":2}`,
			want:    "v1:67df203cb28fd7c3e128bf609950214294caafe696f47b7e102cd0da84b28b2f",
		},
		{
			// Numbers keep their spelling: this is a different request from
			// [1,100,0,100]. A false conflict is safe; a false match is not.
			name:    "number spellings are kept",
			payload: `{"n":[1.0,1e2,-0,100]}`,
			want:    "v1:f906d3724dde4dfea72620e9a0f6f4380be05a092b09ad06fe817412acb6fc7d",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := baseRequest()
			p.Payload = []byte(c.payload)
			if got := mustHash(t, p); got != c.want {
				t.Fatalf("hash encoding changed\n got: %s\nwant: %s", got, c.want)
			}
		})
	}
}

// The same string written as raw UTF-8 and as a \u escape is one request.
// The Go interpreted string "\u00e9" below is the real é character.
func TestRequestHash_RawUTF8EqualsEscape(t *testing.T) {
	a := baseRequest()
	a.Payload = []byte("{\"s\":\"h\u00e9llo \u2713\"}")
	b := baseRequest()
	b.Payload = []byte(`{"s":"h\u00e9llo \u2713"}`)
	if mustHash(t, a) != mustHash(t, b) {
		t.Fatal("raw UTF-8 and escaped spelling hashed differently")
	}
}

// Invalid UTF-8 would otherwise be silently replaced with U+FFFD while
// decoding, so two different byte sequences could share a hash. Postgres
// would reject such a payload at insert anyway; we reject it up front.
func TestRequestHash_RejectsInvalidUTF8(t *testing.T) {
	p := baseRequest()
	p.Payload = []byte("{\"s\":\"\xff\xfe\"}") // interpreted string: raw bytes 0xff 0xfe
	if _, err := requestHash(p); err == nil {
		t.Fatal("expected an error for invalid UTF-8, got nil")
	}
}