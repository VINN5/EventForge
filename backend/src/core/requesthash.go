package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"unicode/utf8"
)

// requestHashTag is the first field of every hash. It names the encoding, so
// a future v2 can never produce the same bytes as v1.
const requestHashTag = "eventforge/submit/v1"

// requestHash fingerprints a submit request for the idempotency check. The
// encoding is a persistent format (stored in idempotency_keys.request_hash);
// the spec is at the top of requesthash_test.go. Change it only by adding a
// new version, never by editing v1.
//
// It hashes the request as the client sent it: defaults are not applied.
func requestHash(p SubmitParams) (string, error) {
	payload, err := canonicalJSON(p.Payload)
	if err != nil {
		return "", fmt.Errorf("request hash: %w", err)
	}

	h := sha256.New()
	// writeField frames one field: 8-byte big-endian length, then the bytes.
	// hash.Hash.Write never returns an error, so its result is ignored.
	writeField := func(b []byte) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(b)))
		h.Write(n[:])
		h.Write(b)
	}

	writeField([]byte(requestHashTag))
	writeField([]byte(p.TenantID))
	writeField([]byte(p.Queue))
	writeField([]byte(p.Type))
	writeField([]byte(p.Downstream))
	writeField(payload)
	writeField([]byte(strconv.Itoa(p.Priority)))
	writeField([]byte(strconv.FormatInt(int64(p.RunAfter), 10))) // nanoseconds
	writeField([]byte(p.OrderingKey))
	writeField([]byte(strconv.Itoa(p.MaxAttempts)))

	return "v1:" + hex.EncodeToString(h.Sum(nil)), nil
}

// canonicalJSON rewrites one JSON document into the canonical form described
// in the spec: keys sorted by their UTF-8 bytes, no whitespace, numbers kept
// exactly as written, and our own string escaping.
//
// It does not use json.Marshal on purpose: Go's encoder has changed its
// escaping rules between releases, and a persistent format must not depend on
// the compiler version.
func canonicalJSON(raw []byte) ([]byte, error) {
	if !utf8.Valid(raw) {
		return nil, errors.New("payload is not valid UTF-8")
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // numbers arrive as their original text, not as float64
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("payload is not valid JSON: %w", err)
	}
	// Decode stops after the first value. Anything after it other than
	// whitespace (a second value, junk) is an error: Token must say EOF.
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("payload has data after the JSON value")
	}

	var buf bytes.Buffer
	if err := writeCanonical(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCanonical(buf *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if x {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case json.Number:
		buf.WriteString(x.String()) // the literal text from the input
	case string:
		writeCanonicalString(buf, x)
	case []any:
		buf.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		// Decoding into a map keeps the last value of a duplicate key,
		// which is also what Postgres jsonb does.
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys) // Go compares strings byte by byte
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeCanonicalString(buf, k)
			buf.WriteByte(':')
			if err := writeCanonical(buf, x[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("unexpected JSON value of type %T", v)
	}
	return nil
}

const lowerHex = "0123456789abcdef"

// writeCanonicalString writes a JSON string with exactly three rules:
// '"' and '\' get a backslash, every other byte below 0x20 becomes \u00xx in
// lowercase hex, and everything else is copied through unchanged.
//
// Looping over bytes is safe for UTF-8: every byte of a multi-byte character
// is 0x80 or above, so it can never equal one of the special ASCII bytes.
func writeCanonicalString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			buf.WriteString(`\"`)
		case c == '\\':
			buf.WriteString(`\\`)
		case c < 0x20:
			buf.WriteString(`\u00`)
			buf.WriteByte(lowerHex[c>>4])
			buf.WriteByte(lowerHex[c&0x0f])
		default:
			buf.WriteByte(c)
		}
	}
	buf.WriteByte('"')
}