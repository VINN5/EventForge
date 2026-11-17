# 0011: The idempotency request hash is a versioned, documented format

Status: accepted

## Context
`idempotency_keys.request_hash` is stored. If the hash of the same request ever
changes, a retry with a known key is wrongly rejected as a conflict.

## Decision
Hash the request as the client sent it (defaults are not applied). The format
`v1`: the fields in a fixed order (tag `eventforge/submit/v1`, tenant, queue,
type, downstream, canonical payload, priority, delay in nanoseconds, ordering
key, max attempts), each framed as an 8-byte big-endian length plus bytes,
SHA-256, stored as `v1:<hex>`.

The payload is rewritten into a canonical form by our own encoder, not
`encoding/json`: keys sorted by UTF-8 bytes, no whitespace, numbers kept exactly
as written, only `"` and `\` backslash-escaped, every other byte below 0x20 as
`\u00xx` in lowercase hex, everything else raw UTF-8. Duplicate keys: the last
wins, as in Postgres `jsonb`. Invalid UTF-8 and trailing data are rejected.

## Consequences
- Golden tests pin the output for awkward inputs; changing v1 breaks them on
  purpose. A change needs a `v2` and a comparison that understands both.
- `1`, `1.0` and `1e2` are different requests: a false conflict is safe, a false
  match is not.
- Hashing before defaults means omitting a field and passing its default value
  are different requests, and changing a default later cannot break stored hashes.

## Rejected
- `json.Marshal` of the decoded payload: its escaping has changed between Go
  releases, so the format would depend on the compiler version.
- Hashing after defaults: changing a default would invalidate stored hashes.