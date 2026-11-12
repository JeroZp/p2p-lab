# Wire protocol

Every implementation (Go, Python) must follow this exactly. The format is
defined by what the Go implementation's `encoding/json` produces; the
golden fixtures in `python/tests/golden/` are real bytes captured from it.

## Framing

Each message is a 4-byte big-endian unsigned length, followed by that many
bytes of JSON. Maximum message size: 10 MB.

```
[ length: uint32 BE ][ JSON body ]
```

## Envelope

```json
{"type": "gossip", "from": [0, 1, 2, ...], "id": "g-1", "payload": {...}}
```

| Field | Type | Notes |
|---|---|---|
| `type` | string | message type, see below |
| `from` | array of 32 integers | sender's NodeID |
| `id` | string | unique per message; replies echo the request's id |
| `payload` | object | omitted when there is none |

## Encoding rules (from Go's `encoding/json`)

- **NodeIDs and hashes** (`[32]byte` in Go) are JSON **arrays of 32
  integers**, not strings.
- **Byte slices** (`[]byte` in Go) are **base64 strings** (standard
  alphabet, with padding). An absent value is `null`.
- **DHT entries have capitalized keys** (`"ID"`, `"Addr"`): the Go struct
  has no json tags, so Go uses the field names as they are.
- Unknown fields are ignored, missing fields take zero values.

NodeID = SHA-256 of the node's raw 32-byte Ed25519 public key.

## Connection rules

- The **dialing** side sends `handshake` first. The accepting side
  registers the peer and replies with its own `handshake`.
- Both sides send a `heartbeat` every 2s. A connection that receives
  nothing for **6 seconds is closed**. A peer that never sends anything
  gets disconnected.

## Message types

| Type | Payload | Reply |
|---|---|---|
| `handshake` | none, `id: "handshake"` | `handshake` |
| `heartbeat` | none; `from` may be all zeros, `id` empty | none |
| `gossip` | `{"ttl": int, "data": base64, "sent_at": unix nanos}` | none; forward with `ttl - 1` while `ttl > 1`, keeping `id` and `from` |
| `dht_find_node` | `{"target": NodeID, "addr": string}` | `dht_find_reply` |
| `dht_find_reply` | `{"nodes": [{"ID": NodeID, "Addr": string}]}` | — |
| `dht_store` | `{"key": string, "value": base64}` | none |
| `dht_find_value` | `{"key": string, "value": null}` | `dht_value_reply` if found, else `dht_find_reply` |
| `dht_value_reply` | `{"values": [base64, ...]}` | — |
| `chunk_request` | `{"hash": hash}` | `chunk_reply` |
| `chunk_reply` | `{"hash": hash, "data": base64, "found": bool}` | — |

Gossip messages are deduplicated by `id`.