# What We're Removing and Why

## Files to Delete

```
src/kafka/mod.rs
src/kafka/codec.rs
src/kafka/protocol.rs
src/kafka/server.rs
src/controller/logical_wal.rs
src/controller/fetch.rs
src/controller/produce.rs
```

**Total: ~1020 lines of complex code**

## What These Files Do (That We Don't Need)

### src/kafka/* (~600 lines)
- Kafka wire protocol encoding/decoding
- API keys: CreateTopics (19), Produce (0), Fetch (1), Metadata (3)
- Correlation IDs, API versions, client IDs
- Complex nested structs for requests/responses
- Error code mapping to Kafka error codes
- Partition metadata responses

**We don't need:** Kafka compatibility. We're not building a Kafka replacement.

### src/controller/logical_wal.rs (~130 lines)
- Maps logical byte offsets → physical WAL offsets
- Accounts for 256-byte ENTRY_OVERHEAD per message
- Scans WAL to find where byte offset N lives
- Trims partial messages when offset lands mid-message
- THE SOURCE OF THE CURRENT BUG

**We don't need:** Byte-based offsets. Messages should have sequential IDs (0, 1, 2, ...).

### src/controller/fetch.rs (~220 lines)
- Routes fetch requests to leader or historical segments
- Handles offset-based fetching
- Forwards reads to other nodes
- Deals with historical segment offsets vs active head offsets
- Complex offset math to figure out which segment contains which offset

**We don't need:** Client-controlled offset seeking. Server should track consumer positions.

### src/controller/produce.rs (~70 lines)
- Routes produce requests to partition leader
- Forwards to other nodes
- Appends raw bytes to WAL without message structure

**We don't need:** Raw byte appends. Messages should be wrapped with IDs.

## What We Gain By Removing This

### 1. No More Offset Bug
The current bug where offset 0 returns the wrong message goes away completely. No more:
- Logical vs physical offset mapping
- Scanning WAL to find byte positions
- ENTRY_OVERHEAD confusion
- Cursor advancement arithmetic

### 2. Server-Tracked Consumer Positions
Instead of clients tracking `.walrus_offsets` files:
- Server remembers where each consumer is
- Restart client, automatically resume from last position
- Multiple consumers on same topic, each with own position
- No client-side state to lose or corrupt

### 3. Sequential Message IDs
Instead of byte offsets (0, 1, 11, 111, 1111):
- Simple message IDs (0, 1, 2, 3, 4)
- No arithmetic, just increment
- Easy to reason about: "give me message 42"

### 4. Simpler Wire Protocol
Instead of Kafka protocol overhead:
- Lightweight binary format (~15-25 bytes overhead vs 50-100)
- 6 simple commands vs complex API key system
- Direct message exchange, no nested structures

### 5. Consumer-Friendly API
Instead of:
```python
# Client has to manage offsets
offset = load_from_file()
data, wm = client.consume(topic, offset=offset)
offset += len(data)
save_to_file(offset)
```

Just:
```python
# Server handles everything
msg = client.consume(topic)
```

### 6. Less Code to Maintain
- 370 fewer lines overall
- No offset mapping logic
- No Kafka protocol compatibility burden
- Simpler mental model

### 7. Better Debugging
When something goes wrong:
- "Consumer X is at message 42" vs "Consumer X is at byte offset 1879"
- Can inspect message IDs directly
- No mapping layer to debug

### 8. Multiple Consumers Made Easy
Want 3 different apps consuming same topic?
```python
WalrusClient(consumer_id="worker-1")  # Starts at msg 0
WalrusClient(consumer_id="worker-2")  # Also starts at msg 0
WalrusClient(consumer_id="worker-3")  # Also starts at msg 0
```

Each tracks independently, server manages all positions.

## What Stays the Same

- Walrus WAL backend (bucket.rs) - still stores data
- Raft-based metadata (metadata.rs) - still manages topics/partitions
- Node-to-node forwarding (rpc.rs) - still routes to leaders
- Distributed architecture - still 3-node cluster

## Summary

**Remove:** 1020 lines of Kafka protocol + offset mapping complexity

**Get:**
- No offset bugs
- Server-tracked consumers
- Sequential message IDs
- Simpler protocol
- Consumer-friendly API
- Multiple consumers per topic
- 370 fewer lines total

We keep all the good parts (WAL, Raft, distribution) and delete all the Kafka baggage.
