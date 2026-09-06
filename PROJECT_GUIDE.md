# MiniKafka — Project Guide

A from-scratch distributed publish-subscribe message broker in C++17: a segmented
append-only commit log with crash recovery, topic partitioning with key-hash
routing, consumer groups with automatic rebalance, a multi-broker cluster with
Raft-inspired leader election, ISR-based replication, and automatic
partition-leader failover.

This document is for someone who has never seen this repo before. It explains
**how the project was actually built** (milestone by milestone, in the order that
made each layer buildable and testable on its own), **how the pieces fit
together** (the layering, and a full walk-through of one request from wire to
disk and back), and **what every file and function does**, folder by folder.

The original design brief this project was built from is `AGENT_BRIEF.md` in
this same directory — it's the milestone-by-milestone spec. This document is
the "as-built" map of what actually exists and how it's organized.

---

## 1. Quick start

```bash
sudo apt install -y build-essential cmake git   # g++ 11+, cmake 3.16+

cd kafka-project
cmake -S . -B build
cmake --build build -j$(nproc)      # GoogleTest is fetched automatically
ctest --test-dir build --output-on-failure
```

Single-broker smoke test:
```bash
./build/mk-broker --id 1 --port 9091 --data-dir /tmp/mkdata &
./build/mk-produce --broker localhost:9091 --topic events --key u1 --value hello
./build/mk-consume  --broker localhost:9091 --topic events --partition 0 --from-beginning
```

Three-broker cluster with replication:
```bash
PEERS="1@localhost:9101,2@localhost:9102,3@localhost:9103"
./build/mk-broker --id 1 --port 9101 --data-dir /tmp/b1 --peers $PEERS &
./build/mk-broker --id 2 --port 9102 --data-dir /tmp/b2 --peers $PEERS &
./build/mk-broker --id 3 --port 9103 --data-dir /tmp/b3 --peers $PEERS &
./build/mk-admin create-topic --broker localhost:9101 --topic orders \
    --partitions 4 --replication-factor 3
```

---

## 2. The layering, top to bottom

Every module only depends on the ones below it. This is also the literal
`target_link_libraries` graph in the root `CMakeLists.txt`.

```
cli/  (mk-broker, mk-produce, mk-consume, mk-admin — 4 separate executables)
   |
   v
broker/         (Broker — dispatches every wire request to the right subsystem)
   |         \          \            \
   v          v          v            v
group/    cluster/   replication/   storage/
(M3)      (M4)       (M5)           (M1)
   \          \          |            |
    \          \         v            v
     \          +---> net/       (Partition, Segment, Record)
      \                |
       +---------> protocol/  (wire structs, pure encode/decode)
                       |
                       v
                   common/  (crc32, big-endian codec, murmur2)
```

- **`common/`** — no dependencies on anything else in this project. Pure
  bit-twiddling utilities.
- **`storage/`** — depends only on `common/`. Knows nothing about networking.
  This is the single most important design rule in the codebase: `Partition`
  only ever touches disk.
- **`protocol/`** — depends on `common/` (codec) and `storage/` (it reuses
  `Record`'s on-disk frame format verbatim as the wire format for records).
  Pure structs plus `encode_x`/`decode_x` free functions. No I/O.
- **`net/`** — depends on `common/`. Generic TCP socket + framing +
  thread-per-connection server. Doesn't know what a "Produce request" is.
- **`group/`, `cluster/`, `replication/`** — each depends on `protocol/` (and
  `net/` for the two that talk broker-to-broker) but not on each other.
- **`broker/`** — the only module that depends on *all* of the above. `Broker`
  is where a raw `RequestFrame` becomes "oh, this is a Produce for topic X
  partition Y" and gets routed to the right subsystem.
- **`cli/`** — four independent `main()`s, each linking the whole `broker_lib`
  stack (client binaries reuse the exact same `protocol`/`net` code the broker
  uses server-side).

---

## 3. Repository layout

```
kafka-project/
  AGENT_BRIEF.md            original design brief (milestones M1-M7)
  PROJECT_GUIDE.md           this file
  CMakeLists.txt             root build: declares every library + the 4 CLI binaries
  src/
    common/                  crc32, big-endian codec, murmur2 hash
    storage/                 Record, Segment, Partition — the commit log itself
    protocol/                wire-format structs + encode/decode (no I/O)
    net/                     Socket, TcpServer — generic TCP + framing
    broker/                  Broker — the dispatcher that wires everything together
    group/                   GroupCoordinator — consumer group protocol
    cluster/                 ClusterMembership, ClusterController — Raft election + partition assignment
    replication/             IsrManager, FollowerFetcher — leader/follower data replication
  cli/
    mk-broker.cpp            the broker process
    mk-produce.cpp           one-shot producer
    mk-consume.cpp           polling consumer (direct partition or consumer-group mode)
    mk-admin.cpp             create-topic
  tests/
    storage/test_partition.cpp
    group/test_coordinator.cpp
    cluster/test_controller.cpp
    replication/test_isr_manager.cpp
    integration/test_kill_leader.cpp   the M6 headline test
```

---

## 4. Folder-by-folder, file-by-file

### `src/common/` — shared low-level utilities

No dependencies on the rest of the project. Everything here is a pure,
stateless function.

| File | What's in it |
|---|---|
| `crc32.h` / `.cpp` | `crc32(data, len)` — CRC-32/ISO-HDLC (the same table/polynomial zlib uses). Used to detect torn writes on recovery: every record on disk carries a CRC of its own body. |
| `codec.h` / `.cpp` | The big-endian wire/disk codec everything else builds on. `put_u8/u16/u32/u64/i16/i64`, `put_bytes`, `put_string` (2-byte length prefix), `put_blob` (4-byte length prefix) for encoding. `class Reader` for decoding — a cursor over a fixed byte buffer with `get_u8/u16/u32/u64/i16/i64/string/blob/raw(n)`, each of which throws `DecodeError` if the buffer runs out (this is exactly how torn-write detection works during log recovery — see §6). |
| `murmur2.h` / `.cpp` | `murmur2(data, len)` — a direct port of `org.apache.kafka.common.utils.Utils.murmur2`, Kafka's actual default-partitioner hash. Used for key-hash partition routing (`mk-produce`) and for picking a consumer-group's coordinator broker (`hash(groupId) % brokerCount`). |

### `src/storage/` — the commit log (M1)

This is the foundation everything else is built on: append-only segmented
files with crash recovery, completely ignorant of networking.

| File | What's in it |
|---|---|
| `record.h` / `.cpp` | `struct Record { offset, timestamp, key, value }` plus `encode_record` / `decode_record`. This is the exact on-disk *and* on-wire frame format: `totalLen(4) crc32(4) offset(8) timestamp(8) keyLen(4) key valueLen(4) value`, all big-endian. `decode_record` throws `DecodeError` on either a truncated frame (not enough bytes — a torn write) or a CRC mismatch (corruption) — the caller can't tell those apart and doesn't need to; both mean "stop here." |
| `segment.h` / `.cpp` | `class Segment` — one `<20-digit-offset>.log` + `.index` file pair. `create_empty()` for a brand-new segment. `scan_and_recover()` — byte-by-byte CRC-verified scan used **only** for the active segment at startup; truncates the file at the first torn/corrupt frame and rebuilds the sparse index from just the clean data. `load_index()` — for older, immutable segments: trusts them completely, just loads the `.index` file straight into memory, no scan. `append(records)` — encodes and writes already offset-assigned records, dropping a sparse `(relativeOffset, bytePosition)` index entry every `indexIntervalBytes` (4096) of data. `read_from(pos)` / `find_start_position(relOffset)` — the read path: binary-search the sparse index for the floor entry, then linear-scan forward from there. |
| `partition.h` / `.cpp` | `class Partition` — the public API everything above storage actually calls. Owns an ordered list of `Segment`s and decides which one is active. `recover()` — scans the directory, trusts all segments but the last, recovers the last via `scan_and_recover()`. `append(records)` — assigns offsets + timestamps, rolls to a new segment if the active one has hit `maxSegBytes` (default 128MB), returns the batch's base offset. `append_replicated(records)` *(M5)* — writes records that already carry a leader's real offsets verbatim (a follower replicating), never assigns offsets, never touches the high watermark. `read(startOffset, maxBytes)` — clamped to the high watermark (consumer-visible only); never returns a partial trailing record except a lone oversized one (so a fetcher can't stall forever). `read_up_to_leo(...)` *(M5)* — same logic, clamped to the true log end instead, used to serve replica fetches. `log_end_offset()` / `high_watermark()` / `set_high_watermark()`. **Thread-safe as of M5** — every public method locks an internal mutex, because M5's background ISR sweep and multiple concurrent client connections can genuinely touch the same `Partition` object at once. |

### `src/protocol/` — wire format (M2, extended every milestone after)

Pure `encode_x`/`decode_x` functions over `common/codec.h`'s `Reader`/buffer —
zero I/O, zero state. `net/server.cpp` reads the raw frame bytes off the
socket and hands them here; the reverse for responses.

| File | What's in it |
|---|---|
| `types.h` | `enum class RequestType` (the wire `requestType` field: `Produce=1, Fetch=2, Metadata=3, Replicate=10, BrokerHeartbeat=11, LeaderAndISR=12, RequestVote=13, ControllerHeartbeat=14, IsrUpdate=15, JoinGroup=20, SyncGroup=21, Heartbeat=22, LeaveGroup=23, CommitOffset=24, FetchOffset=25, FindCoordinator=26, CreateTopic=27`) and `enum class ErrorCode` (`None, UnknownTopic, NotLeaderForPartition, RequestTimedOut, UnknownMemberId, RebalanceInProgress, UnknownRequestType`). `IsrUpdate` and `CreateTopic` got numbers not in the original brief's table — noted inline. |
| `requests.h` / `.cpp` | Every request/response struct pair in the system, each with its own `encode_*`/`decode_*` functions. Grouped by milestone: **Produce/Fetch/Metadata** (M2, client-facing) — `ProduceRequest/Response`, `FetchRequest/Response`, `MetadataRequest/Response` (+ `PartitionMetadata`), `ReplicaFetchRequest` (M5 — same shape as `FetchRequest` plus a `replicaId` field, since the leader needs to know *which* broker is asking), `CreateTopicRequest/Response`. **Consumer groups** (M3) — `FindCoordinator`, `JoinGroup`, `SyncGroup` (+ `SyncGroupAssignment`), `Heartbeat`, `LeaveGroup`, `CommitOffset`, `FetchOffset`, all Request/Response pairs. **Cluster/Raft** (M4) — `RequestVote`, `ControllerHeartbeat`, `BrokerHeartbeat`, `LeaderAndISR` (+ `LeaderAndISREntry`). **Replication** (M5) — `IsrUpdate`. `ProduceResponse`/`FetchResponse`/`CreateTopicResponse` all carry redirect fields (`leaderHost`/`leaderPort` or `controllerHost`/`controllerPort`) populated only on `NotLeaderForPartition`. |

### `src/net/` — generic TCP transport (M2)

Doesn't know anything about brokers, topics, or partitions — just bytes and
connections.

| File | What's in it |
|---|---|
| `socket.h` / `.cpp` | `class Socket` — RAII POSIX socket wrapper (move-only). `Socket::connect(host, port)` / `Socket::listen(host, port)` / `.accept()`. `send_all`/`recv_all` loop until the full length is transferred or throw. `RequestFrame{requestType, correlationId, payload}` / `ResponseFrame{correlationId, payload}` plus `read_request_frame`/`write_request_frame`/`read_response_frame`/`write_response_frame` — the length-prefixed frame implementation shared by every client and the server. |
| `server.h` / `.cpp` | `class TcpServer` — binds, then one `std::thread` per accepted connection (`M2`'s "simple first" model; a session loop reads a request frame, calls the supplied `Handler` callback, writes back the response, repeat until the connection drops). This is literally the only networking primitive `mk-broker` needs; it has no idea what's inside a `RequestFrame`. |

### `src/broker/` — the dispatcher

The one module that ties every other subsystem together.

| File | What's in it |
|---|---|
| `broker.h` / `.cpp` | `class Broker`. Constructed with `(id, host, port, dataDir, peers=nullopt, uncleanLeaderElectionEnable=false)` — `peers` absent means **standalone mode** (exactly M1-M3 behavior: this broker is leader of everything, auto-creates topics on first touch); `peers` present means **cluster mode** (M4+: Raft election starts immediately, `ClusterController`/`ClusterMembership`/`IsrManager` all get constructed). `handle(RequestFrame) -> ResponseFrame` is the single entry point — a big `switch` on `RequestType` dispatching to one `handle_*` method per request type (`handle_produce`, `handle_fetch`, `handle_metadata`, `handle_create_topic`, the 7 `handle_*` group methods that just forward into `GroupCoordinator`, `handle_request_vote`/`handle_controller_heartbeat`/`handle_broker_heartbeat`/`handle_leader_and_isr`/`handle_isr_update` forwarding into `ClusterController`, `handle_replicate` serving `FETCH_REPLICA`). `get_or_create_partition` / `partition_count_for` manage the standalone-mode `partitions_`/`topics_` maps. `reconcile_replication()` — called after any cluster assignment change; diffs "what does the cluster say I should be doing" against "what am I currently doing," starting/stopping `FollowerFetcher`s and registering/deregistering with `IsrManager` accordingly. This is also where a freshly-*promoted* partition's high watermark gets jumped to its own log-end-offset (M6) — a follower's HWM was never consumer-visible while following, so without this a new leader looks empty right after promotion. |

### `src/group/` — consumer groups (M3)

| File | What's in it |
|---|---|
| `coordinator.h` / `.cpp` | `class GroupCoordinator`, one per broker. `join_group` — opens (or joins) a short rebalance window: the calling thread blocks on a condition variable until the window closes, then whichever thread closes it picks the leader (lexicographically smallest member id) and wakes everyone with the same generation. `sync_group` — the leader's call carries the real partition assignment and unblocks the (also blocked) followers' calls with their slice. `heartbeat` — updates the caller's liveness and, opportunistically (no background thread), evicts anyone who's gone stale, flagging the group for a forced rejoin. `leave_group`, `commit_offset`/`fetch_offset` (flat-file persisted per group under `<dataDir>/__consumer_offsets/`), `find_coordinator` (standalone mode: always "this broker" — M4 replaces this with real routing at the `Broker` level). |

### `src/cluster/` — multi-broker cluster (M4, extended in M5/M6)

| File | What's in it |
|---|---|
| `membership.h` / `.cpp` | `struct Address{host, port}`. `class ClusterMembership` — the static `--peers` table (`id -> Address`, including self) plus a persistent, reconnect-on-failure `Socket` per peer. `call(peerId, RequestFrame) -> optional<ResponseFrame>` is the one method everything else in `cluster/`/`replication/` uses to talk to a specific peer broker; `nullopt` on any failure, treated exactly like an RPC timeout. |
| `controller.h` / `.cpp` | `class ClusterController` — the Raft-inspired election plus everything the elected controller is responsible for. **Election** (M4): `role_` (Follower/Candidate/Leader), `currentTerm_`/`votedFor_` persisted to `<dataDir>/raft_state`. A single background thread either runs the randomized 150-300ms election timer or, as leader, broadcasts `ControllerHeartbeat` (election-critical, every 50ms) and `BrokerHeartbeat` (liveness tracking only, every 200ms). `handle_request_vote`/`handle_controller_heartbeat` are the RPC handlers. **Topic/partition assignment** (M5): `create_topic(topic, numPartitions, replicationFactor)` — controller-only, round-robins leaders across currently-alive brokers, assigns `replicationFactor` consecutive brokers as replicas, persists to `<dataDir>/cluster_assignments`, broadcasts `LEADER_AND_ISR`. `assignment_for`/`assignments_for_topic`/`all_assignments` — every broker's read-only view of the cluster's routing table. `apply_leader_and_isr` — follower-side: cache an incoming broadcast. `update_isr` (M5 checkpoint B) — a partition leader reports its ISR changed; controller-only, updates + persists + rebroadcasts. **Partition failover** (M6): `check_partition_failover()`, run on the same 200ms tick as the `BrokerHeartbeat` broadcast — for every assignment whose leader isn't currently alive, promotes an alive ISR member (or, only if `uncleanLeaderElectionEnable_` is set, any alive replica at all — the availability/durability tradeoff the brief's `unclean.leader.election.enable` flag names). `set_on_assignment_change`/`set_on_isr_change`-style callback (`AssignmentChangeCallback`) lets `Broker` react when *its own* broker is the one just promoted, since a broadcast never reaches its own sender. |

### `src/replication/` — data replication + ISR (M5)

| File | What's in it |
|---|---|
| `isr_manager.h` / `.cpp` | `class IsrManager`, leader-side, one per broker, covering every partition it currently leads. `set_leading`/`stop_leading` register/deregister a partition (called from `Broker::reconcile_replication`). `on_replica_fetch` — called on every incoming `FETCH_REPLICA`, records the follower's progress + timestamp, recomputes ISR and the high watermark. `on_leader_append` — called right after a local produce append, so the optimistic HWM-jump-to-LEO that `Partition::append` does on its own gets immediately reclamped to the real min-across-ISR value rather than staying wrong until the next unrelated replica fetch. A single ~200ms sweep thread re-evaluates every locally-led partition purely from elapsed time, so a follower that goes silent still eventually drops out of ISR with zero active traffic. `wait_for_hwm(topic, partitionId, requiredOffset, timeoutMs)` (M5 checkpoint B) — this is what `acks=all` blocks on. `set_on_isr_change` callback — fired (outside the internal lock, since it may do network I/O) whenever ISR membership actually changes, wired by `Broker` to report the change to the cluster controller. |
| `follower.h` / `.cpp` | `class FollowerFetcher` — one instance per (topic, partition) a broker replicates but doesn't lead. Owns its own reconnect-on-failure socket to the current leader. Pull loop: request `FETCH_REPLICA` starting at its own log-end-offset, append whatever comes back via `Partition::append_replicated`, loop immediately if it got data (catch up fast) or back off ~30ms if there was nothing new. |

### `cli/` — the four executables

| File | What it does |
|---|---|
| `mk-broker.cpp` | Parses `--id --port --data-dir [--host] [--peers id@host:port,...] [--unclean-leader-election]`, constructs a `Broker` + `TcpServer`, runs forever. |
| `mk-produce.cpp` | One record per process invocation. `--broker --topic [--key] [--value] [--file] [--acks] [--partition]`. Resolves the target partition via key-hash (`murmur2`) or uniform-random if no key, resolves the current leader's address via `Metadata` up front, then produces with a resilient retry loop (M6): exponential backoff (50ms→2s cap, ~10 attempts), catches connection failures the same way it treats `NotLeaderForPartition`, and re-resolves the leader from scratch on the *original* `--broker` address if a redirect target turns out to be dead — this is what lets a single produce call survive a leader failover. |
| `mk-consume.cpp` | Two modes. **Direct** (`--partition N`): fetch one fixed partition in a poll loop, printing `offset=… key=… value=…`. **Group** (`--group G`): the full consumer-group protocol — `FindCoordinator` → `JoinGroup` → `SyncGroup` (leader computes a client-side range assignment; followers block for it) → `FetchOffset`-based resume → a background `Heartbeat` thread on its own socket → round-robin `Fetch` across assigned partitions with periodic `CommitOffset`, auto-rejoining on `RebalanceInProgress`. Resolves each assigned partition's real leader via `Metadata` and maintains one socket per partition, following redirects reactively. |
| `mk-admin.cpp` | `create-topic --broker H:P --topic T --partitions N [--replication-factor R]`. Redirects to the controller if it hits a non-controller broker first. |

### `tests/`

| File | What it covers |
|---|---|
| `storage/test_partition.cpp` | Round-trip across forced segment rolls + a process restart; torn-write recovery (the case manual testing can't reliably reproduce); high-watermark clamping; `append_replicated`/`read_up_to_leo` round-trip (M5). |
| `group/test_coordinator.cpp` | Offset persistence across a coordinator restart; solo-member-becomes-leader-with-full-assignment; heartbeat rejecting unknown/stale members. |
| `cluster/test_controller.cpp` | Raft term/votedFor persistence across a restart; stale-term `RequestVote` rejection; `ControllerHeartbeat` updating term/leader on a higher term. |
| `replication/test_isr_manager.cpp` | Pure ISR/HWM computation: a never-fetched replica stays out of ISR and doesn't hold HWM back; a lagging replica does hold it back until it catches up; `stop_leading` tears down tracking. |
| `integration/test_kill_leader.cpp` | **The M6 headline test.** Spawns 3 real `mk-broker` processes, creates a 3-replica topic, produces 1000 records with `acks=all` via the real `mk-produce` binary, kills the actual partition leader (found via a live `Metadata` query, not assumed) partway through, consumes everything back via the real `mk-consume` binary, and asserts every value was seen — a set comparison, not an exact count, since a produce retry against a dead broker isn't idempotent (no dedup mechanism exists, matching the brief's exclusion of exactly-once semantics from scope) and a rare duplicate is an honest possibility. Deliberately shells out to the shipped binaries rather than reimplementing produce/consume logic, so it's testing what actually ships. Noticeably slower (~40s) than the rest of the suite. |

---

## 5. Walking one request all the way through: `Produce` with `acks=all`

This is the fastest way to actually understand how the layers compose. Follow
a single keyed produce call from `mk-produce` to disk and back, in a 3-broker
cluster.

1. **`mk-produce`** connects to the given `--broker`, sends a `Metadata`
   request for the topic (`protocol::MetadataRequest` → `net::write_request_frame`
   → raw bytes on the wire).
2. That broker's **`net::TcpServer`** session thread reads the frame
   (`read_request_frame`), hands it to **`Broker::handle`**, which dispatches
   to `handle_metadata`. In cluster mode this reads `ClusterController::assignments_for_topic`
   — the controller-broadcast routing table this broker cached from the last
   `LEADER_AND_ISR` it received.
3. `mk-produce` computes the target partition (`murmur2(key) & 0x7fffffff %
   partitionCount`) and, since `Metadata` already told it who really leads
   that partition, connects directly there.
4. **`Broker::handle_produce`** on the leader: checks `ClusterController::assignment_for`
   to confirm it really is the leader (if not — a stale route — it returns
   `NotLeaderForPartition` with the real leader's address, and `mk-produce`
   retries there). Calls **`Partition::append`** (assigns the offset,
   optimistically bumps the high watermark to the new log-end-offset), then
   **`IsrManager::on_leader_append`** immediately reclamps the HWM to the true
   `min(LEO across ISR)` — undoing the optimistic bump if any replica hasn't
   caught up yet.
5. Because `acks=all`, `handle_produce` calls **`IsrManager::wait_for_hwm`**,
   blocking this connection's own thread until the HWM actually passes the
   batch's last offset (or 5s times out).
6. Meanwhile, on each replica broker, a **`FollowerFetcher`** thread is in a
   tight pull loop sending `FETCH_REPLICA` requests. The leader's
   `handle_replicate` serves them via `Partition::read_up_to_leo` (past the
   consumer-visible HWM — a follower needs the true log end) and reports each
   fetch to `IsrManager::on_replica_fetch`, which recomputes ISR/HWM and wakes
   the blocked `wait_for_hwm` call once every ISR member has caught up.
7. `handle_produce` returns; `mk-produce` prints the offset.
8. A consumer somewhere calls `mk-consume`, which resolves the leader via
   `Metadata` the same way and calls **`Partition::read`** (HWM-clamped —
   only sees what step 6 just confirmed the ISR has).

If the leader dies between steps 4 and 8: `ClusterController::check_partition_failover`
(running on the controller's own 200ms tick) notices the leader is missing
from `alive_broker_ids()`, promotes an alive ISR member, broadcasts
`LEADER_AND_ISR`. Every broker's `reconcile_replication()` reacts — the
promoted broker jumps its own HWM to its LEO and starts leading;
survivors' `FollowerFetcher`s get torn down and recreated pointed at the new
leader. `mk-produce`'s retry loop (step 3-4, if it was mid-call) or the next
call's fresh `Metadata` lookup finds the new leader and continues.

---

## 6. Design principles that show up everywhere

These aren't specific to one file — they're patterns repeated across the
whole codebase, worth recognizing once so the rest reads faster:

- **Storage knows nothing about networking.** `Partition` only touches disk.
  Nothing in `storage/` includes anything from `net/` or `protocol/`.
- **Thread-per-connection can just block.** Instead of callbacks/futures,
  blocking operations (a consumer group's rebalance window in
  `GroupCoordinator::join_group`, the Raft election's `RequestVote` fan-out,
  `acks=all`'s `IsrManager::wait_for_hwm`) just block the current connection's
  own OS thread on a condition variable. Simple, correct, and fine at this
  scale because each connection already gets its own thread.
- **A background sweep thread, not a callback for everything.** `ClusterController`'s
  election timer/heartbeat loop and `IsrManager`'s ISR/HWM sweep both use one
  dedicated thread doing periodic work, rather than trying to react to every
  possible event. This is what lets "a peer went silent" be detected with zero
  active traffic.
- **A callback for "I need to react to my own decision."** Both `ClusterController`
  (`set_on_assignment_change`) and `IsrManager` (`set_on_isr_change`) have this
  same shape: a broadcast to *other* brokers never reaches the sender itself,
  so anything the sender decided that affects its own state needs an explicit
  callback into `Broker` to trigger `reconcile_replication()`.
- **Redirect via the existing response, not a new error path.** `NotLeaderForPartition`
  is reused for "wrong broker for a partition" *and* "wrong broker for a
  cluster-control op like `CreateTopic`" — no separate "NotController" error
  code exists; the response just carries whichever redirect target applies.
- **Every ambiguity the brief left open is resolved and commented at the point
  of the decision**, not silently guessed — search for `M3 —`, `M4 —`, `M5 —`,
  `M6 —` comments through the code for these (the murmur2 sign mask, the
  `FETCH` `maxBytes` truncation rule, `CreateTopic`/`IsrUpdate`'s unassigned
  wire numbers, `replicaMaxLagBytes` → message-count, the HWM-jump-on-promotion
  fix, and others).

---

## 7. The order this was actually built in (and why)

Building in this order matters: each milestone is fully working and testable
before the next one starts depending on it.

1. **M1 — Commit log** (`storage/`): `Record` → `Segment` → `Partition`, driven
   entirely by GoogleTest, no network at all. Get append/read/recovery
   bulletproof before anything else touches it.
2. **M2 — Networked single broker** (`protocol/`, `net/`, `broker/`, `cli/`):
   wrap M1 behind TCP. First live demo — produce in one terminal, consume in
   another, kill and restart the broker and watch the data survive.
3. **M3 — Consumer groups** (`group/`): topic creation, key-hash routing,
   `GroupCoordinator`. Independent of clustering — still one broker.
4. **M4 — Multi-broker cluster** (`cluster/`): static peer list, Raft-inspired
   election, controller-driven partition assignment, `NotLeaderForPartition`
   redirects. This is where "distributed" becomes literally true.
5. **M5 — Replication + ISR** (`replication/`): real data copying between
   brokers, `acks=all`, the high watermark becoming a real durability
   guarantee instead of a single-node stand-in.
6. **M6 — Failover + hardening**: the piece that actually connects M4's
   election and M5's ISR into automatic partition failover
   (`ClusterController::check_partition_failover`), a resilient producer
   retry loop, and the headline `test_kill_leader` integration test.

M7 (polish: throughput benchmark, README with architecture diagram,
`describe-topic`/`list-topics`, metrics) is the only milestone from
`AGENT_BRIEF.md` not yet built — it's non-functional polish, not core
mechanics.

---

## 8. Running the tests

```bash
ctest --test-dir build --output-on-failure          # everything, ~45s total
ctest --test-dir build -R test_partition             # just one binary
./build/tests/test_kill_leader                        # run the headline test directly
```

`test_kill_leader` is the slow one (~40s, dominated by 1000 sequential CLI
process spawns) — everything else finishes in under 3 seconds combined.
