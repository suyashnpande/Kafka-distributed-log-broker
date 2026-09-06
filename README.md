# MiniKafka

A distributed publish-subscribe message broker built from scratch in C++17 —
segmented commit log with crash recovery, consumer groups, a multi-broker
cluster with Raft-inspired leader election, ISR replication, and automatic
partition failover.

See `PROJECT_GUIDE.md` for full architecture details.

## Build

```bash
cmake -S . -B build
cmake --build build -j$(nproc)
```

## Run the demo (5 terminal panes)

Run this once, in any pane, before the rest:
```bash
rm -rf /tmp/mk-cluster && mkdir -p /tmp/mk-cluster/{1,2,3}
```

**Pane 1 — broker 1** (leave running)
```bash
PEERS="1@localhost:9101,2@localhost:9102,3@localhost:9103"
./build/mk-broker --id 1 --port 9101 --data-dir /tmp/mk-cluster/1 --peers $PEERS
```

**Pane 2 — broker 2** (leave running)
```bash
PEERS="1@localhost:9101,2@localhost:9102,3@localhost:9103"
./build/mk-broker --id 2 --port 9102 --data-dir /tmp/mk-cluster/2 --peers $PEERS
```

**Pane 3 — broker 3** (leave running)
```bash
PEERS="1@localhost:9101,2@localhost:9102,3@localhost:9103"
./build/mk-broker --id 3 --port 9103 --data-dir /tmp/mk-cluster/3 --peers $PEERS
```
Watch panes 1–3: one of them prints `became LEADER for term 1` — that's the elected controller.

**Pane 4 — admin + producer**
```bash
./build/mk-admin create-topic --broker localhost:9101 --topic orders \
    --partitions 4 --replication-factor 3

./build/mk-produce --broker localhost:9101 --topic orders --key k1 --value order1
./build/mk-produce --broker localhost:9101 --topic orders --key k2 --value order2
```

**Pane 5 — consumer** (leave running)
```bash
./build/mk-consume --broker localhost:9101 --topic orders --partition 0 --from-beginning
```
Records print here as they're produced. If nothing shows, try `--partition 1`, `2`, or `3` — the key decides which partition it landed on.

**Try killing a leader** (any partition's leader, from any pane): `Ctrl+C` a broker, then produce/consume again — the cluster automatically fails over and traffic keeps flowing.

## Cleanup

```bash
pkill -9 -x mk-broker
rm -rf /tmp/mk-cluster
```
