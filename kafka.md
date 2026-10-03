# Apache Kafka — Core Concepts

> Study notes. Hands-on practice is in [kafka-tutorial.md](kafka-tutorial.md).
> Version used in these notes: **Kafka 4.x** (KRaft mode, no ZooKeeper).

---

## 1. What is Kafka?

Apache Kafka is a **distributed event streaming platform**. In simple words, it is a
**log** (a list of messages that only grows at the end) that is:

- **distributed**: split across many servers
- **replicated**: every message is copied to more than one server, so you don't lose data
- **durable**: messages are saved on disk and kept for a set time (for example, 7 days), even after someone reads them

Kafka was created at **LinkedIn** (2011) and is now used by most big tech companies
(Netflix, Uber, Airbnb, and many banks) for:

| Use case | Example |
|---|---|
| Messaging between microservices | `order-service` publishes "OrderCreated", `payment-service` reacts |
| Activity tracking | clicks, page views, searches |
| Log and metrics aggregation | collect logs from 10,000 servers in one place |
| Stream processing | detect fraud in real time |
| Change Data Capture (CDC) | copy every database change to other systems |
| Event sourcing | the event log is the "source of truth" |

**Key idea:** Kafka does **not** delete a message when a consumer reads it. Many different
consumers can read the same data, at their own speed, and can even **replay** old data.

---

## 2. Main Building Blocks

### 2.1 Record (message / event)

The unit of data. Each record has:

| Field | Description |
|---|---|
| **Key** | Optional. Used to choose the partition. Same key → same partition → **order is kept** |
| **Value** | The content (JSON, Avro, Protobuf, bytes...) |
| **Headers** | Optional metadata (e.g., `trace-id`) |
| **Timestamp** | When it was created or written |
| **Offset** | Position of the record inside the partition (set by Kafka) |

### 2.2 Topic

A **named stream of records**, like a table name or a folder. Example: `orders`, `payments`.
Producers write to a topic; consumers read from a topic.

### 2.3 Partition

A topic is split into **partitions**. Each partition is an **ordered, append-only log**.

```
Topic "orders" (3 partitions)

Partition 0: [0][1][2][3][4][5]  ← new records are added at the end
Partition 1: [0][1][2][3]
Partition 2: [0][1][2][3][4]
              ↑
            offset
```

Why partitions?
- **Scalability**: partitions are spread across brokers, so many servers work in parallel.
- **Parallelism**: one consumer per partition inside a group (more partitions = more consumers).
- **Ordering**: Kafka guarantees order **only inside one partition**, not across the whole topic.

→ Full explanation in [§4 Partitions in Depth](#4-partitions-in-depth).

### 2.4 Offset

A number that identifies the position of a record in a partition (0, 1, 2...).
Consumers save ("commit") the last offset they processed, so they can continue
from there after a restart.

### 2.5 Segment

On disk, each partition is a folder with several **segment files** (e.g., 1 GB each).
Kafka deletes or compacts **whole old segments**, not single messages. This is one
reason Kafka is fast: it only does sequential disk writes.

```
/var/lib/kafka/data/orders-0/
  00000000000000000000.log        ← the records
  00000000000000000000.index      ← offset → file position
  00000000000000000000.timeindex  ← timestamp → offset
```

### 2.6 Broker

A **broker** is one Kafka server (one JVM process). It stores partitions and answers
requests from producers and consumers. Each broker has a unique `node.id`.

### 2.7 Cluster

A **cluster** is a group of brokers working together. Clients connect to any broker
(the "bootstrap server") and learn where every partition lives.

### 2.8 Controller and KRaft

Someone needs to manage the cluster: which brokers are alive, who is the leader of
each partition, which topics exist. This is the job of the **controller**.

- **Old way (before Kafka 4.0):** an external system, **ZooKeeper**, stored this metadata.
- **New way (KRaft, default since 3.3, only option since 4.0):** Kafka manages its own
  metadata using the **Raft consensus protocol**. A small group of **controller nodes**
  (usually 3 or 5) votes to choose an **active controller**. The metadata is saved in an
  internal topic called `__cluster_metadata`.

A node can have one of these roles (`process.roles`):

| Role | What it does |
|---|---|
| `broker` | Stores data and serves clients |
| `controller` | Manages metadata, takes part in the Raft vote |
| `broker,controller` | Both ("combined mode"). Good for **dev and study**, **not recommended for production** |

**Quorum rule:** the controllers need a **majority** to work.
3 controllers → can lose 1. 5 controllers → can lose 2.

---

## 3. Replication (how Kafka avoids data loss)

Each partition has a **replication factor (RF)**. With `RF=3`, there are 3 copies of
each partition, on 3 different brokers.

```
Partition orders-0, RF=3

Broker 1: [Leader]    ← producers write here, consumers read here
Broker 2: [Follower]  ← copies data from the leader
Broker 3: [Follower]  ← copies data from the leader
```

| Concept | Meaning |
|---|---|
| **Leader** | The replica that receives all writes (and normally reads) for the partition |
| **Follower** | A replica that copies the leader's data |
| **ISR (In-Sync Replicas)** | The replicas that are up to date with the leader (the leader included) |
| **High Watermark** | The last offset that was copied to all ISR. Consumers only see records up to this point |
| **`min.insync.replicas`** | The minimum ISR size needed to accept a write with `acks=all` |
| **Leader election** | If the leader broker dies, the controller chooses a new leader from the ISR |
| **Unclean leader election** | Choosing a leader that is **not** in the ISR. This can lose data, so it is disabled by default |
| **Rack awareness** | `broker.rack` = data center / Availability Zone. Kafka puts the replicas in **different racks** |

**The "golden" durability setting (used in production):**

```
replication.factor = 3
min.insync.replicas = 2
acks = all            (producer)
```

→ The cluster keeps accepting writes with 1 broker down, and **no acknowledged message is lost**.

---

## 4. Partitions in Depth

### 4.1 Mental model: checkout lanes in a supermarket

- The **topic** is the supermarket. It's just a name.
- Each **partition** is a **checkout lane**. Each lane has its own line, **in order**.
- More lanes = more customers served at the same time (**throughput**).
- But there is **no order between lanes**. You can't say who paid "first" in lane 1 vs lane 3.

A topic is a **logical** idea. The partition is the **real, physical** thing: it is the unit
that is stored on disk, replicated, given a leader, and assigned to a consumer.

### 4.2 Where partitions live

Example: topic `orders`, **6 partitions**, `RF=3`, **3 brokers**:

| Partition | Leader | Replicas (first = preferred leader) |
|---|---|---|
| orders-0 | Broker 1 | 1, 2, 3 |
| orders-1 | Broker 2 | 2, 3, 1 |
| orders-2 | Broker 3 | 3, 1, 2 |
| orders-3 | Broker 1 | 1, 3, 2 |
| orders-4 | Broker 2 | 2, 1, 3 |
| orders-5 | Broker 3 | 3, 2, 1 |

- `RF=3` means **each partition** has 3 copies on 3 **different** brokers (1 leader + 2 followers).
  6 partitions × 3 copies = **18 replicas**, which is 6 per broker. Here RF = number of brokers, so
  **every broker has a copy of every partition**. With 6 brokers, each partition would be on only 3 of them.
- Disk cost: 1 GB of data in the topic uses **3 GB** of disk in the cluster.
- RF can't be bigger than the number of brokers (`InvalidReplicationFactorException`).
- Each broker is **leader of 2** partitions and **follower of 4**. The work is shared evenly.
- Kafka spreads the leaders on purpose. If all the leaders were on Broker 1, it would do all the work.
- The **first replica** in the list is the **preferred leader**. After a failure and recovery,
  Kafka moves leadership back to it (`auto.leader.rebalance.enable=true`, by default).

### 4.3 How a record chooses its partition

```
                   ┌─ partition set explicitly?  → use it
 producer.send() ──┼─ has a key?                 → hash(key) % number_of_partitions
                   └─ no key                     → "sticky" partitioner: fill a batch
                                                   for one partition, then switch
```

**What is a hash?** A **hash function** takes data of any size (for example, the text `"user-1"`)
and returns a **number**. Two properties matter here:

1. **Deterministic:** the same input always gives the same number. `"user-1"` always becomes
   the same value, today, tomorrow, on any machine.
2. **Spreads well:** similar keys (`user-1`, `user-2`) give very different numbers, so the
   records are distributed across the partitions.

The Java client uses a hash function called **murmur2**. The formula is:

```
partition = toPositive(murmur2(key bytes)) % number_of_partitions
```

Step by step with key `"user-1"` and 3 partitions (real values):

```
"user-1"  → bytes          → u s e r - 1
          → murmur2        → 1404122828
          → toPositive     → 1404122828   (clears the sign bit if the number is negative)
          → % 3            → 2            → partition 2
```

The `%` (modulo) is the remainder of the division. With 3 partitions the result can only be
0, 1 or 2, so it turns a huge number into a valid partition number.

Example with 3 partitions (real murmur2 results):

```
key="user-1"  → hash % 3 = 2 → always partition 2
key="user-4"  → hash % 3 = 1 → always partition 1
key="user-8"  → hash % 3 = 0 → always partition 0
key="user-7"  → hash % 3 = 2 → partition 2 (keys can share a partition)
key=null      → any partition (balanced, but no order guarantee)
```

**Why it matters:** because the hash is deterministic, all records with key `"user-1"` go to the
same partition. Inside a partition Kafka keeps the order, so all `user-1` events are read in the
order they were sent (see 4.4).

> ⚠️ If you change the number of partitions (for example, 3 → 4), `% 4` gives a different
> result, and the same key can move to another partition. Ordering per key breaks during the
> change (see 4.9).

> ⚠️ Gotcha: different client libraries can use **different hash functions**. The Java client
> uses murmur2. librdkafka (used by Python `confluent-kafka`, Go, .NET) uses CRC32 by default.
> If two apps in different languages produce the same key, set `partitioner=murmur2_random`
> in librdkafka so that the key goes to the same partition.

### 4.4 Ordering: why the key matters

Events for order `#42`: `Created` → `Paid` → `Shipped`.

**With `key = orderId`** (correct):
```
Partition 1: [#42 Created] [#42 Paid] [#42 Shipped]   ← one consumer reads them in order ✅
```

**Without a key** (wrong for this case):
```
Partition 0: [#42 Paid]
Partition 1: [#42 Shipped]
Partition 2: [#42 Created]
→ 3 different consumers can process "Shipped" before "Created" ❌
```

**Rule:** use as the key the **entity whose events must stay in order** (orderId, userId,
accountId). Order is guaranteed **per key**, not for the whole topic.

(With `enable.idempotence=true`, order is kept even when the producer retries.
See [§5.5](#55-idempotent-producer-no-duplicates-no-reordering-on-retry).)

### 4.5 Each partition has its own offsets

Offsets are **per partition**, not per topic:

```
orders-0: offsets 0, 1, 2, 3 ...
orders-1: offsets 0, 1, 2 ...      ← offset 2 here has NO relation to offset 2 in orders-0
```

So a record is identified by **(topic, partition, offset)**, and a consumer group saves one
committed offset **per partition**.

### 4.6 Partitions and consumers (parallelism)

**Inside one consumer group, one partition is read by at most one consumer.**
Topic with **6 partitions**:

| Consumers in the group | What happens |
|---|---|
| 1 | One consumer reads all 6 partitions |
| 2 | 3 partitions each |
| 3 | 2 partitions each |
| 6 | 1 partition each → **maximum parallelism** |
| 8 | 6 work, **2 stay idle** ❌ |

→ **The number of partitions = the maximum number of consumers that can work in parallel** in a group.
This is the main reason to have more partitions.

### 4.7 On disk

Each partition replica is **one folder** on the broker: `<topic>-<partition>`.

```
/var/lib/kafka/data/
  orders-0/
    00000000000000000000.log    ← old segment (closed)
    00000000000000052371.log    ← active segment (receives new writes)
    ...index, .timeindex
  orders-3/
  orders-4/
```

- The file name = the **first offset** inside that segment.
- Writes only go to the **end** of the **active segment** (sequential I/O = fast).
- Retention deletes **whole closed segments** that are older than `retention.ms`.

### 4.8 Hot partitions (key skew)

If one key has much more traffic than the others (for example, a celebrity user on a social
network), **its partition gets overloaded**, while the others stay idle. More partitions don't help,
because one key always goes to one partition.

Fixes:
- Choose a key with more distinct values (e.g., `orderId` instead of `countryId`).
- **Salting**: `key = userId + "-" + random(0..9)` spreads the key over 10 partitions,
  but **you lose ordering** for that key.

### 4.9 Changing the number of partitions

```bash
kafka-topics.sh --bootstrap-server $BS --alter --topic orders --partitions 12
```

- You can **increase** partitions, but **never decrease** them.
- Increasing changes `hash(key) % N`. So `user-1` can move to another partition, and the
  **ordering for existing keys breaks** during the change.
- Existing data **does not move**. Only new records use the new mapping.

→ **Plan the partition count when you create the topic.**

### 4.10 How many partitions should I create?

A common formula:

```
partitions = max( T / P , T / C )

T = target throughput of the topic (MB/s)
P = throughput ONE partition can handle on the producer side (measure it)
C = throughput ONE consumer can process from one partition (measure it)
```

Example: you need 100 MB/s, and one consumer processes 10 MB/s → at least **10 partitions**.
Add space for growth, and choose a number that divides well (12 can be split across
1, 2, 3, 4, 6, or 12 consumers, and across 3 brokers).

**Too few partitions** → low parallelism, and you can't scale consumers.
**Too many partitions** → more open files and memory, slower failover (more leaders to
elect), more overhead for clients and replication. Keep it to a few thousand partitions per broker at most.

| Workload | Typical start |
|---|---|
| Small / study topic | 3–6 |
| Normal production service | 6–24 |
| Very high-volume topic (clicks, logs) | 48–200+ (based on measurements) |

### 4.11 Mini lab (on your study cluster)

```bash
kafka-topics.sh --bootstrap-server $BS --create --topic part.demo --partitions 3 --replication-factor 3

# Produce keyed messages
kafka-console-producer.sh --bootstrap-server $BS --topic part.demo \
  --property parse.key=true --property key.separator=:
>user-1:a
>user-2:b
>user-1:c
>user-3:d

# See partition + offset of each record
kafka-console-consumer.sh --bootstrap-server $BS --topic part.demo --from-beginning \
  --property print.key=true --property print.partition=true --property print.offset=true

# End offset of each partition (how many records each one has)
kafka-get-offsets.sh --bootstrap-server $BS --topic part.demo
```

Check: does `user-1` always appear in the same partition? Do the offsets start at 0 in **each** partition?

---

## 5. Producers

A **producer** is an application that writes records to a topic.

### 5.1 How a producer chooses the partition

1. If you set the partition explicitly → that partition.
2. If the record has a **key** → `hash(key) % number_of_partitions`. Same key → same partition.
3. No key → "sticky partitioner": it fills a batch for one partition, then moves to another.

> ⚠️ If you **add partitions** to a topic later, the key → partition mapping changes,
> so ordering per key can break. Plan the partition count from the start.

### 5.2 `acks` (acknowledgement)

| `acks` | The producer waits for... | Speed | Safety |
|---|---|---|---|
| `0` | nothing | fastest | can lose data |
| `1` | leader only | fast | loses data if the leader dies before the followers copy it |
| `all` (`-1`) | all ISR replicas | slower | safest (**default since Kafka 3.0**) |

### 5.3 Other important producer settings

| Setting | Purpose |
|---|---|
| `enable.idempotence=true` | No duplicates when the producer retries (Java client default since 3.0) |
| `linger.ms` | Wait a few ms to build bigger batches → higher throughput |
| `batch.size` | Max batch size in bytes per partition |
| `compression.type` | `lz4`, `zstd`, `snappy`, `gzip`. Less network and disk usage |
| `retries` / `delivery.timeout.ms` | How long the producer keeps retrying before it gives up |

### 5.4 Batches (how the producer groups messages)

A **batch** is a **group of records for the same partition** that the producer sends together,
in one network request, instead of sending one record at a time.

**Analogy:** a delivery company doesn't send one truck for each package. It waits until the
truck is full (or until it is time to leave), then delivers many packages in one trip.

```
Your code:  send(A)  send(B)  send(C)  send(D)  send(E)
                │       │        │        │        │
                ▼       ▼        ▼        ▼        ▼
Producer memory (one open batch per partition):
   orders-0: [A][C][E]   ← fills until batch.size (16 KB) OR linger.ms passes
   orders-1: [B][D]
                │
                ▼  one request to the broker carries the batches of all its partitions
            Broker (stores each batch on disk as it is)
```

**When is a batch sent?** When the **first** of these happens:
1. The batch is **full** → `batch.size` (default 16 KB).
2. The **wait time** is over → `linger.ms` (default 5 ms in Kafka 4.0+, 0 ms before that).

**Why batches?**
- **Fewer network requests**: 1 request with 500 records instead of 500 requests.
- **Better compression**: compression works on the whole batch, so similar records compress well.
- **Faster disk writes**: the broker writes the whole batch at once, and consumers also fetch whole batches.

**Trade-off:** a bigger `linger.ms` means bigger batches (**more throughput**), but each message
waits a bit longer (**more latency**). Big tech high-volume topics often use `linger.ms=5–20` and
`batch.size=64–256 KB`.

**"In flight"** = a batch that was **sent but not confirmed yet** (no ack). By default, up to **5**
batches can be in flight for each broker connection (`max.in.flight.requests.per.connection=5`).
This is what makes the retry problems in §5.5 possible.

### 5.5 Idempotent producer (no duplicates, no reordering on retry)

The producer sends **up to 5 batches at the same time** to the same partition, without
waiting for each reply (`max.in.flight.requests.per.connection=5`). When a batch fails and is
retried, two problems can happen:

**Problem 1 — Order breaks (without idempotence):**
```
send batch 1 [A]  ──►  ❌ fails (timeout)
send batch 2 [B]  ──►  ✅ written         log: [B]
retry batch 1 [A] ──►  ✅ written         log: [B][A]   ← A should come first!
```

**Problem 2 — Duplicate (without idempotence):**
```
send batch 1 [A]  ──►  ✅ written, but the ack is lost on the network
retry batch 1 [A] ──►  ✅ written again   log: [A][A]   ← duplicate!
```

**How idempotence fixes both:** the producer gets a **Producer ID (PID)**, and every batch
gets a **sequence number** for each partition. The broker remembers the last sequence it wrote:

```
broker expects seq=1, receives seq=2 → REJECT (out of order); producer resends 1, then 2
broker already wrote seq=1, receives seq=1 again → DISCARD the copy, but reply "OK"
```

Result: each message is written **exactly once** in the partition, **in the order it was sent**.

Limits:
- It only protects against the **producer's own automatic retries**, inside one producer session.
  If your **code** calls `send()` twice, or the producer restarts (new PID), you can still get
  duplicates. For that you need **transactions** (`transactional.id`) or a deduplicating consumer.
- It needs `acks=all` and `max.in.flight.requests.per.connection ≤ 5`.
- Default `true` in the **Java** client since Kafka 3.0, but **`false` in librdkafka**
  (Python `confluent-kafka`, Go, .NET). In those clients, set `enable.idempotence=true` yourself.
- In the Java client, if you set `acks=1` (or `0`), idempotence is **turned off**.

---

## 6. Consumers and Consumer Groups

A **consumer** reads records. Kafka uses a **pull** model: the consumer asks for data
(`poll()`); the broker does not push data to it.

### 6.1 Consumer group

Consumers with the same `group.id` form a **consumer group** and **share the work**:
**each partition is read by only one consumer in the group.**

```
Topic "orders" (4 partitions)

Group "payment-service":            Group "email-service":
  Consumer A ← P0, P1                 Consumer X ← P0, P1, P2, P3
  Consumer B ← P2, P3

Both groups receive ALL messages, independently.
```

Rules:
- More consumers than partitions → the extra consumers stay **idle**.
- **Different groups** = different applications. Each one gets a full copy of the stream.
- This gives you **queue** (inside a group) and **publish/subscribe** (across groups) in one system.

### 6.2 Offsets and commits

Consumers save their progress in the internal topic `__consumer_offsets`.

| Strategy | Risk |
|---|---|
| Commit **before** processing | **At-most-once**: if it crashes, the message is lost |
| Commit **after** processing | **At-least-once**: if it crashes, the message is processed again (duplicate). **Most common** |
| Transactions + `isolation.level=read_committed` | **Exactly-once** (read → process → write to Kafka) |

`auto.offset.reset` decides what to do when a group has no saved offset:
`earliest` (start from the beginning) or `latest` (only new messages).

### 6.3 Rebalance

When a consumer joins or leaves a group, partitions are **reassigned**. This is a "rebalance".
- Old protocol: "stop-the-world" (everyone stops for a moment).
- **Kafka 4.0+**: new consumer group protocol (KIP-848, `group.protocol=consumer`). It is incremental and much faster.

### 6.4 Consumer lag

**Lag** = last offset in the partition − last committed offset of the group.
High lag that keeps growing = the consumers can't keep up. This is **one of the most
important metrics** in production.

---

## 7. Data Retention

Kafka keeps data based on a policy, not based on reads.

| `cleanup.policy` | Behavior | Example |
|---|---|---|
| `delete` (default) | Delete old segments by time (`retention.ms`) or size (`retention.bytes`) | Logs, events (keep 7 days) |
| `compact` | Keep only the **latest value for each key** | Current state of a user profile, config |
| `delete,compact` | Both | |

**Tiered storage** (production-ready since Kafka 3.9): old segments go to cheap object
storage (like Amazon S3), and recent data stays on local disks. This allows long
retention at low cost.

---

## 8. Delivery Guarantees — Summary

| Guarantee | How |
|---|---|
| **At-most-once** | `acks=0`, or commit before processing |
| **At-least-once** | `acks=all` + retries + commit after processing (**default choice**) |
| **Exactly-once** | Idempotent producer + transactions + `read_committed` consumers (only inside Kafka) |

> In real systems, people usually use **at-least-once + idempotent consumers**
> (for example, save the `event_id` in the database and ignore duplicates).

---

## 9. Full Message Flow

```
 Producer                         Kafka Cluster                         Consumer Group
 --------                  ---------------------------                  --------------
 1. send(key="user-42",          Broker 1 (Leader P0)
    value="order...")  ───►  2. Leader appends to log  ──┐
                             3. Followers fetch the copy │
                                Broker 2 (Follower P0) ◄─┤
                                Broker 3 (Follower P0) ◄─┘
                             4. All ISR have it → high watermark moves
 5. ack received ◄──────────    (acks=all)
                                                          6. poll() ──► Consumer A
                                                          7. process the record
                                                          8. commit offset
                                                             (__consumer_offsets)
```

---

## 10. Kafka vs. Traditional Queues

| | Kafka | RabbitMQ / Amazon SQS |
|---|---|---|
| Model | Log (data stays after it is read) | Queue (message is removed after ack) |
| Replay old data | ✅ Yes (reset offsets) | ❌ No (normally) |
| Many independent readers | ✅ Yes (consumer groups) | Needs fan-out (exchanges / SNS) |
| Ordering | Per partition | Per queue (SQS FIFO) |
| Throughput | Very high (millions of msgs/s) | High |
| Per-message routing logic | Simple | Rich (RabbitMQ) |
| Best for | Event streaming, logs, CDC, analytics | Task queues, job processing |

---

## 11. Kafka Ecosystem

| Tool | What it does |
|---|---|
| **Kafka Connect** | Moves data between Kafka and other systems (databases, S3, Elasticsearch) without code |
| **Kafka Streams** | A Java library for stream processing (filter, join, aggregate) |
| **Apache Flink** | A powerful stream processing engine, often used with Kafka |
| **Schema Registry** | Stores message schemas (Avro / Protobuf / JSON Schema) and checks compatibility |
| **MirrorMaker 2** | Copies data between clusters (disaster recovery, multi-region) |
| **Cruise Control** | (from LinkedIn) Rebalances partitions across brokers automatically |

---

## 12. Kafka on AWS — Options

| Option | You manage | Cost on your Free plan |
|---|---|---|
| **Self-managed on EC2** | Everything (install, config, upgrades) | Uses Free Tier eligible instances (paid by your credits) ← **our tutorial** |
| **Amazon MSK Provisioned (Standard)** | Broker size, storage, configs | ❌ Not free. Charged per broker-hour |
| **Amazon MSK Provisioned (Express)** | Broker size only (storage is managed) | ❌ Not free |
| **Amazon MSK Serverless** | Almost nothing | ❌ Not free |
| **Amazon Kinesis Data Streams** | AWS-native alternative (not Kafka) | ❌ Not free |

We use **EC2** because (1) it fits your free tier rule and (2) you learn more when you
build everything by hand.

---

## 13. Default Ports

| Port | Use |
|---|---|
| `9092` | Clients and brokers (PLAINTEXT listener) |
| `9093` | KRaft controllers (CONTROLLER listener) |
| `9094` / `9096` / `9098` | Common for TLS / SASL listeners (in MSK, for example) |

---

## 14. Common Interview Questions (quick answers)

1. **How does Kafka guarantee order?** Only inside a partition. Use the same key for related events.
2. **How do you avoid data loss?** `RF=3`, `min.insync.replicas=2`, `acks=all`, `unclean.leader.election.enable=false`, replicas in different AZs.
3. **What happens if a broker dies?** The controller elects a new leader from the ISR for each of its partitions. Clients refresh their metadata and continue.
4. **How do you scale consumers?** Add consumers to the group, up to the number of partitions.
5. **What is consumer lag and why does it matter?** How far behind the consumers are. Growing lag = processing is too slow → add consumers/partitions or optimize the code.
6. **How do you handle duplicates?** Idempotent producer + idempotent consumer (dedupe by event ID), or transactions.
7. **What is the difference between ZooKeeper and KRaft?** KRaft keeps metadata inside Kafka with Raft. It is simpler to operate, has faster failover, and supports more partitions. ZooKeeper was removed in 4.0.
8. **How many partitions should a topic have?** Target throughput ÷ throughput per partition (measure it!). Also think about the maximum number of consumers. Don't create thousands of partitions "just in case."
9. **Kafka vs SQS?** Kafka = replayable log with many readers and ordering per partition. SQS = simple managed queue where the message is deleted after it is processed.

---

## 15. Glossary (EN → PT)

| English | Português |
|---|---|
| append-only log | log somente de acréscimo |
| broker | servidor Kafka |
| replica / replication factor | réplica / fator de replicação |
| in-sync replicas | réplicas sincronizadas |
| consumer lag | atraso do consumidor |
| rebalance | redistribuição (de partições) |
| retention | retenção |
| throughput | vazão |
| hash function | função hash (transforma um dado em um número fixo) |
| modulo (`%`) | módulo (resto da divisão) |
| durability | durabilidade |
| quorum | quórum (maioria para decidir) |
