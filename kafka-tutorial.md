# Tutorial — Build a Kafka Cluster by Hand on AWS (Free Tier)

> Read [kafka.md](kafka.md) first. It explains the concepts used here.
>
> **Goal:** build a 3-node Apache Kafka cluster **manually** on Amazon EC2, the same way big
> tech companies design production clusters, but small enough for study and inside the Free Tier.

---

## Contents

- [Part 0 — Before You Start (cost and prerequisites)](#part-0--before-you-start)
- [Part 1 — How Big Tech Companies Run Kafka](#part-1--how-big-tech-companies-run-kafka)
- [Part 2 — Our Study Cluster Design](#part-2--our-study-cluster-design)
- [Part 3 — Create the AWS Infrastructure](#part-3--create-the-aws-infrastructure)
- [Part 4 — Install and Configure Kafka on Each Node](#part-4--install-and-configure-kafka-on-each-node)
- [Part 5 — Hands-on Labs](#part-5--hands-on-labs)
- [Part 6 — Stop or Delete Everything](#part-6--stop-or-delete-everything)
- [Part 7 — Troubleshooting](#part-7--troubleshooting)
- [Appendix — 100% Free Local Version (Docker)](#appendix--100-free-local-version-docker)

---

## Part 0 — Before You Start

### 0.1 Your project

| Item | Value |
|---|---|
| Plan | **Free plan** (AWS cannot charge your card) |
| Free Tier credits | $100 (they expire on 2027-04-03) |
| Selected Region | `us-east-2` (Ohio). Confirm it in AWS Settings > View all projects > Overview > Additional Info > Region |
| CLI profile | `vinicius` |

### 0.2 What costs what (approximate, us-east-2)

On the Free plan, EC2 usage is **paid by your Free Tier credits**, not by your card.
We only use **Free Tier eligible** resources.

| Resource | Quantity | Approx. price | While running |
|---|---|---|---|
| EC2 `t3.small` (2 vCPU, 2 GB RAM, Free Tier eligible) | 3 | $0.0208/hour each | $0.062/h |
| Public IPv4 address (needed for SSH) | 3 | $0.005/hour each | $0.015/h |
| EBS gp3 disk, 10 GB | 3 | $0.08/GB-month | ~$0.003/h |
| Data transfer between AZs | small | $0.01/GB each direction | cents |
| **Total** | | | **≈ $0.08 per hour (≈ $0.80 for 10 hours)** |

Rules to keep costs at **zero real money and low credit usage**:

1. **Stop** the instances when you finish a study session (Part 6). When they are stopped, you only pay for the disks (≈ $2.40/month for 30 GB).
2. **Terminate** everything when you finish studying Kafka.
3. We set `CpuCredits=standard`. `t3` instances use "unlimited" mode by default, which can **charge extra** when the CPU stays high for a long time. "Standard" mode only slows the CPU down instead.
4. ❌ **Do not use Amazon MSK** for this study. It is not part of the Free Tier.
5. You can check your credit balance in the **AWS Billing and Cost Management console > Free Tier**.

### 0.3 Prerequisites on your computer

- AWS CLI v2, logged in:
  ```bash
  aws login --profile vinicius
  export AWS_PROFILE=vinicius
  export AWS_REGION=us-east-2
  aws sts get-caller-identity     # check that the login works
  ```
- An SSH client (`ssh`), which Linux already has.

---

## Part 1 — How Big Tech Companies Run Kafka

This is what a **production** Kafka platform usually looks like at companies like
LinkedIn, Uber, Netflix, or a big bank.

```
                       Region (e.g., us-east-2)
 ┌──────────────────────────────────────────────────────────────────────┐
 │   AZ a                     AZ b                     AZ c             │
 │ ┌──────────────┐        ┌──────────────┐        ┌──────────────┐     │
 │ │ Controller 1 │        │ Controller 2 │        │ Controller 3 │     │  ← dedicated KRaft
 │ └──────────────┘        └──────────────┘        └──────────────┘     │    controllers (3 or 5)
 │ ┌──────────────┐        ┌──────────────┐        ┌──────────────┐     │
 │ │ Broker 1..N  │        │ Broker 1..N  │        │ Broker 1..N  │     │  ← many brokers,
 │ │ rack=a       │        │ rack=b       │        │ rack=c       │     │    same number per AZ
 │ └──────────────┘        └──────────────┘        └──────────────┘     │
 │        ▲ TLS + auth (mTLS / SASL / OAuth) + ACLs                     │
 └────────┼─────────────────────────────────────────────────────────────┘
          │
   Producers / Consumers ── Schema Registry ── Monitoring (Prometheus/Grafana, lag alerts)
          │
   MirrorMaker 2 ──► DR cluster in another Region
```

### 1.1 Best practices — Architecture

| # | Practice | Why |
|---|---|---|
| 1 | **Spread brokers across 3 Availability Zones** and set `broker.rack` = AZ | An entire AZ can fail without data loss. Kafka puts the replicas in different AZs |
| 2 | **Dedicated controllers** (3 or 5 nodes, `process.roles=controller`) | Heavy broker load can't slow down cluster management. Easier to operate |
| 3 | **Several clusters, separated by purpose** (critical payments, logs, analytics) | Limits the "blast radius" of a failure. No noisy neighbors |
| 4 | **Disaster recovery**: replicate to another Region with MirrorMaker 2 | Survives the loss of a whole Region |
| 5 | **Infrastructure as Code** (Terraform, Ansible, Kubernetes + Strimzi) | Clusters are repeatable. Nothing is created by hand in production |
| 6 | **Tiered storage** for long retention | Old data goes to S3-like storage, which is much cheaper than broker disks |

### 1.2 Best practices — Durability and topics

| # | Practice | Why |
|---|---|---|
| 7 | `default.replication.factor=3`, `min.insync.replicas=2` | Survives 1 broker down without losing writes |
| 8 | Producers use `acks=all` + `enable.idempotence=true` | No data loss, no duplicates on retry |
| 9 | `unclean.leader.election.enable=false` | Never elect an out-of-date replica (that would lose data) |
| 10 | `auto.create.topics.enable=false` | Topics are created through a review process or as code, not by a typo in a client |
| 11 | **Topic naming convention**, e.g. `<domain>.<entity>.<event>.v1` (`sales.order.created.v1`) | Ownership is clear, and a self-service platform is possible |
| 12 | **Plan the partition count** (target throughput ÷ throughput per partition) | Adding partitions later breaks the key ordering |
| 13 | **Schema Registry** with compatibility rules (Avro/Protobuf) | Producers can't break consumers ("data contracts") |

### 1.3 Best practices — Operations

| # | Practice | Why |
|---|---|---|
| 14 | **Monitor**: under-replicated partitions, offline partitions, active controller count, ISR shrinks, request latency, disk usage, **consumer lag** | These are the main signals of a problem. LinkedIn even built a tool only for lag (Burrow) |
| 15 | **Rolling restarts/upgrades**: one broker at a time, wait until under-replicated partitions = 0 | Zero downtime |
| 16 | **Cruise Control** to rebalance partitions | Keeps the load even when brokers are added or removed |
| 17 | **Client quotas** (bytes/sec per client) | One bad client can't take down the cluster |
| 18 | **Follower fetching** (`client.rack` + `RackAwareReplicaSelector`) | Consumers read from a replica in their own AZ → much lower cross-AZ network cost |
| 19 | **Hardware/OS tuning**: JVM heap ~6 GB and the rest of the RAM for the OS page cache, XFS, `vm.swappiness=1`, high file-descriptor limit | Kafka depends on the page cache for speed |
| 20 | **Security**: TLS in transit, authentication, ACLs (least privilege), encryption at rest, private subnets only | Kafka often carries sensitive data |

### 1.4 Best practices — Clients

| # | Practice |
|---|---|
| 21 | Producers: `linger.ms` 5–20 + `compression.type=lz4` or `zstd` for throughput |
| 22 | Consumers: commit **after** processing (at-least-once) + **idempotent processing** (dedupe by event ID) |
| 23 | Use a **dead-letter topic (DLQ)** for messages that keep failing, so they don't block the partition |
| 24 | Use the new consumer group protocol (`group.protocol=consumer`, Kafka 4.0+) for fast rebalances |
| 25 | Always use **several** bootstrap servers, never just one |

---

## Part 2 — Our Study Cluster Design

We copy the **most important ideas** from Part 1, but at a small scale.

```
                 Default VPC — us-east-2
 ┌──────────────────────────────────────────────────────────────┐
 │  us-east-2a            us-east-2b            us-east-2c      │
 │ ┌──────────────┐     ┌──────────────┐     ┌──────────────┐   │
 │ │ kafka-1      │     │ kafka-2      │     │ kafka-3      │   │
 │ │ t3.small     │◄───►│ t3.small     │◄───►│ t3.small     │   │
 │ │ broker +     │     │ broker +     │     │ broker +     │   │
 │ │ controller   │     │ controller   │     │ controller   │   │
 │ │ node.id=1    │     │ node.id=2    │     │ node.id=3    │   │
 │ │ rack=2a      │     │ rack=2b      │     │ rack=2c      │   │
 │ └──────▲───────┘     └──────▲───────┘     └──────▲───────┘   │
 │        └── ports 9092/9093 only between the nodes (SG) ──┘   │
 └────────┼─────────────────────────────────────────────────────┘
          │ SSH (port 22) only from YOUR IP
       Your laptop
```

| Big tech practice | In our study cluster |
|---|---|
| 3 AZs + rack awareness | ✅ 1 node per AZ, `broker.rack` = AZ |
| Dedicated controllers | ⚠️ **Combined mode** (each node is broker + controller), to save cost. The concept is the same: a quorum of 3 |
| RF=3, min ISR=2, acks=all | ✅ Same |
| Unclean election off, auto-create off | ✅ Same |
| Follower fetching | ✅ Configured (Lab 7) |
| Network isolation | ✅ Security group: Kafka ports only between the nodes, SSH only from your IP |
| TLS / SASL / ACLs | ❌ Skipped (PLAINTEXT) to keep it simple. Optional next step |
| Service manager | ✅ `systemd` service with auto restart |
| Monitoring | ⚠️ Kafka CLI tools (no Prometheus/Grafana) |
| Infrastructure as Code | ❌ Manual on purpose, so you learn every step. Next step: script it |

> **Kafka version:** this tutorial uses **Apache Kafka 4.1.0**. The commands are written for that
> version. Newer 4.x versions should work too.

---

## Part 3 — Create the AWS Infrastructure

Run these commands **on your laptop**. All of them use the default VPC (free) in `us-east-2`.

### Step 1 — Set variables

```bash
export AWS_PROFILE=vinicius
export AWS_REGION=us-east-2

VPC_ID=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true \
  --query 'Vpcs[0].VpcId' --output text)
MY_IP=$(curl -s https://checkip.amazonaws.com)
echo "VPC=$VPC_ID  MY_IP=$MY_IP"
```

### Step 2 — Create an SSH key pair

```bash
aws ec2 create-key-pair --key-name kafka-study --key-type ed25519 \
  --query KeyMaterial --output text > ~/.ssh/kafka-study.pem
chmod 400 ~/.ssh/kafka-study.pem
```

### Step 3 — Create the security group (the firewall)

```bash
SG_ID=$(aws ec2 create-security-group --group-name kafka-study-sg \
  --description "Kafka study cluster" --vpc-id "$VPC_ID" \
  --query GroupId --output text)

# SSH only from your IP
aws ec2 authorize-security-group-ingress --group-id "$SG_ID" \
  --protocol tcp --port 22 --cidr "$MY_IP/32"

# Kafka ports (9092 client/broker, 9093 controller) ONLY between members of this group
aws ec2 authorize-security-group-ingress --group-id "$SG_ID" \
  --protocol tcp --port 9092-9093 --source-group "$SG_ID"

echo "SG=$SG_ID"
```

> 🔒 **Never** open 9092/9093 to `0.0.0.0/0`. In this tutorial Kafka has no authentication,
> so anyone on the internet could read and write your data.

### Step 4 — Launch 3 instances, one per AZ

```bash
N=1
for AZ in a b c; do
  SUBNET=$(aws ec2 describe-subnets \
    --filters Name=vpc-id,Values="$VPC_ID" Name=availability-zone,Values=us-east-2$AZ Name=default-for-az,Values=true \
    --query 'Subnets[0].SubnetId' --output text)

  aws ec2 run-instances \
    --image-id resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
    --instance-type t3.small \
    --credit-specification CpuCredits=standard \
    --key-name kafka-study \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET" \
    --associate-public-ip-address \
    --metadata-options HttpTokens=required \
    --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":10,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=kafka-$N},{Key=Project,Value=kafka-study}]" \
    --query 'Instances[0].InstanceId' --output text

  N=$((N+1))
done
```

What each option does:

| Option | Why |
|---|---|
| `resolve:ssm:...al2023...` | Uses the latest Amazon Linux 2023 image |
| `t3.small` | Free Tier eligible, 2 GB RAM (Kafka needs memory for the JVM and the page cache) |
| `CpuCredits=standard` | No extra charges for CPU bursts |
| `HttpTokens=required` | IMDSv2 only (a security best practice) |
| `DeleteOnTermination=true` | The disk is deleted with the instance, so you don't pay for forgotten disks |
| Tag `Project=kafka-study` | Makes it easy to find and delete everything later |

### Step 5 — Get the IP addresses

```bash
aws ec2 wait instance-running --filters Name=tag:Project,Values=kafka-study

aws ec2 describe-instances \
  --filters Name=tag:Project,Values=kafka-study Name=instance-state-name,Values=running \
  --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,Placement.AvailabilityZone,PrivateIpAddress,PublicIpAddress]' \
  --output table
```

**Write down the values.** You need them in Part 4:

| Node | AZ | Private IP (Kafka uses this) | Public IP (SSH uses this) |
|---|---|---|---|
| kafka-1 | us-east-2a | `IP1 = ...` | ... |
| kafka-2 | us-east-2b | `IP2 = ...` | ... |
| kafka-3 | us-east-2c | `IP3 = ...` | ... |

> The **private IP stays the same** when you stop and start an instance. The **public IP changes**.
> That's why Kafka uses the private IPs.

---

## Part 4 — Install and Configure Kafka on Each Node

Open **3 terminals**, one SSH session per node:

```bash
ssh -i ~/.ssh/kafka-study.pem ec2-user@<PUBLIC_IP_OF_NODE>
```

Do **Steps 6 to 9 on all 3 nodes**. Only `NODE_ID` changes.

### Step 6 — Set variables on the node

```bash
# >>> CHANGE THESE <<<
export NODE_ID=1                 # 1 on kafka-1, 2 on kafka-2, 3 on kafka-3
export IP1=10.0.x.x              # private IP of kafka-1
export IP2=10.0.x.x              # private IP of kafka-2
export IP3=10.0.x.x              # private IP of kafka-3

# Automatic
export MY_IP=$(hostname -I | awk '{print $1}')
TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
export RACK=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/availability-zone)
echo "node=$NODE_ID ip=$MY_IP rack=$RACK"
```

### Step 7 — Install Java and Kafka

```bash
# Kafka 4.x brokers need Java 17+
sudo dnf install -y java-17-amazon-corretto-headless

KAFKA_VERSION=4.1.0
cd /tmp
curl -fLO https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz
sudo tar -xzf kafka_2.13-${KAFKA_VERSION}.tgz -C /opt
sudo ln -sfn /opt/kafka_2.13-${KAFKA_VERSION} /opt/kafka   # symlink → easy upgrades later

# Dedicated user with no login (best practice: don't run Kafka as root)
sudo useradd --system --no-create-home --shell /sbin/nologin kafka
sudo mkdir -p /var/lib/kafka/data /var/log/kafka /etc/kafka
sudo chown -R kafka:kafka /var/lib/kafka /var/log/kafka

# Shortcuts for later
cat >> ~/.bashrc <<EOF
export PATH=\$PATH:/opt/kafka/bin
export BS=$IP1:9092,$IP2:9092,$IP3:9092
export IP1=$IP1 IP2=$IP2 IP3=$IP3 RACK=$RACK
EOF
source ~/.bashrc
```

### Step 8 — Write the configuration file

```bash
sudo tee /etc/kafka/server.properties > /dev/null <<EOF
############ Identity and roles (KRaft) ############
process.roles=broker,controller
node.id=${NODE_ID}
controller.quorum.voters=1@${IP1}:9093,2@${IP2}:9093,3@${IP3}:9093

############ Network ############
listeners=PLAINTEXT://${MY_IP}:9092,CONTROLLER://${MY_IP}:9093
advertised.listeners=PLAINTEXT://${MY_IP}:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT

############ Rack awareness (1 rack = 1 AZ) ############
broker.rack=${RACK}
replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector

############ Storage ############
log.dirs=/var/lib/kafka/data
log.retention.hours=24
log.segment.bytes=104857600

############ Durability (big tech defaults) ############
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
unclean.leader.election.enable=false
auto.create.topics.enable=false
EOF
```

What the less obvious settings do:

| Setting | Meaning |
|---|---|
| `controller.quorum.voters` | The 3 controllers that vote (Raft). Format: `id@host:port` |
| `listeners` | Where this node **listens** |
| `advertised.listeners` | The address that this node **tells clients** to use. A wrong value here is the #1 Kafka setup bug |
| `broker.rack` | The AZ. Kafka spreads the replicas across racks |
| `replica.selector.class` | Lets consumers read from a follower in their own AZ (follower fetching) |
| `log.retention.hours=24` | Keeps data for 1 day only (small disks) |
| `log.segment.bytes=100MB` | Small segments, so you can see the files roll over in the labs |

### Step 9 — Format the storage

Kafka needs one **cluster ID**, and it must be the **same on all nodes**.

**On kafka-1 only**, generate it:
```bash
/opt/kafka/bin/kafka-storage.sh random-uuid
# example output: q1Sh-9_ISia_zwGINzRvyQ   ← copy it
```

**On all 3 nodes**, format with that same ID:
```bash
export CLUSTER_ID=<paste-the-same-id-here>
sudo -u kafka /opt/kafka/bin/kafka-storage.sh format \
  -t "$CLUSTER_ID" -c /etc/kafka/server.properties
# Expected: "Formatting metadata directory /var/lib/kafka/data with metadata.version ..."
```

### Step 10 — Create a systemd service (on all 3 nodes)

```bash
sudo tee /etc/systemd/system/kafka.service > /dev/null <<'EOF'
[Unit]
Description=Apache Kafka (KRaft mode)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="KAFKA_HEAP_OPTS=-Xms512m -Xmx512m"
Environment="LOG_DIR=/var/log/kafka"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now kafka
```

> Heap = 512 MB on purpose. On a 2 GB machine, the rest of the memory is used by the OS
> **page cache**, which Kafka depends on. In production: ~6 GB heap on machines with 32–64 GB of RAM.
>
> Start the 3 nodes within about 1 minute of each other. The controllers need a **majority (2 of 3)**
> before they can elect a leader.

### Step 11 — Check the cluster

```bash
sudo systemctl status kafka --no-pager
sudo tail -n 50 /var/log/kafka/server.log

# Raft quorum: who is the active controller (LeaderId)? Are all voters there?
kafka-metadata-quorum.sh --bootstrap-server $BS describe --status
kafka-metadata-quorum.sh --bootstrap-server $BS describe --replication

# Brokers visible to clients
kafka-broker-api-versions.sh --bootstrap-server $BS | grep -E '^[0-9.]+:9092'
```

✅ You should see 3 voters, a `LeaderId`, and 3 brokers. **Your cluster is running!**

---

## Part 5 — Hands-on Labs

Run them from any node (the `$BS` variable has all 3 brokers).

### Lab 1 — Create a topic and see where the replicas are

```bash
kafka-topics.sh --bootstrap-server $BS --create \
  --topic sales.order.created.v1 --partitions 6 --replication-factor 3

kafka-topics.sh --bootstrap-server $BS --describe --topic sales.order.created.v1
```

Look at each line:
- `Leader` → the broker that receives the writes for that partition
- `Replicas` → where the copies are (one per AZ, thanks to `broker.rack`)
- `Isr` → the copies that are up to date

❓ **Question:** are the leaders spread evenly across brokers 1, 2, and 3? Why does that matter?

### Lab 2 — Produce and consume with keys (ordering)

**Terminal A (node 1) — consumer:**
```bash
kafka-console-consumer.sh --bootstrap-server $BS --topic sales.order.created.v1 \
  --group order-service --property print.key=true --property print.partition=true
```

**Terminal B (node 2) — producer** (type `key:value`):
```bash
kafka-console-producer.sh --bootstrap-server $BS --topic sales.order.created.v1 \
  --producer-property acks=all \
  --property parse.key=true --property key.separator=:
>user-1:order 100
>user-2:order 200
>user-1:order 101
>user-1:order 102
```

✅ Every `user-1` message goes to the **same partition** → the order is kept for that user.

### Lab 3 — Consumer groups and lag

1. Open a **second consumer** with the same `--group order-service` (Terminal C, node 3). Produce more messages. Each consumer now gets **only some of the partitions**.
2. Check the group:
   ```bash
   kafka-consumer-groups.sh --bootstrap-server $BS --describe --group order-service
   ```
   Columns to understand: `PARTITION`, `CURRENT-OFFSET`, `LOG-END-OFFSET`, `LAG`, `CONSUMER-ID`.
3. Stop **both** consumers (Ctrl+C), produce 10 more messages, and describe the group again → `LAG` = 10.
4. **Replay** everything from the beginning (the group must be stopped first):
   ```bash
   kafka-consumer-groups.sh --bootstrap-server $BS --group order-service \
     --topic sales.order.created.v1 --reset-offsets --to-earliest --execute
   ```
   Start a consumer again → you get **all** the old messages again. A normal queue can't do this.

### Lab 4 — A broker dies (failover)

**On kafka-2:**
```bash
sudo systemctl stop kafka
```

**On kafka-1:**
```bash
kafka-topics.sh --bootstrap-server $BS --describe --topic sales.order.created.v1
kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions
```

Observe:
- Broker 2 is **gone from every `Isr`**.
- The partitions that broker 2 led now have a **new leader**.
- Producing with `acks=all` **still works**, because ISR = 2 ≥ `min.insync.replicas` = 2.

Start it again and watch it come back into the ISR:
```bash
sudo systemctl start kafka        # on kafka-2
```
After a few minutes, Kafka moves leadership back to the "preferred" leader. To do it right now:
```bash
kafka-leader-election.sh --bootstrap-server $BS --election-type PREFERRED --all-topic-partitions
```

### Lab 5 — `min.insync.replicas` protects your data

```bash
kafka-topics.sh --bootstrap-server $BS --create --topic payments.strict.v1 \
  --partitions 1 --replication-factor 3 --config min.insync.replicas=3
```

Stop kafka-3 (`sudo systemctl stop kafka`), then produce:
```bash
kafka-console-producer.sh --bootstrap-server $BS --topic payments.strict.v1 \
  --producer-property acks=all \
  --producer-property request.timeout.ms=5000 \
  --producer-property delivery.timeout.ms=10000
>test
```
✅ You get a `NOT_ENOUGH_REPLICAS` error. Kafka **refuses** the write instead of risking data
loss. Start kafka-3 again.

> This is why production uses `RF=3` + `min.insync.replicas=2`: it tolerates **1** failure.

### Lab 6 — Lose the quorum (why you need a majority)

Stop **kafka-2 and kafka-3**. On kafka-1:
```bash
kafka-metadata-quorum.sh --bootstrap-server $IP1:9092 describe --status
```
The cluster **stops working**: only 1 of 3 controllers is alive, and that is not a majority.
Start both again → the cluster recovers by itself.

> **Lesson:** this is also why big techs use **dedicated controllers**. In combined mode, losing
> brokers also means losing controllers.

### Lab 7 — Follower fetching (saves cross-AZ cost)

Run a consumer on kafka-1 (AZ `us-east-2a`) that tells Kafka its "rack":
```bash
kafka-console-consumer.sh --bootstrap-server $BS --topic sales.order.created.v1 \
  --from-beginning --consumer-property client.rack=$RACK
```
Now the consumer reads from the replica **in its own AZ**, even when the leader is in another AZ.
At big tech scale, this saves a lot of money, because AWS charges for traffic between AZs.

### Lab 8 — Performance test (acks and batching)

```bash
kafka-topics.sh --bootstrap-server $BS --create --topic perf.test \
  --partitions 6 --replication-factor 3

# Safe: acks=all
kafka-producer-perf-test.sh --topic perf.test --num-records 100000 --record-size 1000 \
  --throughput -1 --producer-props bootstrap.servers=$BS acks=all

# Fast but less safe: acks=1
kafka-producer-perf-test.sh --topic perf.test --num-records 100000 --record-size 1000 \
  --throughput -1 --producer-props bootstrap.servers=$BS acks=1

# Big tech tuning: batching + compression
kafka-producer-perf-test.sh --topic perf.test --num-records 100000 --record-size 1000 \
  --throughput -1 --producer-props bootstrap.servers=$BS acks=all linger.ms=20 batch.size=131072 compression.type=lz4
```
Compare `records/sec` and `avg latency`. (Each test sends ~100 MB, so keep the tests small: the disks are only 10 GB.)

### Lab 9 — Look at the files on disk

```bash
sudo ls -lh /var/lib/kafka/data/perf.test-0/
sudo -u kafka /opt/kafka/bin/kafka-dump-log.sh --print-data-log \
  --files /var/lib/kafka/data/sales.order.created.v1-0/00000000000000000000.log | head -20
```
You will see the `.log`, `.index`, and `.timeindex` segment files from [kafka.md §2.5](kafka.md#25-segment).

### Lab 10 — Rolling restart (how upgrades are done in production)

For each node, **one at a time**:
1. `sudo systemctl restart kafka`
2. Wait until this command prints **nothing**:
   `kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions`
3. Go to the next node.

Producers and consumers keep working the whole time → **zero downtime**.

---

## Part 6 — Stop or Delete Everything

Run these **on your laptop**.

### Stop (pause between study sessions)

```bash
export AWS_PROFILE=vinicius AWS_REGION=us-east-2
IDS=$(aws ec2 describe-instances --filters Name=tag:Project,Values=kafka-study \
  Name=instance-state-name,Values=running --query 'Reservations[].Instances[].InstanceId' --output text)
aws ec2 stop-instances --instance-ids $IDS
```
When you start them again (`aws ec2 start-instances --instance-ids $IDS`), Kafka starts
automatically (systemd), and the private IPs are the same. Only the public IPs change (use Step 5 to see them).

### Delete everything (when you finish studying)

```bash
IDS=$(aws ec2 describe-instances --filters Name=tag:Project,Values=kafka-study \
  Name=instance-state-name,Values=running,stopped --query 'Reservations[].Instances[].InstanceId' --output text)
aws ec2 terminate-instances --instance-ids $IDS
aws ec2 wait instance-terminated --instance-ids $IDS

SG_ID=$(aws ec2 describe-security-groups --filters Name=group-name,Values=kafka-study-sg \
  --query 'SecurityGroups[0].GroupId' --output text)
aws ec2 delete-security-group --group-id "$SG_ID"
aws ec2 delete-key-pair --key-name kafka-study
rm -f ~/.ssh/kafka-study.pem
```

Final check (all should be empty):
```bash
aws ec2 describe-instances --filters Name=tag:Project,Values=kafka-study \
  Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId'
aws ec2 describe-volumes --filters Name=status,Values=available --query 'Volumes[].VolumeId'
```

---

## Part 7 — Troubleshooting

| Problem | Likely cause / fix |
|---|---|
| `kafka.service` keeps restarting | Run `sudo journalctl -u kafka -n 100` and check `/var/log/kafka/server.log` |
| `Invalid cluster.id` | The nodes were formatted with **different** cluster IDs. Stop Kafka, delete `/var/lib/kafka/data/*`, and format again with the same ID |
| `No meta.properties found` | You skipped Step 9 (format) |
| Clients hang / `Connection to node -1 could not be established` | Wrong `advertised.listeners`, or the security group doesn't allow 9092 between the nodes |
| `describe --status` hangs | No quorum: fewer than 2 controllers are running, or port 9093 is blocked |
| `TopicExistsException` | The topic already exists, so use `--describe` |
| `UNKNOWN_TOPIC_OR_PARTITION` when producing | `auto.create.topics.enable=false` → create the topic first (this is on purpose!) |
| Out of memory / instance is very slow | Lower the heap (`-Xmx384m`), or check that no other process is using RAM (`free -m`) |
| SSH timeout | Your home IP changed → update the port 22 rule with your new `MY_IP` |

---

## Next Steps (after you finish the labs)

1. **Dedicated controllers**: rebuild with 3 small controller-only nodes + 3 broker-only nodes (`t3.micro` for the controllers).
2. **Security**: add a `SASL_PLAINTEXT` listener with SCRAM users, then ACLs (`kafka-acls.sh`).
3. **Write a real producer/consumer** in Python (`confluent-kafka`) or Java, using the client best practices from Part 1.4.
4. **Automate** the whole tutorial with a shell script or Terraform (Infrastructure as Code).
5. **Monitoring**: export JMX metrics with the Prometheus JMX exporter and watch under-replicated partitions and lag.

---

## Appendix — 100% Free Local Version (Docker)

If you want to practice **without using any AWS credits**, run the same 3-node KRaft cluster
on your own computer. Create `docker-compose.yml`:

```yaml
x-kafka-env: &kafka-env
  CLUSTER_ID: "q1Sh-9_ISia_zwGINzRvyQ"
  KAFKA_PROCESS_ROLES: "broker,controller"
  KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
  KAFKA_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
  KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
  KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
  KAFKA_LOG_DIRS: "/var/lib/kafka/data"
  KAFKA_DEFAULT_REPLICATION_FACTOR: "3"
  KAFKA_MIN_INSYNC_REPLICAS: "2"
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "3"
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3"
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "2"
  KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE: "false"
  KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
  KAFKA_HEAP_OPTS: "-Xms256m -Xmx256m"

services:
  kafka-1:
    image: apache/kafka:4.1.0
    container_name: kafka-1
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: "1"
      KAFKA_BROKER_RACK: "zone-a"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-1:9092"
  kafka-2:
    image: apache/kafka:4.1.0
    container_name: kafka-2
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: "2"
      KAFKA_BROKER_RACK: "zone-b"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-2:9092"
  kafka-3:
    image: apache/kafka:4.1.0
    container_name: kafka-3
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: "3"
      KAFKA_BROKER_RACK: "zone-c"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-3:9092"
```

```bash
docker compose up -d
docker exec -it kafka-1 bash
export PATH=$PATH:/opt/kafka/bin BS=kafka-1:9092,kafka-2:9092,kafka-3:9092
kafka-metadata-quorum.sh --bootstrap-server $BS describe --status
```

Then do the same labs from Part 5. To "kill a broker," use `docker stop kafka-2`
(and `docker start kafka-2` to bring it back). Clean up with `docker compose down -v`.
