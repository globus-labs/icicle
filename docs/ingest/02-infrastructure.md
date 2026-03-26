# Infrastructure

This document explains how to set up the AWS infrastructure required for
running the ingest pipeline: **Amazon Managed Kafka (MSK)**, **Kafbat UI**,
**S3 code bucket**, and the **IAM role** for Amazon Managed Flink (KDA).


## 1. Amazon Managed Kafka (MSK)

### 1.1. Create an MSK Cluster

Open the [MSK Console](https://us-east-1.console.aws.amazon.com/msk/home?region=us-east-1#/home) → **Create Cluster → Custom create** with:

| Setting | Value |
|---------|-------|
| Cluster Type | Provisioned |
| Kafka Version | 3.8.x |
| ZooKeeper | Enabled |
| Broker Type | `kafka.t3.small` |
| Availability Zones | 2 zones, 1 broker per zone |
| Storage per Broker | 200 GB |
| VPC & Security Group | Default VPC, default security group (accept all traffic on all ports) |
| Authentication | IAM role-based |
| Public Access | Enabled |

### 1.2. Update MSK Configuration

Update the cluster's configuration with the following settings.

> **Note:** `auto.create.topics.enable` is set to `false` below, which means
> you must create Kafka topics manually before running pipelines (see
> [section 2](#2-kafbat)). If you prefer automatic topic creation, set it to
> `true`.

```bash
auto.create.topics.enable=false
default.replication.factor=2
min.insync.replicas=2
num.io.threads=8
num.network.threads=5
num.partitions=1
num.replica.fetchers=2
replica.lag.time.max.ms=30000
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
socket.send.buffer.bytes=102400
unclean.leader.election.enable=false
allow.everyone.if.no.acl.found=false
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
```

### 1.3. Get Broker Endpoints

* Obtain the **public bootstrap servers** from the MSK console.
  Example:

  ```
  b-1-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198,
  b-2-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198
  ```

### 1.4. IAM User for Kafka

* Create an IAM user: **`lighthouse-dev`**
* Attach the **`AdministratorAccess`** policy.
  ⚠️ **Use with caution** — this grants full AWS permissions.
* Generate **access keys** and **secret keys** for CLI and SDK usage.

### 1.5. Test Connectivity

Use `nc` to verify connectivity to the MSK brokers:

```bash
nc -vz b-1-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com 9198
```

### 1.6. Update Bootstrap Servers

Search for `set_bootstrap_servers` in the repository and **replace the server IPs** with the retrieved bootstrap endpoints.


## 2. Kafbat


[Kafbat](https://github.com/kafbat/kafka-ui) provides a web-based Kafka management UI to **create/truncate topics** and **view messages**.

> **Deployment:** `scripts/icicle-deploy/deploy-gpfs.ipynb` includes a Docker Compose
> setup that deploys Kafbat alongside the GPFS infrastructure. Run the Kafbat
> cells in that notebook to get a UI at `http://<host>:8080/`.

> **Note:** Topic names are defined in `IngestConfig` in [`src/ingest/conf.py`](../../src/ingest/conf.py).
> If you changed `auto.create.topics.enable` to `true` in [section 1.2](#12-update-msk-configuration),
> topics are created automatically on first use. Otherwise, create them
> manually via Kafbat UI before running pipelines.

### 2.1. Required Kafka Topics

Create the following topics with **1 partition** each via Kafbat UI:

| Topic | Used by |
|---|---|
| `search-alpha-primary-ingest-results` | PRI pipeline results |
| `search-alpha-secondary-ingest-results` | SND pipeline results |
| `search-alpha-counts` | CNT pipeline results |

To customize topic names, edit the `IngestConfig` dataclass in
[`src/ingest/conf.py`](../../src/ingest/conf.py).


## 3. S3 Code Bucket

Create an S3 bucket to hold the compiled application ZIP uploaded by
`make compile`:

```bash
aws s3 mb s3://search-alpha-bucket-1
```

> **Note:** S3 bucket names are globally unique. The name `search-alpha-bucket-1`
> is used throughout the repo (`compile.sh`, `kda-config.sh`, docs). If you
> choose a different name, update those files accordingly. This bucket is
> separate from the input data buckets (`icicle-fs-small`, etc.) created in
> [01-preprocessing.md](01-preprocessing.md).


## 4. IAM Role for Managed Flink

An IAM role is required for the **Managed Flink** application to access **S3** and **MSK** resources.

### 4.1. Role Setup

* **Role Name:** `Kinesis-Analytics-Admin`
* **Usage:** Grant the Flink application permissions to access both MSK and S3.

### 4.2. Permissions Policy (Development)

⚠️ **Warning:** This policy grants **full access** to all resources. Restrict permissions in production environments.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "*",
      "Resource": "*"
    }
  ]
}
```

### 4.3. Trust Policy

Allow the **Kinesis Analytics / Managed Flink** service to assume this role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "kinesisanalytics.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```
