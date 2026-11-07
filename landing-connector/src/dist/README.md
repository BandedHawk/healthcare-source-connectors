# Landing Connector

Copyright (c) 2026 Smile Digital Health Inc. All rights reserved. See `legal/LICENSE`.

A landing node picks up healthcare data from one intake (a directory, a remote share, HTTP push or a message
queue), stores each item exactly as received,
encrypted, in Apache Iceberg tables, and announces it to the file connector on a message queue with a signed,
encrypted `item.landed` message. The file connector then splits it. Landed items are never changed.

This distribution contains no Java runtime. Run it on a host with Java 25, or build a container image from it
on a Java 25 base image of your choice.

## Contents

| Path | What it is |
|---|---|
| `app.jar` | The application |
| `bin/landing-connector`, `bin/sdh-admin` | Start the node, or run an operator command, on a host |
| `config/jvm.options` | JVM options the node must run with (no heap or core dumps, no attach) |
| `config/application.yml.example` | Every setting, with defaults where they exist |
| `container/` | `Dockerfile` and container-only JVM options, for building an image |
| `native/linux-amd64/`, `native/linux-arm64/` | zstd and snappy native libraries, for containers with a `noexec` `/tmp` |
| `legal/` | `LICENSE`, `NOTICE`, `THIRD-PARTY.txt` (every bundled component, its licenses and notices), the license texts, and `sbom.cdx.json` (a CycloneDX software bill of materials for `app.jar`) |

## Verify what you received

Each release comes with a public key, `cosign.pub`, published by Smile Digital Health. Check the tarball's
signature before unpacking it, with [cosign](https://docs.sigstore.dev/) or with openssl alone:

```sh
cosign verify-blob --key cosign.pub --insecure-ignore-tlog=true \
  --signature landing-connector-<version>-dist.tar.gz.sig landing-connector-<version>-dist.tar.gz
openssl dgst -sha256 -verify cosign.pub \
  -signature <(base64 -d landing-connector-<version>-dist.tar.gz.sig) landing-connector-<version>-dist.tar.gz
```

The container image is signed in its registry, and its software bill of materials (OS packages and Java components)
is attached as a signed attestation:

```sh
cosign verify --key cosign.pub --insecure-ignore-tlog=true <registry>/landing-connector@sha256:<digest>
cosign verify-attestation --key cosign.pub --insecure-ignore-tlog=true --type cyclonedx \
  <registry>/landing-connector@sha256:<digest>
```

Signatures aren't recorded in Sigstore's public transparency log, so `--insecure-ignore-tlog=true` is expected; the
public key is what you trust.

## Requirements

- Linux on x86-64 or ARM64, with glibc.
- Java 25.
- PostgreSQL, for the lease; it can be the JDBC catalog's database.
- An Iceberg catalog (REST or JDBC) and object storage, as for the file connector. The landing tables live in
  their own namespace (`sdh_landing` by default), which may be in another catalog or storage account.
- A message broker the file connector consumes from.
- Keys: a local keystore and PEM files, or a KMS. The node needs `landed-items-kek`, its own signing key, and the
  file connector's inbox public key, which messages are encrypted to.

Run one active node per intake. A second node for the same intake is refused at startup, after waiting up to 70
seconds in case the first has crashed. A node started with `sdh.landing.standby: true` waits instead, for as long as
it takes, and takes over within about a minute when the active node stops.

## Configure

Copy `config/application.yml.example` to `config/application.yml` and edit it. Pass secrets as environment
variables, never in the file:
- `SDH_CRYPTO_KEYSTORE_PASSWORD`
- `SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE`
- `SDH_LEASE_JDBC_PASSWORD`
- `SDH_LANDING_SOURCE_SFTP_PASSWORD` or `SDH_LANDING_SOURCE_SFTP_PRIVATE_KEY_PASSPHRASE`, for an SFTP source
- `SDH_LANDING_SOURCE_SMB_PASSWORD`, for an SMB source
- `SDH_LANDING_SOURCE_BUCKET_SECRET_ACCESS_KEY` or `SDH_LANDING_SOURCE_BUCKET_CONNECTION_STRING`, for an S3 or Azure
  Blob source with a key; otherwise the cloud's default credentials are used
- `SDH_LANDING_HTTP_TLS_PRIVATE_KEY_PASSWORD`, for an HTTP push intake whose TLS key has a password
- `SDH_LANDING_BROKER_RABBITMQ_PASSWORD`, `SDH_LANDING_BROKER_ARTEMIS_PASSWORD` or
  `SDH_LANDING_BROKER_EVENTHUBS_CHECKPOINT_CONNECTION_STRING`, for a RabbitMQ, Artemis or Event Hubs queue
  intake; other brokers' secrets go in the URI as placeholders, such as `{{env:KAFKA_JAAS}}`
- catalog and storage credentials

Any setting can also be given as an environment variable (`sdh.lease.jdbc.url` becomes `SDH_LEASE_JDBC_URL`) or as
an argument (`--sdh.lease.jdbc.url=…`).

The source is read-only by default: files are left where they are, and a ledger keeps them from being landed twice.
With `access: read-write`, each file is deleted, moved or renamed once it has landed and been announced; the node
checks at startup that it can do so.

The source is a local or mounted directory (`type: directory`), an SFTP server (`type: sftp`), an SMB share
(`type: smb`), a bucket (`type: s3`, `azure-blob` or `gcs`), an Iceberg table (`type: iceberg`), HTTP push
(`type: http`), or a message queue (`type: queue`):
- **SFTP:** the server's host key must be pinned in a known-hosts file; an unknown or changed key is never
  accepted. Log in with a private key in OpenSSH format, or a password.
- **SMB:** SMB 3 with encryption only, NTLMv2 with a user and password; guest and anonymous logins are refused, and
  so is a share that doesn't encrypt.
- **Buckets:** an S3 bucket (or one on an S3-compatible store, with `endpoint` and usually `path-style-access`), an
  Azure Blob container, or a GCS bucket (`sdh.landing.source.bucket`). The bucket is listed every `poll-interval`.
  Credentials are an S3 access key or Azure connection string from the environment, or the cloud's default
  credentials (AWS default chain, Azure default credential with `endpoint`, Google application default
  credentials). Objects appear whole, so `settle: 0s` is usually enough. An object written again is a new version
  and is landed again. `move` and `rename` copy, then delete; the copy and delete happen only if the object hasn't
  changed since it was listed.
- **Iceberg tables:** the table is read through its own catalog (`sdh.landing.source.table`), read-only, every
  `poll-interval`. Each Parquet data file that an append adds is landed as one item, exactly as it is; set the file
  connector's format to `parquet` to split it into one JSON record per row. With `start: earliest` (the default),
  the rows the table holds at the first start are landed first, then each append; with `latest`, only appends made
  after the first start. A compaction lands nothing. An overwrite or delete can't be landed as new items: it is
  reported as `intake.table_rewrite` and passed over, so tell the producer to append. Files that aren't Parquet, are
  encrypted, or (at the first start) have row-level deletes are skipped and reported. How far the intake has got is
  kept on the ledger table, so a restart carries on; keep the source table's snapshots for longer than the node
  could be down, or `intake.table_history_lost` is reported and some appends may be missed.
- **Paths** on a remote source are absolute paths on the server (`path`, `move-to`); on a bucket they are key
  prefixes, `/` being the bucket's root, so `path: /inbound` reads keys under `inbound/`.
- **Outages:** a dropped or stalled connection is noticed within `read-timeout` and retried at each poll; an item
  cut off part way resumes where it stopped. Refused credentials, a missing share or bucket, or a changed host key
  are retried only every `auth-retry-interval`, so an account isn't locked. Each is reported once, by reason, as
  `intake.unreachable`, and `intake.reachable` when it is back. No names or paths are logged.
- **HTTP push:** producers send each item as the body of `POST https://<host>:8443/intakes/<intake>/items`, over
  TLS (`sdh.landing.http.tls`). They authenticate with an OAuth2 bearer token (checked for the issuer, audience and
  scope) or with a client certificate (from `trusted-cas`, with a subject in `subjects`). The body is streamed into
  landing, never held whole or written to disk unencrypted. The answer is `202` with a receipt: the item ID, size,
  SHA-256 and landing time. A producer that isn't sure a push arrived sends it again with the same
  `Idempotency-Key` header and gets the same receipt. Pushes are landed one at a time; one that waits longer than
  `max-wait` gets `503` with `Retry-After`, as does any push while storage or the broker is down. A body larger
  than `max-item-bytes` gets `413`, and one cut off gets `400`. The port opens only on the active node, so a
  standby isn't sent pushes; use a TCP probe on it. An `X-Correlation-Id` header (up to 255 visible ASCII
  characters) is kept, encrypted, as the producer's reference, and a `Replaces` header naming an item this intake
  landed records that the new item replaces it (an unknown item gets `400`).
- **Queues:** Kafka, Amazon SQS, RabbitMQ, ActiveMQ Artemis, Google Pub/Sub, Azure Service Bus or Azure Event Hubs
  (`sdh.landing.broker`). Messages are landed in
  batches: a batch closes at `max-messages`, `max-bytes` or `max-wait`, and becomes one item that keeps where each
  message starts. A message is acknowledged (for Kafka, its offset committed) only once its batch is landed; if the
  node stops first, the broker delivers it again. While storage or the broker for `item.landed` is down, a batch is
  held and tried again, rather than handed back, so a broker's delivery limit doesn't dead-letter it. A message
  delivered again within `dedup-window` of the last batch
  is recognised by its broker ID and not landed twice. RabbitMQ messages have an ID only if the producer sets
  `message_id`; without one, a message delivered again after a crash is landed again. Give the Kafka consumer
  group, Pub/Sub subscription or Event Hubs consumer group to this intake alone. Event Hubs keeps its place in a blob
  container you create (`sdh.landing.broker.eventhubs`); a new consumer group starts from the oldest event the hub
  holds. A Service Bus queue or subscription must not require sessions. The file connector splits a batch message
  by message, and a message it can't split fails alone.

**Producer feedback** (`sdh.results`, off by default). The node sends each item's receipt to the producer's own
queue, sealed to the producer's public key (`sdh.results.recipient`, pinned by SHA-256): the item ID, size, SHA-256,
landing time, the producer's reference (the file's path, or the push's `X-Correlation-Id`), and for a queue
batch each message's broker ID. The file connector of the same intake sends the final result there too (see its
README). If the results queue is down, landing waits; nothing is lost. An HTTP push intake with results must admit
just one sender: set `sdh.landing.http.oauth2.subject`, or list one `mtls.subjects` entry.

**Resubmissions.** A producer that sends a corrected file can say which item it replaces: a `Replaces` header on a
push, or, on a directory, SFTP, SMB or bucket intake, a file `<name>.sdh-replaces` beside `<name>` holding the
earlier item's ID, put there before `<name>` is ready. Both items are kept in full; only the link is recorded. A
marker that isn't a valid ID of an item this intake landed is ignored and reported as `item.replaces_ignored`, and
the file is landed without the link. Marker files are never landed, and on a read-write intake are deleted once
the file has landed.

Landed items are kept forever unless you set `sdh.maintenance.retention.landed-items` (at least `7d`, so the
splitter has time to read each item). Retention removes whole days, and never part of an item. The ledger is always
kept, because a read-only source would otherwise land its files again. Set the retention longer than the splitter
can ever fall behind: an item purged before the splitter reads it is reported there as failed.

An item given up part way (it changed or vanished while being read, a push broke off, a batch failed) leaves
stored chunks that nothing reads. They are removed `sdh.maintenance.retention.given-up` after the item was given up
(`7d` by default, at least `1d`), and each removal is audited as `item.purged`. Maintenance must be on.

A read-write source, local or remote, is claimed by the node in a hidden file `.sdh-owner` in the source directory
(an object under the prefix on a bucket), so a second node with other settings is refused. To hand the source to
another node, stop the old one and delete `.sdh-owner`.

### Memory

The node keeps its ledger, one entry for every file or object it has landed, in memory, and keeps it forever. Queue
and HTTP intakes add one entry per batch or push, so they stay small; a listing intake that lands many small files
grows. The ledger may take half the heap. A node whose ledger has outgrown that won't start: it logs
`ledger.too_large` (error code `LEDGER_TOO_LARGE`) with the heap it needs. Before that, `ledger.memory_low` warns
once, while there is still room. `sdh.landing.ledger_items` gives the number of entries.

| Ledger entries | Heap at least | Container memory limit (heap is half) |
|---|---|---|
| up to 5.5 million | 512 MiB | 1Gi |
| up to 11 million | 1 GiB | 2Gi |
| up to 22 million | 2 GiB | 4Gi |
| up to 44 million | 4 GiB | 8Gi |

Leave room to grow: a ledger near the top of its row needs the next row's heap the next time its index doubles.

## Run on a host

```sh
bin/landing-connector                      # Java 25 on the PATH, or JAVA=/path/to/java
JAVA_OPTS=-Xmx1g bin/landing-connector     # extra JVM options
SDH_OPERATOR=alice bin/sdh-admin replay --item-id <id>   # operator commands: replay
```

Run it under a service manager (for example systemd) that restarts it when it exits. The node exits with status 70
on conditions it can't safely continue from, and resumes where it stopped.

## Build a container image

From this directory:

```sh
docker build -f container/Dockerfile -t <registry>/landing-connector:<version> .
```

The default base image is `gcr.io/distroless/java25-debian13:nonroot`, a Java 25 runtime with no shell or package
tools. To use another Java 25 image, add `--build-arg JAVA_IMAGE=<image>`, for example `eclipse-temurin:25-jre`. It
must be Linux with glibc, and `java` must be on the `PATH`. Pin the base image by digest for production, and rebuild
when it receives security updates.

The image runs as uid/gid 10001. It contains:
- the application in `/app`;
- these notices in `/app/legal/`.

## Deploy on Kubernetes

The node needs:
- **`/data/intake`:** the intake directory, shared with whatever delivers files. Read-only is enough for a
  read-only source.
- **`/tmp`:** a writable `emptyDir`; `noexec` is fine. Uploads to object storage are staged here, already
  encrypted.
- **`/run/secrets`:** the keys, as a read-only secret volume (local key mode only).
- **Configuration:** a ConfigMap mounted as `application.yml`, with `SPRING_CONFIG_ADDITIONAL_LOCATION` pointing at it.
- **Secrets:** a Secret whose keys are environment variable names, passed whole with `envFrom`.
- **Queue intakes:** no volume for the intake; the broker's TLS trust store, if it isn't public, in the secret
  volume.
- **HTTP push only:** a Service for the push port, the TLS certificate and key in the secret volume, and a TCP
  readiness probe on the port, so only the active node receives pushes. For mTLS, the ingress or load balancer
  must pass TLS through to the pod, so that the node sees the client's own certificate. Give the Deployment
  `strategy: { type: Recreate }`. A standby is never ready, so a rolling update waits for ever for a new pod to
  become ready while the old active node holds the lease. With Recreate, both pods are replaced together, and
  pushes are refused (connection refused, then `503`) until the new active node takes the lease. For the same
  reason the Deployment shows one of its two replicas ready, and `kubectl rollout status` never completes: watch
  for one ready pod instead.

Two replicas give an active node and a standby: both run with `standby: true`, the first to take the lease lands,
and the other waits. The intake volume must be mountable by both pods (`ReadWriteMany`, or `ReadOnlyMany` for a
read-only source, unless both run on one node).

```sh
kubectl create secret generic landing-claims-keys --from-file=keks.p12 --from-file=signing.pem \
  --from-file=splitter-inbox.pem
kubectl create secret generic landing-claims-env --from-literal=SDH_CRYPTO_KEYSTORE_PASSWORD=… \
  --from-literal=SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE=… --from-literal=SDH_LEASE_JDBC_PASSWORD=…
kubectl create configmap landing-claims-config --from-file=application.yml
```

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: landing-claims }
spec:
  replicas: 2                          # the active node and a standby
  selector: { matchLabels: { app: landing-claims } }
  template:
    metadata: { labels: { app: landing-claims } }
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001                 # secret files become root:10001, mode 0440
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: landing-connector
          image: <registry>/landing-connector:<version>
          env:
            - { name: SPRING_CONFIG_ADDITIONAL_LOCATION, value: "file:/config/" }
            - { name: SDH_LANDING_STANDBY, value: "true" }   # both replicas wait; the first to get the lease lands
          envFrom:
            - secretRef: { name: landing-claims-env }
          securityContext:
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities: { drop: [ALL] }
          resources: { limits: { memory: 1Gi } }   # the heap is half the limit
          volumeMounts:
            - { name: intake, mountPath: /data/intake }
            - { name: tmp, mountPath: /tmp }
            - { name: keys, mountPath: /run/secrets, readOnly: true }
            - { name: config, mountPath: /config, readOnly: true }
      volumes:
        - { name: intake, persistentVolumeClaim: { claimName: landing-claims-intake } }
        - { name: tmp, emptyDir: { sizeLimit: 1Gi } }
        - { name: keys, secret: { secretName: landing-claims-keys, defaultMode: 0440 } }
        - { name: config, configMap: { name: landing-claims-config } }
```

There is no HTTP health endpoint. The process is the health signal: it exits on conditions it can't safely continue
from, and Kubernetes restarts it. A standby is healthy while it waits.

### Operator commands

The image has no shell, so run the JVM directly:

```sh
kubectl exec <pod> -- java @/app/jvm.options @/app/container.options @/app/admin.args \
  replay --item-id <id> --operator alice
```

`--operator` names who is running the command, for the audit log; on a host, `$SDH_OPERATOR` does the same.
`replay --item-id <id>` sends a fully landed item's `item.landed` again and waits for the broker to acknowledge
it. It is how an operator recovers an item lost between landing and the split stage, or whose `item.split` was
lost after: the split stage splits an item it hasn't, and announces again one it has. The broker is given an ID
of its own for the message (`il-<id>~replay-<millis>`), so a broker that drops repeated IDs doesn't drop it; the
message itself is the one first sent. No receipt goes to the producer, and nothing is written to the tables, so
it is safe to run beside the node. Every run is audited (`admin.replay`).

### Troubleshooting

A shell-less container can't be entered with `kubectl exec -- sh`. Attach a debug container instead (kubectl 1.27 or
later):

```sh
kubectl debug -it <pod> --image=busybox --target=landing-connector --profile=restricted
```

It runs as the node's user and shares its processes, so the node's files are under `/proc/1/root/`. Landed items are
restored with the file connector's `restore` command.

## Licenses

`legal/THIRD-PARTY.txt` lists every third-party component in `app.jar` with its licenses, copyright and notices.
`legal/licenses/` holds the license texts. Where a component is offered under a choice of licenses, all of them are
listed, and its "Elected license" line names the one Smile Digital Health distributes it under. Permissive licenses
(MIT, Apache, BSD, ISC, Creative Commons) are elected over any GNU General Public License or similar license.
