# Convert Stage

Copyright (c) 2026 Smile Digital Health Inc. All rights reserved. See `legal/LICENSE`.

The convert stage takes each record the split stage (the `file-connector` distribution) has split, converts it to
JSON (C-CDA, X12, EDIFACT, CSV and others; FHIR stays as it is), stores it encrypted in Apache Iceberg tables, and
announces it on a message queue with a signed, encrypted message for the readers downstream. A record that can't be
converted is stored in its original form and announced as failed. When all of an item's records are stored, it
announces the item, and, if the intake's producer receives results, sends them its result.

This distribution contains no Java runtime. Run it on a host with Java 25, or build a container image from it
on a Java 25 base image of your choice.

## Contents

| Path | What it is |
|---|---|
| `app.jar` | The application |
| `bin/convert-stage`, `bin/sdh-admin` | Start the stage, or run an operator command, on a host |
| `config/jvm.options` | JVM options the stage must run with (no heap or core dumps, no attach) |
| `config/application.yml.example` | Every setting, with defaults where they exist |
| `container/` | `Dockerfile` and container-only JVM options, for building an image |
| `native/linux-amd64/`, `native/linux-arm64/` | zstd and snappy native libraries, for containers with a `noexec` `/tmp` |
| `legal/` | `LICENSE`, `NOTICE`, `THIRD-PARTY.txt` (every bundled component, its licenses and notices), the license texts, and `sbom.cdx.json` (a CycloneDX software bill of materials for `app.jar`) |

## Verify what you received

Each release comes with a public key, `cosign.pub`, published by Smile Digital Health. Check the tarball's
signature before unpacking it, with [cosign](https://docs.sigstore.dev/) or with openssl alone:

```sh
cosign verify-blob --key cosign.pub --insecure-ignore-tlog=true \
  --signature convert-stage-<version>-dist.tar.gz.sig convert-stage-<version>-dist.tar.gz
openssl dgst -sha256 -verify cosign.pub \
  -signature <(base64 -d convert-stage-<version>-dist.tar.gz.sig) convert-stage-<version>-dist.tar.gz
```

The container image is signed in its registry, and its software bill of materials (OS packages and Java components)
is attached as a signed attestation:

```sh
cosign verify --key cosign.pub --insecure-ignore-tlog=true <registry>/convert-stage@sha256:<digest>
cosign verify-attestation --key cosign.pub --insecure-ignore-tlog=true --type cyclonedx \
  <registry>/convert-stage@sha256:<digest>
```

Signatures aren't recorded in Sigstore's public transparency log, so `--insecure-ignore-tlog=true` is expected; the
public key is what you trust.

## Requirements

- Linux on x86-64 or ARM64, with glibc.
- Java 25.
- PostgreSQL, for the duties lease and the stage's registry of the records it has stored; the split stage's
  database can be used.
- The Iceberg catalog and object storage the split stage writes `split_records` to: REST (for example Apache
  Polaris) or JDBC, on Amazon S3, Google Cloud Storage or Azure Data Lake Storage Gen2.
- A message broker: Kafka, Amazon SQS, Google Pub/Sub, Azure Service Bus or Event Hubs, ActiveMQ Artemis or RabbitMQ.
- Keys: a local keystore and PEM files, or a KMS (AWS KMS, Azure Key Vault or HashiCorp Vault Transit). The stage
  needs `records-kek` (to read split records and write records; it never reads landed items), its own signing key,
  the downstream readers' public key, its inbox key pair (the split stage encrypts its messages to the public half)
  and the split stage's signing public key (`sdh.crypto.keys.landing-signer`).
- The split stage, and the queue it sends `record.split` and `item.split` on (`sdh.stage.input`).

Run as many instances as you like. Each converts records as they arrive, claiming each one first, so records are
converted in parallel across instances and no record by two at once. A record whose instance crashed is taken up
again when its message is delivered again. One instance at a time also finds split records whose message was lost
and runs table maintenance; if it stops, another takes over within the lease's TTL (60 seconds by default).

## Configure

Copy `config/application.yml.example` to `config/application.yml` and edit it. Pass secrets as environment
variables, never in the file:
- `SDH_CRYPTO_KEYSTORE_PASSWORD`
- `SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE`
- `SDH_CRYPTO_KEYS_INBOX_PASSPHRASE`
- `SDH_LEASE_JDBC_PASSWORD`
- catalog and storage credentials

Any setting can also be given as an environment variable (`sdh.lease.jdbc.url` becomes `SDH_LEASE_JDBC_URL`) or as an argument (`--sdh.lease.jdbc.url=…`).

## Run on a host

```sh
bin/convert-stage                          # Java 25 on the PATH, or JAVA=/path/to/java
JAVA_OPTS=-Xmx2g bin/convert-stage         # extra JVM options
SDH_OPERATOR=alice bin/sdh-admin status    # operator commands: status
```

Run it under a service manager (for example systemd) that restarts it when it exits. The stage exits with status
70 on conditions it can't safely continue from, and resumes where it stopped. At startup it waits for a key service
that is down or not serving, logging `crypto.kms_waiting`; a key or permission refused stops it at once.

## Build a container image

From this directory:

```sh
docker build -f container/Dockerfile -t <registry>/convert-stage:<version> .
```

The default base image is `gcr.io/distroless/java25-debian13:nonroot`, a Java 25 runtime with no shell or package
tools. To use another Java 25 image, add `--build-arg JAVA_IMAGE=<image>`, for example `eclipse-temurin:25-jre`. It
must be Linux with glibc, and `java` must be on the `PATH`. Pin the base image by digest for production, and rebuild
when it receives security updates.

The image runs as uid/gid 10001. It contains the application in `/app` and these notices in `/app/legal/`. The base
image brings its own notices. For Debian-based images such as distroless, see `/usr/share/doc/<package>/copyright`.

## Deploy on Kubernetes

The stage needs no data volume. It needs:
- **`/tmp`:** a writable `emptyDir` of about 1 GiB; `noexec` is fine. Uploads to object storage are staged here, with record data already encrypted.
- **`/run/secrets`:** the keys, as a read-only secret volume (local key mode only).
- **Configuration:** a ConfigMap mounted as `application.yml`, with `SPRING_CONFIG_ADDITIONAL_LOCATION` pointing at it.
- **Secrets:** a Secret whose keys are environment variable names, passed whole with `envFrom`. It holds
  `SDH_CRYPTO_KEYSTORE_PASSWORD`, `SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE` (both local key mode only),
  `SDH_CRYPTO_KEYS_INBOX_PASSPHRASE`, `SDH_LEASE_JDBC_PASSWORD`, and whatever `application.yml` refers to as
  `${...}`, such as catalog and storage credentials.

```sh
kubectl create secret generic convert-stage-keys --from-file=keks.p12 --from-file=signing.pem \
  --from-file=recipient.pem --from-file=inbox.pem --from-file=split-signer.pem
kubectl create secret generic convert-stage-env --from-literal=SDH_CRYPTO_KEYSTORE_PASSWORD=… \
  --from-literal=SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE=… --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE=… \
  --from-literal=SDH_LEASE_JDBC_PASSWORD=…
kubectl create configmap convert-stage-config --from-file=application.yml
```

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: convert-stage }
spec:
  replicas: 2                          # any number: records are converted in parallel
  strategy: { type: Recreate }         # no two builds at once: a record received again under another is checked again
  selector: { matchLabels: { app: convert-stage } }
  template:
    metadata: { labels: { app: convert-stage } }
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001                 # secret files become root:10001, mode 0440
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: convert-stage
          image: <registry>/convert-stage:<version>
          env:
            - { name: SPRING_CONFIG_ADDITIONAL_LOCATION, value: "file:/config/" }
          envFrom:
            - secretRef: { name: convert-stage-env }
          securityContext:
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities: { drop: [ALL] }
          resources: { limits: { memory: 2Gi } }   # the heap is half the limit; the rest is the JVM's own memory and operator commands
          volumeMounts:
            - { name: tmp, mountPath: /tmp }
            - { name: keys, mountPath: /run/secrets, readOnly: true }
            - { name: config, mountPath: /config, readOnly: true }
      volumes:
        - { name: tmp, emptyDir: { sizeLimit: 1Gi } }
        - { name: keys, secret: { secretName: convert-stage-keys, defaultMode: 0440 } }
        - { name: config, configMap: { name: convert-stage-config } }
```

There is no HTTP health endpoint. The process is the health signal: it exits on conditions it can't safely continue
from, and Kubernetes restarts it.

### Autoscaling

Any number of pods can run: they share the work through the broker and the lease database. To scale them on the
backlog, use [KEDA](https://keda.sh) with a `ScaledObject` on the stage's input, which has a scaler for each
supported broker (Kafka consumer lag, SQS, Pub/Sub, Service Bus, RabbitMQ, Event Hubs, Artemis queue depth). On
Kafka:

```
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: { name: convert-stage }
spec:
  scaleTargetRef: { name: convert-stage }
  minReplicaCount: 1                   # never 0: see below
  maxReplicaCount: 6                   # no more than the input topic's partitions
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: <brokers>
        consumerGroup: <the stage's groupId>
        topic: <the split stage's output topic>
        lagThreshold: "20"                # records waiting per pod
```

- **Keep at least one pod.** The pod holding the duties lease reconciles: it finds the work whose messages were
  lost, which adds nothing to the backlog, so a stage scaled to zero would never be started to find it.
- **Remove `replicas` from the Deployment,** or each apply resets what KEDA scaled to.
- **On Kafka, give the input topic at least `maxReplicaCount` partitions:** a pod beyond that has nothing to read.
  Partitions can be added later, with the topic drained.
- **On Kafka 4.0 or later, add `groupProtocol=consumer` to the input's URI** (KIP-848), so that a rebalance, at
  each scaling, doesn't wait for every pod's next poll.
- Scaling down is safe, but not instant: a pod that stops leaves the records it hadn't stored under its claims,
  which expire after `sdh.stage.claim.ttl` (60 s). They are delivered again, and another pod converts them,
  about a minute late. Nothing is lost or converted twice.

### Operator commands

The image has no shell, so run the JVM directly:

```sh
kubectl exec <pod> -- java @/app/jvm.options @/app/container.options @/app/admin.args status --operator alice
```

`--operator` names who is running the command, for the audit log; on a host, `$SDH_OPERATOR` does the same.
`status` shows the duties lease, the items not yet complete, the records claimed now, and how far the reconciler
has checked (`reconciler`); `status --item <id>` shows one item and its records by state. It reads only the lease
database.

### After an outage

The pod holding the duties lease runs the reconciler: at startup, then every `sdh.reconcile.interval` (15 minutes).
It finds records split more than `sdh.reconcile.grace` (10 minutes) ago that this stage hasn't converted, rebuilds
their messages and converts them. It remembers how far it has checked, so after an outage it checks every day
since, a day at a time, saving its progress after each.

It can't tell a message still queued from a lost one, so it waits for the queue first. It waits
`sdh.reconcile.backlog-quiet` (15 minutes) after it starts, for the pods to begin receiving. Then it keeps waiting
while records older than the grace are still coming off the queue on any pod. When the queue has been quiet that
long, it rebuilds what is really lost.

So the first start after a long outage is slower than usual:
- **Lost records are converted last,** after the backlog has drained and the queue has been quiet for 15 minutes.
- **It then reads every day it missed:** a week down is seven days of record IDs to check.
- **If the backlog takes longer than `sdh.reconcile.max-deferral` (6 hours) to drain,** it goes ahead anyway. Then it
  converts records still on the queue, one at a time on the duties pod, while the other pods read them off the
  queue. Each record is still converted once, and the second copy is dropped as a duplicate. But the backlog drains
  more slowly, and a failed record it reaches first can't be placed in the producer's result
  (`convert.placement_unknown`; the record and its error are stored as usual, and the event's record number places
  it by hand). After that run it waits again. If you expect a longer drain, raise `max-deferral` for the restart.

What to watch:
- **`status`'s `reconciler` line:** how far it has checked and how long ago. More than an hour behind, it warns: the
  duties pod is down (see the `duties` line), or it is waiting for the backlog or catching up.
- **`convert.reconcile_deferred`** (audit, `sdh.reason` `STARTING` or `BACKLOG`, `sdh.count` = minutes waited): a run
  that waited. Expect `STARTING` once after each start, and `BACKLOG` while a backlog drains.
- **`convert.reconcile_deferral_capped`:** it stopped waiting after `max-deferral`. On a normal day this means the
  queue is always more than the grace behind: add pods.
- **`convert.reconcile_catching_up` and `convert.reconcile_caught_up`** (audit, `sdh.count` = days): the start and
  end of a catch-up. A start with no end means the pod stopped part-way; the next run resumes from the last day
  checked.
- **`convert.records_recovered`:** a message was lost, unless it follows `reconcile_deferral_capped` (then it is
  mostly queued records).
- **`convert.item_total_missing`:** an item whose records are all converted but whose `item.split` hasn't come. Act
  only if it is still incomplete once the queue has drained: replay it from the landing node (`replay --item-id`).
- **Outage length against queue retention** (Kafka and Pub/Sub 7 days by default, SQS 4): longer, and queued
  messages are lost. Check the retention before a planned outage, and drain the queues first if you can: then the
  reconciler waits only its 15 minutes at startup.
- **Afterwards,** check that every item completed at the split stage also completed here (`stage.item_completed`).

A stage upgraded from a build without the watermark checks only the last `sdh.reconcile.lookback` (2 days) its first
time.

### Troubleshooting

A shell-less container can't be entered with `kubectl exec -- sh`. Attach a debug container instead (kubectl 1.27 or
later):

```sh
kubectl debug -it <pod> --image=busybox --target=convert-stage --profile=restricted
```

`--profile=restricted` lets it start in a namespace that enforces the restricted Pod Security Standard. It runs as
the stage's user and shares its processes, so the stage's files are under `/proc/1/root/`. The debug container stays
in the pod's spec, stopped, until the pod is replaced.

## Licenses

`legal/THIRD-PARTY.txt` lists every third-party component in `app.jar` with its licenses, copyright and notices.
`legal/licenses/` holds the license texts. Where a component is offered under a choice of licenses, all of them are
listed, and its "Elected license" line names the one Smile Digital Health distributes it under. Permissive licenses
(MIT, Apache, BSD, ISC, Creative Commons) are elected over any GNU General Public License or similar license.
