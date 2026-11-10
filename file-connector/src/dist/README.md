# File Connector

Copyright (c) 2026 Smile Digital Health Inc. All rights reserved. See `legal/LICENSE`.

The file connector takes each healthcare file (C-CDA, X12, EDIFACT, FHIR, CSV and others) that a landing node has
landed, splits it into records, stores every record encrypted in Apache Iceberg tables, and announces each one on a
message queue with a signed, encrypted message. Files reach it only through a landing node (the
`landing-connector` distribution), which stores each one as received and tells the file connector about it with an
`item.landed` message. A piece it can't parse, or an item it can't split to the end, is stored as it is and kept in
the stage's fail store (`stage_failure` in its registry schema, audited `failure.failed`), and not announced; the
item then ends in `item.progress` instead of `item.split`, until every failure is corrected or declined
(STAGES.md §15).

This distribution contains no Java runtime. Run it on a host with Java 25, or build a container image from it
on a Java 25 base image of your choice.

## Contents

| Path | What it is |
|---|---|
| `app.jar` | The application |
| `bin/file-connector`, `bin/sdh-admin` | Start the connector, or run an operator command, on a host |
| `config/jvm.options` | JVM options the connector must run with (no heap or core dumps, no attach) |
| `config/application.yml.example` | Every setting, with defaults where they exist |
| `container/` | `Dockerfile` and container-only JVM options, for building an image |
| `native/linux-amd64/`, `native/linux-arm64/` | zstd and snappy native libraries, for containers with a `noexec` `/tmp` |
| `legal/` | `LICENSE`, `NOTICE`, `THIRD-PARTY.txt` (every bundled component, its licenses and notices), the license texts, and `sbom.cdx.json` (a CycloneDX software bill of materials for `app.jar`) |

## Verify what you received

Each release comes with a public key, `cosign.pub`, published by Smile Digital Health. Check the tarball's
signature before unpacking it, with [cosign](https://docs.sigstore.dev/) or with openssl alone:

```sh
cosign verify-blob --key cosign.pub --insecure-ignore-tlog=true \
  --signature file-connector-<version>-dist.tar.gz.sig file-connector-<version>-dist.tar.gz
openssl dgst -sha256 -verify cosign.pub \
  -signature <(base64 -d file-connector-<version>-dist.tar.gz.sig) file-connector-<version>-dist.tar.gz
```

The container image is signed in its registry, and its software bill of materials (OS packages and Java components)
is attached as a signed attestation:

```sh
cosign verify --key cosign.pub --insecure-ignore-tlog=true <registry>/split-stage@sha256:<digest>
cosign verify-attestation --key cosign.pub --insecure-ignore-tlog=true --type cyclonedx \
  <registry>/split-stage@sha256:<digest>
```

Signatures aren't recorded in Sigstore's public transparency log, so `--insecure-ignore-tlog=true` is expected; the
public key is what you trust. A Kubernetes admission policy (for example Sigstore policy-controller or Kyverno) can
require the signature before the image runs.

## Requirements

- Linux on x86-64 or ARM64, with glibc.
- Java 25.
- PostgreSQL, for the item and duties leases and the items' checkpoints; it can be the JDBC catalog's database.
- An Iceberg catalog: REST (for example Apache Polaris) or JDBC.
- Object storage: Amazon S3, Google Cloud Storage or Azure Data Lake Storage Gen2. Of the S3-compatible stores,
  SeaweedFS is tested; MinIO is not supported.
- A message broker: Kafka, Amazon SQS, Google Pub/Sub, Azure Service Bus or Event Hubs, ActiveMQ Artemis or RabbitMQ.
- Keys: a local keystore and PEM files, or a KMS (AWS KMS, Azure Key Vault or HashiCorp Vault Transit). Google
  Cloud KMS is available but untested against the live service; contact Smile Digital Health before relying on
  it in production. Besides its own keys, the connector needs `landed-items-kek` (to read landed items), its inbox
  key pair (the landing node encrypts `item.landed` to the public half) and the landing node's signing public key.
  To reprocess (below), it also needs its inbox key's public half, pinned by its SHA-256
  (`sdh.crypto.keys.inbox.public-pem`, `.sha256`), and, with a local signing key, that key's public half
  (`sdh.crypto.keys.signing.public-pem`, `.sha256`); a KMS gives its own.
- A landing node for the intake, and the queue it sends `item.landed` on (`sdh.stage.input`).

Run as many instances per intake as you like. Each splits one item at a time, under that item's lease, so items are
split in parallel across instances and no item by two at once. An item whose instance crashed is taken up again, from
its checkpoint, when its message is delivered again. One instance at a time also finds landed items whose message was
lost and runs table maintenance; if it stops, another takes over within the lease's TTL (60 seconds by default).

## Configure

Copy `config/application.yml.example` to `config/application.yml` and edit it. Pass secrets as environment
variables, never in the file:
- `SDH_CRYPTO_KEYSTORE_PASSWORD`
- `SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE`
- `SDH_CRYPTO_KEYS_INBOX_PASSPHRASE`
- `SDH_LEASE_JDBC_PASSWORD`
- catalog and storage credentials

Any setting can also be given as an environment variable (`sdh.lease.jdbc.url` becomes `SDH_LEASE_JDBC_URL`) or as an argument (`--sdh.lease.jdbc.url=…`).

Records are stored as split, in their original form. Conversion to JSON and the producer's final results
(`sdh.results`) are the convert stage's (the `convert-stage` distribution), which reads what this stage sends.

## Run on a host

```sh
bin/file-connector                         # Java 25 on the PATH, or JAVA=/path/to/java
JAVA_OPTS=-Xmx2g bin/file-connector        # extra JVM options
SDH_OPERATOR=alice bin/sdh-admin status    # operator commands: status, restore, failures, decline, reopen, export, correct, retry, release, reprocess
```

Run it under a service manager (for example systemd) that restarts it when it exits. The connector exits with
status 70 on conditions it can't safely continue from, and resumes where it stopped. At startup it waits for a key
service that is down or not serving, logging `crypto.kms_waiting`; a key or permission refused stops it at once.

## Build a container image

From this directory:

```sh
docker build -f container/Dockerfile -t <registry>/split-stage:<version> .
```

The default base image is `gcr.io/distroless/java25-debian13:nonroot`, a Java 25 runtime with no shell or package
tools. To use another Java 25 image, add `--build-arg JAVA_IMAGE=<image>`, for example `eclipse-temurin:25-jre`. It
must be Linux with glibc, and `java` must be on the `PATH`. Pin the base image by digest for production, and rebuild
when it receives security updates.

The image runs as uid/gid 10001. It contains:
- the application in `/app`;
- these notices in `/app/legal/`.

The base image brings its own notices. For Debian-based images such as distroless, see
`/usr/share/doc/<package>/copyright`.

## Deploy on Kubernetes

The connector needs no data volume: files come from landing. It needs:
- **`/tmp`:** a writable `emptyDir` of about 1 GiB; `noexec` is fine. Uploads to object storage are staged here, with record data already encrypted.
- **`/run/secrets`:** the keys, as a read-only secret volume (local key mode only).
- **Configuration:** a ConfigMap mounted as `application.yml`, with `SPRING_CONFIG_ADDITIONAL_LOCATION` pointing at it.
- **Secrets:** a Secret whose keys are environment variable names, passed whole with `envFrom`. It holds
  `SDH_CRYPTO_KEYSTORE_PASSWORD`, `SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE` (both local key mode only),
  `SDH_CRYPTO_KEYS_INBOX_PASSPHRASE`,
  `SDH_LEASE_JDBC_PASSWORD`, and whatever `application.yml` refers to as `${...}`, such as catalog and storage
  credentials.

```sh
kubectl create secret generic file-connector-keys --from-file=keks.p12 --from-file=signing.pem \
  --from-file=recipient.pem --from-file=inbox.pem --from-file=landing-signer.pem
kubectl create secret generic file-connector-env --from-literal=SDH_CRYPTO_KEYSTORE_PASSWORD=… \
  --from-literal=SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE=… --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE=… \
  --from-literal=SDH_LEASE_JDBC_PASSWORD=…
kubectl create configmap file-connector-config --from-file=application.yml
```

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: file-connector }
spec:
  replicas: 2                          # any number: items are split in parallel
  strategy: { type: Recreate }         # no two builds at once: a record received again under another is checked again
  selector: { matchLabels: { app: file-connector } }
  template:
    metadata: { labels: { app: file-connector } }
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001                 # secret files become root:10001, mode 0440
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: file-connector
          image: <registry>/split-stage:<version>
          env:
            - { name: SPRING_CONFIG_ADDITIONAL_LOCATION, value: "file:/config/" }
          envFrom:
            - secretRef: { name: file-connector-env }
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
        - { name: keys, secret: { secretName: file-connector-keys, defaultMode: 0440 } }
        - { name: config, configMap: { name: file-connector-config } }
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
metadata: { name: file-connector }
spec:
  scaleTargetRef: { name: file-connector }
  minReplicaCount: 1                   # never 0: see below
  maxReplicaCount: 6                   # no more than the input topic's partitions
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: <brokers>
        consumerGroup: <the stage's groupId>
        topic: <the landing node's topic>
        lagThreshold: "5"                # items waiting per pod
```

- **Keep at least one pod.** The pod holding the duties lease reconciles: it finds the work whose messages were
  lost, which adds nothing to the backlog, so a stage scaled to zero would never be started to find it.
- **Remove `replicas` from the Deployment,** or each apply resets what KEDA scaled to.
- **On Kafka, give the input topic at least `maxReplicaCount` partitions:** a pod beyond that has nothing to read.
  Partitions can be added later, with the topic drained.
- Scaling down is safe: an item a pod was splitting when it stopped is delivered again, and another pod resumes
  it from its checkpoint once the item's lease is released or has expired. Nothing is lost or split twice.
- **On Kafka, a pod holds its message while it splits the item,** so it doesn't poll meanwhile. The stage sets
  `maxPollIntervalMs` to 30 minutes unless the input's URI sets it: set it above your longest item's split
  time. An item taking longer is moved to another pod, which waits for it, and is announced again; nothing is
  lost, but its partition waits. On Kafka 4.0 or later, add `groupProtocol=consumer` to the URI (KIP-848), so
  that a rebalance, at each scaling, doesn't wait for a pod busy with an item.

### Operator commands

The image has no shell, so run the JVM directly:

```sh
kubectl exec <pod> -- java @/app/jvm.options @/app/container.options @/app/admin.args status --operator alice
```

`--operator` names who is running the command, for the audit log; on a host, `$SDH_OPERATOR` does the same.
`status` shows the duties lease and the items by state; `status --item <id>` shows one item's checkpoint.
`restore --item-id <id> --to /tmp` decrypts a landed item into the pod's `/tmp`. `kubectl cp` can't copy out of this
image, because it needs `tar`; read it through a debug container instead (below), from `/proc/1/root/tmp/<id>`,
and delete it when you're done.
No admin command sends to a broker, so the admin needs no broker settings: what the stage sends after an operator's
command, its pods send. An item lost between this stage and the convert stage is found by the convert stage's
reconciler, or reprocessed there (`reprocess --item-id` on the convert stage), which reads its records from
`split_records` whether or not they ever reached it (STAGES.md §15.8.9). `replay` went with §15.8.10 step 5.

#### Failures

`failures` lists the stage's open failures, newest first, by item, record number (`-1` is the item as a whole),
state, error code and offset; `--all` lists every state, and `--item-id <id>` one item's, with each change: who,
when, and whether a reason was given. It shows no content.

`decline --item-id <id> --seq <n>` records that a failure won't be corrected; `reopen` takes that back. Each needs a
reason, read from standard input, never the command line, so it stays out of shell histories and the cluster's
audit log. Give `-i` to `kubectl exec`:

```sh
kubectl exec -i <pod> -- java @/app/jvm.options @/app/container.options @/app/admin.args decline --item-id <id> --seq -1 --operator alice < reason.txt
```

The reason (at most 4 KiB) is kept encrypted with the stage's records KEK, one entry per change, and is never
printed or logged (`failure.declined`, `failure.reopened`). They write only to the fail store. Within a renewal
interval a pod of the stage sends the item's end again, alone, under the item's lease: `item.split` once none of its
failures is open, so the convert stage can complete it, or `item.progress` with the new counts. An item still being
split counts the change when it ends. The stage notes when it last read an item's failures for its end
(`split_item.announced_at`), and sends the end again whenever one changed since.

To correct a failure, export what failed encrypted to your own key, edit it on your machine, and submit the edit.
Keep the private key on your machine; only the public key goes to the pod:

```sh
openssl ecparam -name prime256v1 -genkey -noout -out operator.key && openssl ec -in operator.key -pubout -out operator.pem
kubectl exec -i <pod> -- java @/app/jvm.options @/app/container.options @/app/admin.args export --item-id <id> --seq <n> --operator alice < operator.pem | sed -n 's/^sdh-export: //p' > failed.jwe
```

For `--seq` from 0, `failed.jwe` holds the piece the splitter couldn't parse; for `--seq -1`, the whole item as
landed. It is a JWE (`ECDH-ES+A256KW`, or `RSA-OAEP-256` for an RSA key; `A256GCM`) that only `operator.key` opens,
with any JOSE tool (for example `step crypto jwe decrypt --key operator.key < failed.jwe`). The export is audited with
the key's SHA-256. Then:

```sh
kubectl exec -i <pod> -- java @/app/jvm.options @/app/container.options @/app/admin.args correct --item-id <id> --seq <n> --operator alice < corrected.txt
```

`correct` takes a failure that is `FAILED`, keeps the edit encrypted (at most the stage's largest item), and moves
the failure to `REVISION_PENDING`. Within a renewal interval a pod splits it, and the failure becomes `SUPERSEDED`
whether or not it passes (STAGES.md §15.8.5):
- a piece's correction must give exactly one record, the piece's next revision at this stage (`g1 r2`, then `r3`,
  …); it is stored in `split_records` at the piece's number and place, with `correction_of` naming the failed row,
  which stays;
- the item's correction is the whole item, edited, and is cut as the item's next generation (`g2`, then `g3`, …;
  STAGES.md §15.8.5): from its start, every record of it stored in `split_records` with `cause` `correction`, and
  sent on with that generation, each naming the item as what it resolves. The records of the generation before are
  kept, and superseded downstream. A correction whose generation is older than the item's when a pod comes to it (a
  reprocess came first) isn't cut: it fails, `CORRECTION_SUPERSEDED`, its edit kept.

If it passes, the new records are sent on as `record.split` with their revision, and the item's end is
sent again (`item.split` once nothing else is open, its failed count without the corrected piece). If it fails
(`CORRECTION_RECORD_COUNT`, or the splitter's code), nothing is stored for a piece, and the
revision is a failure of its own, `FAILED`, which you correct in turn. `failures --item-id` shows each failure with
its revision, its own changes (a failure superseded is changed by `pipeline`), and each correction submitted for it,
the revision it makes and what it came to. Delete `failed.jwe` and your edits when you're done. `restore` also gives a landed item whole, into the pod.

`retry --item-id <id> --seq <n>` processes what failed again, unchanged, for a failure that wasn't the data's (a
limit raised, the stage fixed): it needs no input, and takes a failure that is `FAILED`, as `correct` does. A pod
splits it within a renewal interval, under the rules above, as the next revision (for `--seq -1`, the next
generation, cut from the item as landed); if it fails, its own failure, with the retry's error code, is the one
open.

A record that passed by a correction or retry names the failure it resolves in its `record.split` (`resolves`).
Reopening a failure of an item already complete (`reopen`) makes it incomplete again: the stage sends
`item.progress`, which the convert stage takes as reopening its own item, and once nothing is open the item
completes again, under a message ID of its own (`~c2`, `~c3`, …; STAGES.md §15.6e).

#### Reprocess

`reprocess` splits whole items again, from the start of the item as landed, under the version running now
(STAGES.md §15.8.4): `--item-id <id>[,<id>...]`, `--made-by <version>` (the items whose current generation was cut
under that version, which `status --item` shows), or `--landed-from <time> --landed-to <time>` (the items landed in
that range, ISO-8601 with the offset, such as `2026-11-09T00:00:00Z`, the end excluded; `--intake`, if given, must be
the stage's own). `--item-id` narrows the other two. `--seq` is refused: one record is reprocessed at the convert
stage. `--dry-run` lists what would be chosen and records nothing. Otherwise the reason is read from standard input,
as for `decline`, kept encrypted, and the run's ID printed:

```sh
kubectl exec -i <pod> -- java @/app/jvm.options @/app/container.options @/app/admin.args reprocess --landed-from 2026-11-01T00:00:00Z --landed-to 2026-11-02T00:00:00Z --operator alice < reason.txt
```

Within a renewal interval the pods queue the run's items, a batch at a time, each as its next generation (`g2`,
`g3`, …), and send each as `item.reprocess` to the stage's own input (`sdh.stage.reprocess`, the same queue as
`sdh.stage.input`), sealed to and signed by the stage itself; the stage takes `item.reprocess` only when signed by its
own key, and `item.landed` only when signed by another. An item is refused if it is past the generation chosen
(`superseded`), no longer in landing (`past_retention`), or has a `release` waiting (`release_pending`).

The new generation is cut from the start of the item. Its records are stored with `cause` `reprocess` and the run's
ID, sent on as `record.split` with their generation and cause, and its end as `item.split` of that generation; the
failures of the generations before are superseded. An item still being split when its next generation is queued is
stopped after the batch under way (`INTERRUPTED`, `split.generation_interrupted`, counted by
`sdh.split.generations_interrupted`), its end sent with `interrupted: true`, and then cut again; a `HELD` item is
interrupted the same way, with no `release`. The convert stage counts an interrupted generation's records but sends
nothing for it, and the producer hears of the item once, at its first report there, whatever its generation. A
whole-item correction pending for an item chosen is warned of: of the two, the one processed after supersedes the
other. `reprocess --status --run <id>` lists the run item by item (queued, refused and why, stored, failed or
interrupted); `reprocess --cancel --run <id>` cancels what isn't yet queued. Audited as `admin.reprocess`, then
`stage.reprocess_queued`, `stage.reprocess_refused`, `stage.generation_started`, `stage.reprocessed` and
`stage.reprocess_done`, and counted by `sdh.stage.reprocessed`. Without `sdh.stage.reprocess` and the stage's own
public keys the pods queue nothing, and the runs wait. An item landed but never split (its `item.landed` lost, and
the reconciler not yet past its grace) is chosen too, by `--item-id` or by landing time, and split as generation 2;
should its `item.landed` come after, it cuts nothing.

#### Held items

An item part way through splitting when the stage's version changed is held (`HELD`, `split.version_held`): pods
of the new version neither resume nor reconcile it. `status --item <id>` shows its checkpoint and version.
`release` ends the hold, with one of:

```sh
kubectl exec -i <pod> -- java @/app/jvm.options @/app/container.options @/app/admin.args release --item-id <id> --resume --operator alice
kubectl exec -i <pod> -- java @/app/jvm.options @/app/container.options @/app/admin.args release --item-id <id> --close --operator alice < reason.txt
```

`--resume` splits the rest of it under the version running now, so its records are made under two versions, which
its `item.split` names. `--close` ends it unfinished (`CLOSED`), with a reason, kept as a decline's: nothing more is
sent, the convert stage keeps it incomplete, and you land it again with landing's `resubmit`. Either is a request a
pod applies within a renewal interval, under the item's lease (`split.released` or `split.item_closed`); one for an
item that is no longer held is refused (`split.release_refused`). `status --item` lists the requests and what each
came to, without the reasons.


### Troubleshooting

A shell-less container can't be entered with `kubectl exec -- sh`. Attach a debug container instead (kubectl 1.27 or
later):

```sh
kubectl debug -it <pod> --image=busybox --target=file-connector --profile=restricted
```

`--profile=restricted` lets it start in a namespace that enforces the restricted Pod Security Standard. It runs as
the connector's user and shares its processes, so the connector's files are under `/proc/1/root/`, for example
`/proc/1/root/tmp`. The debug container stays in the pod's spec, stopped, until the pod is replaced.

## Licenses

`legal/THIRD-PARTY.txt` lists every third-party component in `app.jar` with its licenses, copyright and notices.
`legal/licenses/` holds the license texts. Where a component is offered under a choice of licenses, all of them are
listed, and its "Elected license" line names the one Smile Digital Health distributes it under. Permissive licenses
(MIT, Apache, BSD, ISC, Creative Commons) are elected over any GNU General Public License or similar license.
