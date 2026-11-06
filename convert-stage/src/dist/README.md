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
70 on conditions it can't safely continue from, and resumes where it stopped.

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

### Operator commands

The image has no shell, so run the JVM directly:

```sh
kubectl exec <pod> -- java @/app/jvm.options @/app/container.options @/app/admin.args status --operator alice
```

`--operator` names who is running the command, for the audit log; on a host, `$SDH_OPERATOR` does the same.
`status` shows the duties lease, the items not yet complete and the records claimed now; `status --item <id>` shows
one item and its records by state. It reads only the lease database.

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
