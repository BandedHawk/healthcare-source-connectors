# File Connector

Copyright (c) 2026 Smile Digital Health Inc. All rights reserved. See `legal/LICENSE`.

The file connector watches an inbound directory, splits each healthcare file (C-CDA, X12, EDIFACT, FHIR, CSV and
others) into sections, stores every section encrypted in Apache Iceberg tables, and announces each one on a message
queue with a signed, encrypted message.

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
cosign verify --key cosign.pub --insecure-ignore-tlog=true <registry>/file-connector@sha256:<digest>
cosign verify-attestation --key cosign.pub --insecure-ignore-tlog=true --type cyclonedx \
  <registry>/file-connector@sha256:<digest>
```

Signatures aren't recorded in Sigstore's public transparency log, so `--insecure-ignore-tlog=true` is expected; the
public key is what you trust. A Kubernetes admission policy (for example Sigstore policy-controller or Kyverno) can
require the signature before the image runs.

## Requirements

- Linux on x86-64 or ARM64, with glibc.
- Java 25.
- PostgreSQL, for the single-instance lease; it can be the JDBC catalog's database.
- An Iceberg catalog: REST (for example Apache Polaris) or JDBC.
- Object storage: Amazon S3, Google Cloud Storage or Azure Data Lake Storage Gen2. Of the S3-compatible stores,
  SeaweedFS is tested; MinIO is not supported.
- A message broker: Kafka, Amazon SQS, Google Pub/Sub, Azure Service Bus or Event Hubs, ActiveMQ Artemis or RabbitMQ.
- Keys: a local keystore and PEM files, or a KMS (AWS KMS, Azure Key Vault or HashiCorp Vault Transit). Google
  Cloud KMS is available but untested against the live service; contact Smile Digital Health before relying on
  it in production.

Run exactly one connector per inbound directory. A second instance is refused at startup, after waiting up to
70 seconds (the lease's TTL plus its safety margin) in case the first one has crashed. A connector restarted after a
crash waits the same way and takes over once the old lease lapses.

## Configure

Copy `config/application.yml.example` to `config/application.yml` and edit it. Pass secrets as environment
variables, never in the file:
- `SDH_CRYPTO_KEYSTORE_PASSWORD`
- `SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE`
- `SDH_LEASE_JDBC_PASSWORD`
- catalog and storage credentials

Any setting can also be given as an environment variable (`sdh.lease.jdbc.url` becomes `SDH_LEASE_JDBC_URL`) or as an argument (`--sdh.lease.jdbc.url=…`).

## Run on a host

```sh
bin/file-connector                         # Java 25 on the PATH, or JAVA=/path/to/java
JAVA_OPTS=-Xmx2g bin/file-connector        # extra JVM options
SDH_OPERATOR=alice bin/sdh-admin status    # operator commands: status, restore, decrypt-fallback
```

Run it under a service manager (for example systemd) that restarts it when it exits. The connector exits with
status 70 on conditions it can't safely continue from, and resumes where it stopped.

## Build a container image

From this directory:

```sh
docker build -f container/Dockerfile -t <registry>/file-connector:<version> .
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

The connector needs:
- **`/data/inbound`:** a writable volume, shared with whatever delivers files.
- **`/tmp`:** a writable `emptyDir` of about 1 GiB; `noexec` is fine. Uploads to object storage are staged here, with section data already encrypted.
- **`/run/secrets`:** the keys, as a read-only secret volume (local key mode only).
- **Configuration:** a ConfigMap mounted as `application.yml`, with `SPRING_CONFIG_ADDITIONAL_LOCATION` pointing at it.
- **Secrets:** a Secret whose keys are environment variable names, passed whole with `envFrom`. It holds
  `SDH_CRYPTO_KEYSTORE_PASSWORD`, `SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE` (both local key mode only),
  `SDH_LEASE_JDBC_PASSWORD`, and whatever `application.yml` refers to as `${...}`, such as catalog and storage
  credentials.

```sh
kubectl create secret generic file-connector-keys --from-file=keks.p12 --from-file=signing.pem \
  --from-file=recipient.pem --from-file=recovery-public.pem
kubectl create secret generic file-connector-env --from-literal=SDH_CRYPTO_KEYSTORE_PASSWORD=… \
  --from-literal=SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE=… --from-literal=SDH_LEASE_JDBC_PASSWORD=…
kubectl create configmap file-connector-config --from-file=application.yml
```

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: file-connector }
spec:
  replicas: 1                          # one connector per inbound directory
  strategy: { type: Recreate }
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
          image: <registry>/file-connector:<version>
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
            - { name: inbound, mountPath: /data/inbound }
            - { name: tmp, mountPath: /tmp }
            - { name: keys, mountPath: /run/secrets, readOnly: true }
            - { name: config, mountPath: /config, readOnly: true }
      volumes:
        - { name: inbound, persistentVolumeClaim: { claimName: file-connector-inbound } }
        - { name: tmp, emptyDir: { sizeLimit: 1Gi } }
        - { name: keys, secret: { secretName: file-connector-keys, defaultMode: 0440 } }
        - { name: config, configMap: { name: file-connector-config } }
```

There is no HTTP health endpoint. The process is the health signal: it exits on conditions it can't safely continue
from, and Kubernetes restarts it.

### Operator commands

The image has no shell, so run the JVM directly:

```sh
kubectl exec <pod> -- java @/app/jvm.options @/app/container.options @/app/admin.args status --operator alice
```

`--operator` names who is running the command, for the audit log; on a host, `$SDH_OPERATOR` does the same.
`restore` writes to the inbound volume with `--to /data/inbound/failed` (never picked up); take the file from there
through whatever delivers files. `kubectl cp` can't copy out of this image, because it needs `tar`.

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
