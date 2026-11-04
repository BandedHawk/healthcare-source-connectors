# Landing Connector

Copyright (c) 2026 Smile Digital Health Inc. All rights reserved. See `legal/LICENSE`.

A landing node picks up healthcare data from one intake (a directory), stores each item exactly as received,
encrypted, in Apache Iceberg tables, and announces it to the file connector on a message queue with a signed,
encrypted `item.landed` message. The file connector then splits it. Landed items are never changed.

This distribution contains no Java runtime. Run it on a host with Java 25, or build a container image from it
on a Java 25 base image of your choice.

## Contents

| Path | What it is |
|---|---|
| `app.jar` | The application |
| `bin/landing-connector` | Start the node on a host |
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
- catalog and storage credentials

Any setting can also be given as an environment variable (`sdh.lease.jdbc.url` becomes `SDH_LEASE_JDBC_URL`) or as
an argument (`--sdh.lease.jdbc.url=…`).

The source is read-only by default: files are left where they are, and a ledger keeps them from being landed twice.
With `access: read-write`, each file is deleted, moved or renamed once it has landed and been announced; the node
checks at startup that it can do so.

The source is a local or mounted directory (`type: directory`), an SFTP server (`type: sftp`) or an SMB share
(`type: smb`):
- **SFTP:** the server's host key must be pinned in a known-hosts file; an unknown or changed key is never
  accepted. Log in with a private key in OpenSSH format, or a password.
- **SMB:** SMB 3 with encryption only, NTLMv2 with a user and password; guest and anonymous logins are refused, and
  so is a share that doesn't encrypt.
- **Paths** on a remote source are absolute paths on the server (`path`, `move-to`).
- **Outages:** a dropped or stalled connection is noticed within `read-timeout` and retried at each poll; an item
  cut off part way resumes where it stopped. Refused credentials, a missing share or a changed host key are retried
  only every `auth-retry-interval`, so an account isn't locked. Each is reported once, by reason, as
  `intake.unreachable`, and `intake.reachable` when it is back. No names or paths are logged.

Landed items are kept forever unless you set `sdh.maintenance.retention.landed-items` (at least `7d`, so the
splitter has time to read each item). Retention removes whole days, and never part of an item. The ledger is always
kept, because a read-only source would otherwise land its files again. Set the retention longer than the splitter
can ever fall behind: an item purged before the splitter reads it is reported there as failed.

## Run on a host

```sh
bin/landing-connector                      # Java 25 on the PATH, or JAVA=/path/to/java
JAVA_OPTS=-Xmx1g bin/landing-connector     # extra JVM options
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
