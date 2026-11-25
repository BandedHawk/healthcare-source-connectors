# Platform hardening baseline

For the platform administrators who run the cluster, the database, the broker and the object store a pipeline uses,
and the security official who reviews them. It covers what the product's images and kits already harden, then what
the customer's platform must add: the cluster, its nodes, and the platform services. It is HIPAA.md §11.2's document
D4. Why each memory control matters is HIPAA-OPERATIONS.md §2.9; the network is [NETWORK.md](NETWORK.md); who may
reach the cluster and its pods is HIPAA-OPERATIONS.md §2.4 and [IDENTITY.md](IDENTITY.md) §3.3; the stages' own
settings are [CONFIGURATION.md](CONFIGURATION.md).

It describes a pipeline deployed from `deploy/reference/` and the kits' READMEs, with the `hipaa` compliance profile
on, one pipeline to a namespace (HIPAA-OPERATIONS.md §2.8). What the product ships is tested (§1); what the platform
adds is guidance, tried where §6 says.

## 1. What the product ships

**The images** (DESIGN.md §14.1, §14.3 to §14.5):

- The customer images are built on `gcr.io/distroless/java25-debian13:nonroot`, pinned by digest in
  `docker/base-images.env`: no shell, no package manager, uid and gid 10001.
- Everything under `/app` belongs to root and is read only. The image needs a writable `/tmp` and nothing else,
  and `/tmp` holds only what is already encrypted.
- Each release is checked before it is signed: its base images must be current and its SBOMs must pass the
  vulnerability scan.
- Images are signed by digest with the release key (cosign, ECDSA P-256). Each image's CycloneDX SBOM is attached
  as a signed attestation, and the tarballs carry detached signatures.

**The JVM** reads `config/jvm.options` (`/app/jvm.options` in the image) after `JDK_JAVA_OPTIONS` and
`JAVA_TOOL_OPTIONS`, so tuning can be added but these can't be undone. The `jvm-dumps` compliance check refuses
to start a JVM that runs without the first four (CONFIGURATION.md §1):

| Flag | What it keeps from happening |
|---|---|
| `-XX:-HeapDumpOnOutOfMemoryError` | a heap dump, with the PHI being processed, written on running out of memory |
| `-XX:-CreateCoredumpOnCrash` | a core dump of the process on a crash |
| `-XX:ErrorFile=/dev/null` | an `hs_err` file, which holds raw memory near the crash |
| `-XX:+DisableAttachMechanism` | a tool joining the process (`jcmd`), and so a heap dump on demand |
| `-XX:+ExitOnOutOfMemoryError` | a process running on in an unknown state: it exits, and is restarted clean |
| `-XX:-UsePerfData` | an `hsperfdata` file in `/tmp` |
| `--enable-native-access=ALL-UNNAMED` | (no control: it stops Java 25's warning when libraries load native code) |

**The pods**, as every stage's and every control API's Deployment in the kits' READMEs has them. Each meets the
restricted Pod Security Standard, and the AWS PoC and the sandbox run them in namespaces that enforce it:

| Setting | Value |
|---|---|
| `runAsNonRoot` | `true`, with `runAsUser` and `runAsGroup` 10001 |
| `seccompProfile` | `RuntimeDefault` |
| `allowPrivilegeEscalation` | `false` |
| `capabilities` | `drop: [ALL]` |
| `readOnlyRootFilesystem` | `true` |
| memory limit | set, with the heap at half of it (`-XX:MaxRAMPercentage=50`) |
| volumes | `emptyDir` for `/tmp`, a Secret for the keys (mode 0440), a ConfigMap for the settings, and the landing node's intake claim: no `hostPath`, no host network, PID or IPC |

**The stages themselves:** no PHI in logs, metrics, traces or audit events. The `hipaa` profile refuses
plaintext transports, local keystores and object stores without versioning (CONFIGURATION.md §1).

## 2. The cluster

### 2.1 Pod Security

Label every pipeline namespace so that the API server refuses any pod short of the restricted standard:

```yaml
metadata:
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/enforce-version: latest
```

These are the labels in `deploy/aws/k8s/pipeline/namespace.yaml` and `deploy/sandbox/pipeline/namespace.yaml`. The
point is less the stages, which already comply (§1), than what else reaches the namespace: a privileged, `hostPID`
or `hostPath` pod on the same node can read another process's memory, PHI included (HIPAA-OPERATIONS.md §2.9). Under
restricted, `kubectl debug` works only with `--profile=restricted`, as the kits' READMEs give it (DESIGN.md §14.3).
A platform namespace for Postgres and Kafka may need `baseline` for their images, as on the PoC
(`deploy/aws/k8s/infra/namespace.yaml`): keep `warn: restricted` there, and no PHI-bearing pod.

### 2.2 Secrets

The keys' Secret holds a stage's inbox private key and passphrases (KEYS.md §3), and the Kafka and Postgres
passwords sit beside it. So:

- **Encrypt Secrets at rest** in the cluster's store with a KMS key: EKS's `encryption_config` for `secrets`, as
  `deploy/aws/terraform/compute/main.tf` has it, or the provider's equivalent (GKE application-layer secrets
  encryption, AKS KMS etcd encryption). On a self-managed cluster, an `EncryptionConfiguration` with a KMS
  provider, never `identity` or `aescbc` with a key on the control plane's disk.
- **Or keep them out of the cluster:** an external secret store (AWS Secrets Manager, Vault) through the Secrets
  Store CSI driver, mounted at the same `/run/secrets` paths. The stages read files, so nothing changes for them;
  this has not been tried (§6).
- `get`, `list` and `watch` on Secrets in a pipeline namespace go to the platform's deploy identity only, never to
  people by default (HIPAA-OPERATIONS.md §2.4). `list` gives the values, not just the names.

### 2.3 The API server's audit log

The API server's audit log is the only record of `exec`, `attach`, `port-forward` and `debug`, the realistic way
anyone reads a running stage's memory (HIPAA-OPERATIONS.md §2.9), and of who read a Secret. Turn it on and send it
to the SIEM:

- **On EKS:** the `audit` control-plane log type (`enabled_cluster_log_types`), to CloudWatch Logs and on from
  there. The policy is AWS's own and can't be changed: check that its entries show these requests.
- **On GKE and AKS:** Cloud Audit Logs' admin activity and data access logs, or AKS's `kube-audit` diagnostic
  setting.
- **On a self-managed cluster:** an audit policy that records at least `Metadata` for Secrets and `Request` for
  `pods/exec`, `pods/attach`, `pods/portforward` and `pods/ephemeralcontainers`.

Then alert on those requests in pipeline namespaces, and review them with the daily checks (HIPAA-OPERATIONS.md
§3.1). The AWS PoC has control-plane logs off, to save their cost (§6).

### 2.4 Signed images only

Every image is signed by digest and carries a signed SBOM (§1). An admission policy that requires both in the
pipeline namespaces keeps out an image swapped in the registry or pushed by mistake:

- Sigstore's policy-controller (`ClusterImagePolicy`) or Kyverno (`verifyImages`), with the release's public key
  `cosign.pub` as the authority.
- Signatures aren't in a public transparency log (DESIGN.md §14.4), so the policy must not require one.
- Deploy by digest, never by tag, as `image-digests.txt` lists them, so what was verified is what runs.
- Scope the policy to the product's images (`<registry>/sdh/*`), and give the platform's own images a policy of
  their own.

Neither has been tried with the product's images (§6).

### 2.5 What a stage's pod may do in the cluster

- **No Kubernetes API token.** No stage calls the Kubernetes API. Set `automountServiceAccountToken: false` on the
  stages' service accounts, and give them no Role. EKS Pod Identity and the other workload identities inject
  credentials of their own; this has not been tried with them yet (§6).
- **No instance metadata.** On AWS, the nodes' IMDSv2 with a hop limit of 1 keeps pods from the node's role. The
  reference network policies already allow no egress to `169.254.169.254` (NETWORK.md §4), and the stages use Pod
  Identity on `169.254.170.23`.
- **Exec, attach, debug and port-forward** go to named staff only, per namespace (HIPAA-OPERATIONS.md §2.4).

## 3. The nodes

| Control | Why | How |
|---|---|---|
| **A CIS benchmark** for the nodes and the cluster | Kernel and runtime flaws let a container read across processes | The CIS Kubernetes or EKS benchmark, checked with `kube-bench` or the provider's scanner; a hardened node image (Bottlerocket, or the provider's CIS image); patched by routine node rotation |
| **No swap**, or encrypted swap | Memory paged out to a disk | Kubernetes nodes run without swap by default: keep it so |
| **No kernel crash dumps** (`kdump` off), or dumps only to an encrypted, access-controlled volume | The whole node's memory written out on a kernel crash | The node image's settings |
| **Encrypted volumes:** root disks, and the volumes of Postgres and Kafka | Anything that reaches a disk all the same | EBS encryption with a KMS key (the account's default, or a launch template that sets it); the provider's equivalent elsewhere |
| **No interactive access** | Root on a node reads every process on it | No SSH keys; Systems Manager sessions for named staff only, logged |
| **Dedicated nodes** for the pipeline | Another tenant's compromised pod on the same kernel | Taints and node selectors, or a node group of its own |

Confidential computing nodes (AMD SEV-SNP, Intel TDX) are an option where the threat model includes the cloud
provider's staff; HIPAA-OPERATIONS.md §2.9 discusses them. They have not been tried.

## 4. The platform services

### 4.1 Postgres

It holds the registries, the landing ledger, the failures and the audit journal: no PHI in the clear, but the
journal is the evidence of who did what (D7).

- **At rest:** the data and WAL on encrypted volumes, or a managed service with storage encryption on (RDS,
  Cloud SQL, Azure Database). The base backups and WAL archive in a bucket that is encrypted too
  (HIPAA-OPERATIONS.md §7).
- **In transit:** TLS only, `ssl_min_protocol_version=TLSv1.2`, `hostssl` lines only in `pg_hba.conf`. The
  stages connect with `sslmode=verify-full` (CONFIGURATION.md §2), through PgBouncer if there is one, which
  verifies the server in turn (`server_tls_sslmode = verify-full`).
- **Roles:** each pipeline's own database and roles, from `deploy/reference/postgres.sql`. No stage role is a
  superuser or owns another pipeline's objects.
- **Logging:** `log_connections` and `log_disconnections` on, and role and DDL changes logged
  (`log_statement = ddl`, or pgAudit's `role` and `ddl` classes), to the SIEM. Never `log_statement = all` or
  `mod`: the stages' statements hold no PHI, but such a log is large and its parameters are not reviewed for
  it. On PgBouncer, `log_connections = 1`. The PoC has PgBouncer's connection logging off (§6).

### 4.2 Kafka

It carries the queue messages: signed, and encrypted to their recipient where they carry PHI (DATA-FLOWS.md).

- **At rest:** the log directories on encrypted volumes, or a managed broker's encryption at rest (MSK encrypts
  with a KMS key always).
- **In transit and access:** HIPAA-OPERATIONS.md §2.6, "Securing the broker": no plaintext listener for clients,
  TLS and principals of their own between brokers, `allow.everyone.if.no.acl.found=false`, and each stage only
  the rights of `deploy/reference/kafka-acls.sh`.
- **Logging:** the authorizer's log (`kafka.authorizer.logger`), with denials at least, to the SIEM, and every ACL
  and user change.

### 4.3 The object store

It holds bronze, silver and gold: encrypted by the stages before they write (DESIGN.md §7), so the store's own
encryption is a second layer.

- **Encryption at rest:** SSE-KMS with a key of the platform's (SSE-S3 at least), as on the PoC
  (`deploy/aws/terraform/persistent/main.tf`).
- **Versioning on** for every bucket. The stages refuse to start without it (`store-versioning`,
  CONFIGURATION.md §1), and noncurrent versions are kept for at least the database's recovery window
  (HIPAA-OPERATIONS.md §7).
- **Object Lock** in compliance mode for the backup buckets, and for the data buckets where the records policy
  allows it. A lock is also a limit on erasure: an erased item stays in a locked version until its retention
  ends, so set the lock no longer than the records policy keeps data. Not tried with the product (§6).
- **Refuse plaintext** (a bucket policy denying `aws:SecureTransport` false) and public access (Block Public
  Access on, at the account level).
- **Logging:** CloudTrail data events on the buckets (or server access logs), to the SIEM.

### 4.4 The KMS

KEYS.md has the keys, their policies and rotation. On the platform's side: automatic rotation on, key policies
that admit each stage's role only from the pipeline's network, deletion only by named administrators with a
waiting period, and every use in CloudTrail, to the SIEM.

## 5. Checking a deployment

- [ ] Every pipeline namespace enforces `restricted` at `latest`; `kubectl label --dry-run=server` shows no pod in
      it that would be refused.
- [ ] Every stage and control image is deployed by digest, and the admission policy refuses one not signed with the
      release key.
- [ ] Secrets are encrypted with a KMS key at rest (or come from an external store), and only the deploy identity
      may read them.
- [ ] The API server's audit log reaches the SIEM, with an alert on `exec`, `attach`, `port-forward` and ephemeral
      containers in pipeline namespaces.
- [ ] The stages' service accounts mount no token and have no Role.
- [ ] The nodes pass the CIS benchmark, run without swap or kernel crash dumps, have encrypted disks and no SSH.
- [ ] Postgres, Kafka and the buckets are encrypted at rest. Postgres and Kafka take TLS only, and log
      connections, role changes and ACL changes to the SIEM.
- [ ] Every bucket keeps versions, refuses plaintext and public access, and has CloudTrail data events on.
- [ ] The KMS keys rotate, and their use reaches the SIEM.

## 6. Findings

- **The kits' Deployments don't turn off the service account token.** No stage needs one, but none sets
  `automountServiceAccountToken: false`, and it hasn't been tried with EKS Pod Identity.
- **No signed-image admission has been tried.** The images and SBOMs are signed and verified by
  `sign-release.sh`, but no cluster has been run with a policy that requires them.
- **The AWS PoC has the API server's audit log off** (`enabled_cluster_log_types = []`), and PgBouncer's
  connection logging (`log_connections = 0`). It holds no PHI (HIPAA.md §8.5).
- **Whether the PoC's node root disks are encrypted is unchecked:** its node groups set no launch template, so it
  depends on the account's EBS default, which the deployer role may not read. The volumes of Postgres and Kafka
  are encrypted with the infrastructure key.
- **No node has been checked against a CIS benchmark,** and no hardened node image (Bottlerocket) tried. The PoC's
  nodes run Amazon Linux 2023.
- **Neither an external secret store nor Object Lock has been tried** with the product.

## Sources

| Statement | Source |
|---|---|
| The images: base, user, signing, SBOMs, scanning | DESIGN.md §14.1, §14.3, §14.4, §14.5; `docker/base-images.env`; `scripts/sign-release.sh` |
| The JVM flags | each kit's `src/dist/config/jvm.options`; the `jvm-dumps` check, DESIGN.md §14.1 and the compliance rules |
| The pods' settings | the Deployments in `landing-connector`, `file-connector` and `convert-stage`'s `src/dist/README.md`, and the transform stage's in its own repository |
| The namespaces' Pod Security labels | `deploy/aws/k8s/pipeline/namespace.yaml`, `deploy/aws/k8s/infra/namespace.yaml`, `deploy/sandbox/pipeline/namespace.yaml` |
| Secrets encryption and control-plane logs on the PoC | `deploy/aws/terraform/compute/main.tf` (`encryption_config`, `enabled_cluster_log_types`) |
| The PoC's volumes and buckets | `deploy/aws/terraform/persistent/main.tf` (`aws_ebs_volume.state`, the buckets' encryption and versioning) |
| PgBouncer's and Postgres's TLS and logging on the PoC | `deploy/aws/k8s/infra/state.yaml` |
| The reference database roles and broker rights | `deploy/reference/postgres.sql`, `deploy/reference/kafka-acls.sh` |
| Checked by | `HardeningGuideTest` (landing-connector): every kit's JVM flags are in §1, and every Deployment in the kits' READMEs has §1's pod settings |
