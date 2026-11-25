# Reference settings: one pipeline, HIPAA profile on

A starting point for a deployment that holds PHI (HIPAA.md §8.1 item 13). Each stage runs under
`sdh.compliance.profile: hipaa` with **nothing waived**, and every transport is secured:

| Transport | Here | Tried by |
|---|---|---|
| Kafka | `SASL_SSL`: TLS, the broker's CA in `/run/secrets/kafka-ca.crt`, a SCRAM-SHA-512 login per stage from the environment variable `SDH_KAFKA_JAAS` (`RAW({{env:SDH_KAFKA_JAAS}})` in each URI, so the login never sits in a file) | connector-broker `KafkaSecurityIT` |
| Postgres | `sslmode=verify-full`, the server's CA in `/run/secrets/postgres-ca.crt`; the pipeline's own database | file-connector `PostgresTlsIT`, `PipelineIsolationIT` |
| Keys | AWS KMS for every KEK and signing key | file-connector `AwsKmsIT` |
| Object store | S3 through the pod's role, with a bucket policy refusing plaintext requests (AWS-POC.md §4). Versioning on for every bucket: each stage asks, and refuses to start without it, so its role needs `s3:GetBucketVersioning` (DEPLOYMENT.md, "Object store versions") | file-connector `PipelineIsolationIT` (a bucket set per pipeline); `StoreVersioningTest` |
| Audit export | OTLP to the operator's collector inside the cluster; onward with TLS | HIPAA-OPERATIONS.md §2.7 |

`ReferenceConfigurationTest` (connector-runtime) runs each file through the profile as the node does at startup,
with its image's JVM options, and expects nothing refused or waived. It doesn't start the stages. The AWS PoC's
harness pipeline is deployed from these files (`deploy/aws/k8s/harness/apply.sh`, AWS-POC.md §8.2), with the
changes it names, but has not been run yet (HIPAA.md §11.4, A1). Since DESIGN.md revision (fk) the PoC's own
performance pipelines follow them too (the hipaa profile, Kafka over SASL_SSL, Postgres through PgBouncer with
`verify-full`), which the same test checks.

## Files

- `landing-application.yml`, `splitter-application.yml`, `convert-application.yml`, `transform-application.yml`:
  each stage's `application.yml`, for its ConfigMap. Each follows its AWS PoC counterpart in
  `deploy/aws/k8s/pipeline/` with the PoC's tuning left out. Add tuning back from there, or from
  PERFORMANCE.md, as load needs.
- `postgres.sql`: the pipeline's database, its catalog role, and a login role per stage that acts as it. Only the
  pipeline's stages may connect.
- `kafka-acls.sh`: each stage's rights on the broker (DEPLOYMENT.md, "Broker rights"), for the broker team to apply.
- `network-policies.yaml`: the pipeline namespace's network policies, default-deny and then only what a stage needs
  (security/NETWORK.md §4).

The Deployments are those in each stage's distribution README, as the sandbox and AWS use them. They mount
the settings at `/config` and the keys at `/run/secrets`; put the CA certificates in the keys' Secret.

## Filling them in

`${...}` in capitals are the deployment's own: the pipeline's name, the broker and database hosts, the buckets, the
region, and the key fingerprints. Fill them in, or leave them and set each as an environment variable: Spring
resolves them at startup. The secrets come from each stage's Kubernetes Secret, never from these files:

- `SDH_LEASE_JDBC_PASSWORD`: the stage's database password.
- `SDH_KAFKA_JAAS`: the stage's Kafka login, for example
  `org.apache.kafka.common.security.scram.ScramLoginModule required username="splitter-claims" password="...";`.

## Another cloud, or none

Change the lines the AWS PoC's own notes name, and nothing in the security settings:

- **Keys:** `wrap-provider` and `sign-provider` to `gcp-kms`, `azure-keyvault` or `vault-transit`, with that
  provider's key references and settings (each stage's README, "Keys").
- **Object store:** `io-impl` and its client settings, for GCS, ADLS or an S3-compatible store over `https://`.
- **Broker:** another broker's URI with its TLS options. The profile names what it requires of each
  (DESIGN.md §7.6).

## Isolation

One pipeline per customer, hybrid-entity component or Part 2 program: its own database, buckets, KEKs and signing
keys, broker identities and topics, and namespace. HIPAA-OPERATIONS.md §2.8 describes the layout. These files are
that layout for one pipeline, named by `${PIPELINE}`.
