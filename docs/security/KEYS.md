# Key management

For the organisation that operates a pipeline: every key the product uses, who makes it, where it is kept, who may
use it, and how it is rotated, replaced after a compromise and destroyed. It is HIPAA.md §11.2's document D3, for
the encryption specifications (§ 164.312(a)(2)(iv), § 164.312(e)(2)(ii)) and the key custodians' procedures. What
each key opens is [DATA-FLOWS.md](DATA-FLOWS.md) §4; the settings that name them are
[CONFIGURATION.md](CONFIGURATION.md) §4; the steps of a KEK rotation and a re-key are HIPAA-OPERATIONS.md §6.1.

It describes a pipeline deployed from `deploy/reference/` (AWS KMS, one pipeline) with the `hipaa` compliance
profile on. The profile refuses keys kept beside the stage instead of in a KMS (`keys-local`), so every KEK and
stage signing key below is in a KMS unless that check is waived. The other KMSs the product supports (Google Cloud
KMS, Azure Key Vault, Vault Transit; DESIGN.md §7.4) take the same keys with their own names for the operations.

## 1. The hierarchy

```
 KMS (the roots: never leave it)
 ├── landed-items KEK ─wraps─► a batch data key per commit ─derives─► a key per record ─► landed_items chunks
 ├── records KEK ──────wraps─► batch data keys ─► split_records and records chunks
 │                     wraps─► each stage's ID key, its failures, corrections, reprocess runs, held results
 ├── fhir KEK ─────────wraps─► batch data keys ─► gold chunks
 └── a signing key per stage (ES256)
        └─signs─► a statement for each pod's own signing key (an hour) ─signs─► messages, audit seals

 Key pairs the customer makes (private halves never in the KMS)
 ├── each stage's inbox key pair ◄── the stage before seals its messages to the public half
 ├── gold readers' key pair ◄────── the transform stage seals its messages to the public half
 ├── each operator's key pair ◄──── PHI results (restore, export, the patient lookup) are sealed to it
 └── the producer's key pair ◄───── results to the producer are sealed to it, if results are on

 Keys the product makes and never stores in plain
 ├── batch data keys and per-record keys (stored wrapped, beside the chunks)
 ├── ID keys (stored wrapped, in a table property)
 ├── pod signing keys (memory only)
 ├── an ephemeral key agreement per 10,000 messages or 60 s (memory only)
 └── a data key per held break-glass result (stored wrapped, deleted when fetched or after an hour)
```

Everything stored is under a KEK: destroying a pipeline's KEKs makes all it stored unreadable, backups included
(§7). Everything sent between stages is sealed to a key pair and signed under a stage's signing key.

## 2. Each key

| Key | Kind | Made by | Kept in | Used by | Setting |
|---|---|---|---|---|---|
| **Landed-items KEK** | symmetric (AES-256 in the KMS) | key custodians | KMS | landing wraps and unwraps; the split stage unwraps | `sdh.crypto.keys.landed-items-kek` |
| **Records KEK** | symmetric | key custodians | KMS | split and convert wrap and unwrap; transform unwraps | `sdh.crypto.keys.records-kek` |
| **FHIR KEK** | symmetric | key custodians | KMS | transform wraps and unwraps; gold readers unwrap | `sdh.stage.sink.kek` (transform) |
| **A stage's signing key** | EC P-256, sign only | key custodians | KMS | that stage alone signs; the next stage, gold readers and `audit-verify` check with its public half | `sdh.crypto.keys.signing` (`kid`, `key-ref`) |
| **A pod's signing key** | EC P-256 | each pod, at startup | the pod's memory | the pod signs messages and audit seals, under a statement its stage's key signs | `sdh.crypto.pod-keys` |
| **A stage's inbox key pair** | EC P-256 | platform administrator | the stage's Kubernetes Secret, the private half encrypted with a passphrase | the stage opens what is sealed to it; the stage before seals to the public half | `sdh.crypto.keys.inbox`; the stage before's `sdh.crypto.keys.recipient` |
| **The stage before's signing public key** | EC P-256, public | exported from the KMS once | the stage's Secret | the stage checks every message it opens | `sdh.crypto.keys.landing-signer` (named for the first stage it served; every stage after landing has one) |
| **Gold readers' key pair** | EC P-256 or RSA | the data consumer owner | the readers' own secret store | readers open the transform stage's messages | the transform stage's `recipient`; the reader's `recipientKey` |
| **An operator's key pair** | EC P-256 or P-384, or RSA ≥ 3072 bits | the operator, on a managed workstation | the private half on the workstation alone | the stage seals PHI results to the public half | registered in `sdh_operator_keys` (`/operator-keys`); the CLI reads it from standard input |
| **The producer's key pair** | EC P-256 or RSA | the producer | the producer | the landing and convert stages seal results to it | `sdh.results.recipient` |
| **Batch data key** | AES-256, random | the stage, per commit batch | wrapped by the KEK, in each chunk row (`bdk_wrapped`, `kek_ref`) | per-record keys are derived from it (HKDF-SHA256) | — |
| **ID key** | 256-bit HMAC secret, random | the stage, at its first start | wrapped by the stage's KEK, in the output table's property `sdh.id-key` | record IDs, keyed hashes (the landing ledger) | — |
| **Ephemeral agreement key** | EC P-256 | the sealing stage | memory | one ECDH-ES agreement for up to 10,000 messages or 60 s | `sdh.crypto.jose.agreement-messages`, `-lifetime` |
| **Held-result data key** | AES-256 | the control API, per break-glass result | wrapped by the stage's KEK, in `sdh_control_results` | the requester's one fetch, within an hour | — |

Each stage's own KEK, the one it wraps new data under and the one a rotation moves, is its *scope's* KEK: the
landing node's landed-items KEK, and for a record stage its sink's (`sdh.stage.sink.kek`, or the records KEK when
unset): the records KEK for split and convert, the FHIR KEK for transform. A KEK the stage only reads with is
another stage's. The current KEK of each scope is in `GET /api/v1/keys`. A KMS reaches any KEK the stage's
identity may use; local keys (refused under the profile) are loaded only if listed, the stage's own and those in
`sdh.crypto.keys.other-keks`: one to rotate to, and those still wrapping its data.

**Not covered here:** TLS keys and certificates (the brokers, Postgres, PgBouncer, the push ingress, the control
API's server certificate and its client CA), database and broker passwords, and the identity provider's token
signing keys. They protect connections and logins, not the stored data: the network is D1 (HIPAA.md §11.2),
identity is [IDENTITY.md](IDENTITY.md), and their settings are CONFIGURATION.md §2, §3 and §6.

## 3. Who holds what

| Role (HIPAA-OPERATIONS.md §1) | Holds | May not |
|---|---|---|
| **Key custodians** (two, named by the security official) | administration of the KMS keys: create, grant, enable rotation, disable, schedule deletion | use any key to encrypt, decrypt or sign |
| **Each stage's workload identity** | use of its own scope's KEK, the KEKs it reads with, and its own signing key | administer any key; use another stage's signing key; reach another pipeline's keys |
| **Platform administrator** | the stages' Secrets: inbox key pairs, their passphrases, the public keys of the stages before | read the KMS keys' material; reach operators' private keys |
| **Key admins** (`keys.admin` on the control API) | rotating and re-keying a stage (re-keying with a second key admin); registering and revoking operators' keys, never their own | change the KMS keys themselves |
| **Data consumer owner** | the gold readers' private key, and their identity's right to unwrap the FHIR KEK | write to the pipeline |
| **Each operator** | their own private key | have it on the cluster |
| **Security official** | the register of operators' key fingerprints against their holders | — |

The product holds no key's administration: it asks the KMS to wrap, unwrap and sign, and never creates, disables or
destroys a KMS key. Two people stand between any one person and the data: a key custodian can't use the keys, a
stage's identity can't change them, and a key admin can't register a sealing key for themselves (CONTROL-API-
DESIGN.md §7).

## 4. Key policies

### 4.1 What each identity needs

The AWS KMS provider makes four calls: `Encrypt` and `Decrypt` with the KEKs, `Sign` and `GetPublicKey` with the
signing keys (`AwsKmsKeyProvider`). Every `Encrypt` and `Decrypt` carries the encryption context
`sdh:purpose=sdh-batch-data-key`, so a policy can require it and refuse the key's use for anything else. Grant
these, per pipeline, and nothing more:

| Identity | Landed-items KEK | Records KEK | FHIR KEK | Signing keys |
|---|---|---|---|---|
| Landing node | `Encrypt`, `Decrypt` | — | — | its own: `Sign`, `GetPublicKey` |
| Split stage | `Decrypt` | `Encrypt`, `Decrypt` | — | its own: `Sign`, `GetPublicKey` |
| Convert stage | — | `Encrypt`, `Decrypt` | — | its own: `Sign`, `GetPublicKey` |
| Transform stage | — | `Decrypt` | `Encrypt`, `Decrypt` | its own: `Sign`, `GetPublicKey` |
| Gold readers | — | — | `Decrypt` | — (they check signatures with the public key they are given) |
| Key custodians | administration only | administration only | administration only | administration only |

- **A rotation adds a key, for a while:** the stage's identity needs `Encrypt` and `Decrypt` on the new KEK, and
  every reader of its tables `Decrypt` on it, before the rotation; the old KEK's grants stay until the re-key is
  done (HIPAA-OPERATIONS.md §6.1).
- **The stages check other stages' signatures with pinned public keys,** exported once from the KMS when the
  pipeline is deployed. No stage needs `GetPublicKey` on another's signing key.
- **`audit-verify` and `audit-export`** read the stage's own signing public key (`GetPublicKey`), run in the stage's
  pod as its identity, and need no KEK.
- **The bucket encryption key** (SSE-KMS, below the product's own encryption) is the platform's: S3 uses it on the
  stages' behalf (`kms:ViaService`), and it protects no more than the object store's own access control does.

### 4.2 A key policy for a KEK

On AWS, name the identities in each key's own policy, so that an IAM policy written later can't widen who uses the
key. For the records KEK, with the roles of one pipeline:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "KeyCustodiansAdministerOnly",
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::<account>:role/<key-custodians>" },
      "Action": ["kms:Describe*", "kms:Get*", "kms:List*", "kms:Create*", "kms:Put*", "kms:Update*",
                 "kms:Enable*", "kms:Disable*", "kms:Revoke*", "kms:TagResource", "kms:UntagResource",
                 "kms:ScheduleKeyDeletion", "kms:CancelKeyDeletion", "kms:RotateKeyOnDemand"],
      "Resource": "*"
    },
    {
      "Sid": "StagesWrapAndUnwrap",
      "Effect": "Allow",
      "Principal": { "AWS": ["arn:aws:iam::<account>:role/sdh-<pipeline>-splitter",
                             "arn:aws:iam::<account>:role/sdh-<pipeline>-convert"] },
      "Action": ["kms:Encrypt", "kms:Decrypt"],
      "Resource": "*",
      "Condition": { "StringEquals": { "kms:EncryptionContext:sdh:purpose": "sdh-batch-data-key" } }
    },
    {
      "Sid": "ReadersUnwrap",
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::<account>:role/sdh-<pipeline>-transform" },
      "Action": "kms:Decrypt",
      "Resource": "*",
      "Condition": { "StringEquals": { "kms:EncryptionContext:sdh:purpose": "sdh-batch-data-key" } }
    }
  ]
}
```

Leave out the account root's `kms:*` statement that AWS's default key policy has, which delegates the key to IAM;
the custodians' role, assumed with MFA, is then the only way to the key's administration, so apply the policy as
that role (AWS refuses a policy that would leave its caller unable to change it) and keep a second custodian able to
assume it. A signing key's policy is the same with its own stage's role
granted `kms:Sign` and `kms:GetPublicKey`, and no condition (signing takes no encryption context).

**Google Cloud KMS:** grant `roles/cloudkms.cryptoKeyEncrypterDecrypter` (or `…Decrypter` alone) on the key, and
`roles/cloudkms.signerVerifier` plus `roles/cloudkms.publicKeyViewer` on a signing key, to each stage's service
account; the custodians get `roles/cloudkms.admin` without those. **Azure Key Vault:** with Azure RBAC, `Key Vault
Crypto User` on each key (wrap, unwrap, sign) and `Key Vault Crypto Officer` for the custodians. **Vault Transit:**
a policy per stage allowing `update` on `transit/encrypt/<kek>`, `transit/decrypt/<kek>` and
`transit/sign/<signing-key>/*`, and `read` on `transit/keys/<signing-key>`.

### 4.3 The AWS PoC is looser

The PoC (`deploy/aws/terraform/persistent/`) is for synthetic data and differs from the above:

- **One role per stage across pipelines:** each stage's role holds every pipeline's KEKs and signing keys of that
  stage (`stage_access` in `iam.tf`). A deployment holding PHI has a role per stage per pipeline
  (HIPAA-OPERATIONS.md §2.8).
- **IAM decides:** the keys keep AWS's default key policy; the deployer's own policy denies it decrypting or
  signing with them, and the stages' roles are bounded by a permissions boundary.
- **Wider grants:** each stage has `Encrypt`, `Decrypt`, `GenerateDataKey` and `DescribeKey` on every KEK it uses,
  even one it only reads, and every stage has `GetPublicKey` and `Verify` on every signing key. No encryption
  context is required.
- **The gold check** (its stand-in for a gold reader) also unwraps the records KEK, to compare each bundle with
  its source document.
- **No automatic rotation**, to save its cost on a PoC (`enable_key_rotation = false`).

## 5. Rotation

| Key | Rotated how | How often |
|---|---|---|
| **KEKs** | *New key material, same key:* the KMS's automatic rotation (AWS yearly by default, 90 days to 7 years; Google and Azure by a rotation policy; Vault `transit/keys/<k>/rotate`). Old material still unwraps, so nothing in the product changes. *A different key:* `rotate`, then `re-key` (HIPAA-OPERATIONS.md §6.1). | Automatic rotation on, yearly at least; a different key when a custodian leaves, on suspicion, or on the covered entity's schedule |
| **A stage's signing key** | Not automatic: AWS and Google don't rotate asymmetric keys. Replaced by a new key with a new `kid`, below. | When a custodian leaves, on suspicion, or on schedule |
| **A pod's signing key** | By itself: a new key at every pod start, its statement renewed at half its hour | Always |
| **An inbox key pair** | Replaced, below | On suspicion, or on schedule |
| **Gold readers' key pair** | The reader accepts several keys while the transform stage moves to the new one (`recipientKey` more than once, DESIGN.md §10.2) | On suspicion, when a reader's owner changes, or on schedule |
| **An operator's key pair** | Another key admin registers the new public key (at most a year's expiry), and revokes the old | Yearly at least: registration enforces it |
| **The producer's key pair** | The producer gives its new public key and fingerprint; the stages' `results.recipient` change at a restart | The producer's schedule |
| **Batch data keys, agreement keys** | New for every batch, and every 10,000 messages or 60 s | Always |
| **ID keys** | **Never.** Record IDs, and so downstream deduplication, depend on it. A re-key rewraps it under the new KEK. | — |

**Replacing a stage's signing key or inbox key pair.** Each stage opens messages with exactly one inbox key and
checks them against exactly one signing key of the stage before (`KeyMaterial.inboxOpener`), so there is no
overlap: a message sealed or signed under the old key is refused (`stage.message_rejected`). Drain first, so none is
left:

1. **Make the new key.** For a signing key, the custodians create it in the KMS and grant it to the stage's
   identity (§4.1), and its public half is exported. For an inbox key pair, the platform administrator makes it as
   the first was (an encrypted PKCS#8 EC P-256 key), and records the public half's SHA-256.
2. **Record what the old key signed.** For a signing key: run `audit-verify` and `audit-export` on the stage, and
   keep the old public key with the archive, since `audit-verify` checks seals against the current key only (§8),
   as are the stage's erasure certificates.
3. **Pause the sending stage** (HIPAA-OPERATIONS.md §7.1), and wait until the receiving stage's input is drained
   (`drain-check` on it, or the broker's lag at zero). The sending stage is the one whose signing key is replaced,
   or the one that seals to the inbox key being replaced.
4. **Change both sides' Secrets and settings:** for a signing key, the stage's `signing` (`kid`, `key-ref`) and the
   next stage's `landing-signer` (`kid`, public PEM); for the transform stage's, every gold reader's too. For an
   inbox key, the receiving stage's `inbox` and the sending stage's `recipient` (`kid`, PEM, `sha256`). A stage
   that sends to itself (a reprocess) has its own `signing.public-pem` or `inbox.public-pem` to change as well.
5. **Restart the receiving stage, then resume the sending one.** A stage whose inbox key's fingerprint
   doesn't match its pinned `sha256` refuses to start; a stage logs its signing key's fingerprint at startup
   (`crypto.signing_key`): check it against step 1's.
6. **Retire the old key:** the custodians disable an old signing key in the KMS; the administrator deletes an old
   inbox private key and its passphrase.

A message refused anyway, one already queued for a reprocess say, is not lost: the reconcilers redo its item from
the table before, as they do for a revoked pod statement (PERFORMANCE.md §3.5).

## 6. A key thought exposed

| Exposed | What it opens | Do |
|---|---|---|
| A stage's identity or credentials | its tables' data, through the KMS, until the credentials expire | Revoke the sessions; review CloudTrail for its `Decrypt` calls; then as the incident procedure says (HIPAA-OPERATIONS.md §6) |
| A KEK's administration | changing who may use it | Review the key policy and its CloudTrail; rotate to a new KEK and re-key (§5) |
| A pod's memory (a dump, a host) | the data keys it had unwrapped, its inbox private key, its ID key, its pod key | Revoke its pod statement (`sdh.crypto.pod-keys.revoked`, the ID from its `crypto.pod_statement` log line); replace the stage's inbox key pair (§5); re-key if the dump may have held many batch data keys; the ID key can't be replaced, which exposes the keyed hashes to guessing, not the data |
| A stage's signing key's use | signing as that stage: messages, statements, seals | Disable the key in the KMS, and revoke the pods' current statements, which otherwise stay good for up to an hour (the stage stops sealing as each pod's next statement fails); replace it (§5); check the stage's `Sign` calls in CloudTrail |
| An inbox private key and its passphrase | the messages sealed to it still on the broker, within its retention | Replace it (§5); treat the topic's retained messages as exposed |
| Gold readers' private key | the transform stage's messages still on the broker | Replace it (§5); gold at rest stays under the FHIR KEK |
| An operator's private key | the results sealed to it, wherever copies are | Revoke the key (`DELETE /operator-keys/{fingerprint}`); find what was sealed to it from `sdh.recipient_key` in the audit (AUDIT-EVENTS.md); treat those results as exposed |

## 7. Destruction

**The product never disables or destroys a KMS key.** That is the key custodians' act, in the KMS, two people
(CONTROL-API-DESIGN.md §11):

- **After a re-key:** disable the old KEK once `GET /keys` shows no rows under it on every stage that used it, and
  an hour after the last re-key. Keep it disabled, not deleted, for as long as the backups made before the re-key
  are kept: they are still under it. Then schedule its deletion (AWS waits 7 to 30 days; the PoC's keys, 30).
- **At the end of a contract** (HIPAA-OPERATIONS.md §8): each stage's erasure certificate lists its KEKs and signing
  key. The custodians schedule their deletion, which is the cryptographic erase of everything the pipeline stored:
  its tables, their noncurrent versions, the wrapped keys in its database, and every backup of them. Also destroy:
  - the inbox key pairs and their passphrases, in the stages' Secrets and wherever else they were kept (on the PoC,
    the state directory's `keys/` and `secrets/`), which open what is left on the broker's topics;
  - the gold readers' key pair, with the data consumer owner;
  - the signing keys' exported public halves need no destruction, but keep them with the audit archive so the
    erasure certificates and seals can still be checked.
- **The ID keys** are in their tables' properties and go with the tables. Wrapped under the KEKs, a copy in a backup
  goes with the KEKs.
- **Operators' key pairs:** revoked when a person leaves, and the sealed files on that person's workstation
  destroyed (HIPAA-OPERATIONS.md §8).

## 8. Findings

- **`audit-verify` checks seals against the stage's current signing key only** (`AuditSeal.verify`, one key and
  `kid`). After a stage's signing key is replaced, its older chains' seals show as failed, and so would an erasure
  certificate checked by the product against the new key. Until the product accepts earlier keys by `kid`, verify
  and export the journal before replacing a signing key, and keep the old public key with the archive (§5, step 2).
- **No overlap when replacing a stage's signing key or inbox key pair:** each stage trusts one of each, so a
  replacement means pausing and draining (§5). Gold readers accept several keys; stages don't. A product change
  would let a stage accept a second inbox key and a second upstream signer for the length of a replacement.
- **HIPAA-OPERATIONS.md §6.1 named the records KEK as the transform stage's own;** its scope's KEK is its sink's,
  the FHIR KEK (`StageTables.sinkKek`). Corrected there.
- **DESIGN.md §7.1's key table is from before the stages were split:** it names source-files and failed-files KEKs
  and a recovery key that the product no longer has. This document's §2 is the current list.

## Sources

| Statement | Source |
|---|---|
| The key settings | `connector-runtime` `CommonSettings` (`Crypto`, `Keys`, `PodKeys`, `Jose`); `StageTables.sinkKek` |
| Keys loaded and checked at startup; one inbox key and one upstream signer | `KeyMaterial` (`load`, `forStage`, `forLanding`, `inboxOpener`, `probeKek`) |
| KMS calls and encryption context | `connector-kms` `AwsKmsKeyProvider` (and the Google, Azure and Vault providers) |
| Envelope encryption, ID key | DESIGN.md §7.2, §7.3; `connector-core` `IdKeys` |
| Pod keys and their revocation | PERFORMANCE.md §3.5; `KeyMaterial.messageSigner`, `PodSigner` |
| Audit seals | `connector-core` `AuditSeal`; `AuditJournals` |
| Operators' keys | CONTROL-API-DESIGN.md §7; `OperatorKeys`, `OperatorKeyCommands` |
| Held break-glass results | `ControlResults` |
| Rotation, re-key, the product never destroys a KEK | CONTROL-API-DESIGN.md §11; `KeyCommands`, `KekWatch`, `RekeyPass`; HIPAA-OPERATIONS.md §6.1 |
| The reference's keys | `deploy/reference/*-application.yml` (`sdh.crypto`, `sdh.stage.sink.kek`) |
| The PoC's keys and grants | `deploy/aws/terraform/persistent/main.tf` (KMS), `iam.tf` (`stage_access`, `gold_reader`) |
