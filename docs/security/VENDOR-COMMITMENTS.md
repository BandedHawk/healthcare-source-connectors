# The vendor's security commitments

> **DRAFT, for review.** This document is not yet for customers. §1 states what the product's build and release
> do today, each with where it is built and checked. Sections marked **For review** are commitments only the
> vendor can make: response times, support periods, how to report a vulnerability. Each has an ID, a proposed
> answer to start from, and is listed with the others in §4. Nothing marked for review is a commitment until it is
> answered.

For the customer's security official, for their evaluation of the product (§ 164.308(a)(8)), their protection
from malicious software (§ 164.308(a)(5)(ii)(B)), and their supplier review. It covers what Smile Digital Health,
the vendor, does to keep known vulnerabilities out of a release and to prove a release came from it, how security
fixes are released and how quickly, how to report a vulnerability, and what the vendor never has: access to the
customer's data. It is HIPAA.md §11.2's document D11 (§8.2 item 8 there). How a customer takes a release is
[UPGRADES.md](UPGRADES.md) (D12).

**It is written for a customer that operates the product.** The vendor will also offer managed services using
this pipeline, under separate agreements: there the vendor operates the pipeline and holds the customer's PHI, and
the vendor's and the customer's obligations are reshaped around that relationship (§1.3, V1). This document is
meant to shape that offering's commitments too. The gaps it leaves are kept in [KNOWN-GAPS.md](KNOWN-GAPS.md).

**The project is a proof of concept.** Its builds are outside the company's standard development process and CI
pipeline, and so outside the static and dynamic security scans that process includes. §1 describes the PoC's own
build tooling; once the build is incorporated into the production pipeline, §1 is to be revised to describe what
that pipeline does (§1.4).

## 1. What every release has today

### 1.1 Known vulnerabilities kept out

| What | How | Where |
|---|---|---|
| **Base images current** | the three base images are pinned by digest in one file; a release is not signed on a base whose tag has a newer build (`scripts/base-images.sh`); Renovate opens one pull request when a pinned tag moves | DESIGN.md §14.5; `docker/base-images.env`, `renovate.json` |
| **A vulnerability scan before signing** | grype over the SBOMs of every application jar and image, which include the OS packages and libraries bundled inside other jars; it fails on any High or Critical vulnerability with a fix available | DESIGN.md §14.5; `scripts/vuln-scan.sh` |
| **Accepted findings written down** | an unfixable finding is accepted only by name, limited to the bundled version, with the reason it can't be reached; reviewed at each release; there is no override switch | `docker/vuln-accepted.yaml`: 9 advisories today, in copies of Jackson and Guava bundled inside Parquet and Hadoop jars |
| **Libraries kept up** | security overrides in the parent POM where a library brings an older version, each naming which | DESIGN.md §14.5 |
| **Small images** | the customer image is distroless: no shell, no package manager; it runs as non-root on a read-only root filesystem | DESIGN.md §14.3; [HARDENING.md](HARDENING.md) |

### 1.2 Proof a release is the vendor's

| What | How | Where |
|---|---|---|
| **Signed tarballs** | a detached ECDSA P-256 signature over each tarball, checkable with cosign or with openssl alone | DESIGN.md §14.4; each kit's README, "Verify what you received" |
| **Signed images, by digest** | the signature stored in the registry beside the image; never by tag | DESIGN.md §14.4 |
| **SBOMs** | CycloneDX 1.6: one of the jar's bundled components, in the kit as `legal/sbom.cdx.json`, and one of each image (OS packages and Java components), attached to it as a signed attestation an admission policy can require | DESIGN.md §14.4 |
| **The signing key** | an EC P-256 key held in Azure Key Vault, which never leaves it; the public key, `cosign.pub`, ships with each release | DESIGN.md §14.4 |
| **No public transparency log** | signatures aren't recorded in Sigstore's Rekor, which would publish artifact digests; the public key is the trust anchor | DESIGN.md §14.4 |
| **Licences** | every third-party component's licence and notice in the kit and the image (`legal/`) | DESIGN.md §14.2 |

`scripts/sign-release.sh` does all of §1.1 and §1.2 in one run, for the landing, split and convert stages
(§1.4), then verifies the result as a customer would: with the public key only, a tampered tarball refused, and
another key refused.

### 1.3 What the vendor never has, when the customer operates the product

- **No access to the customer's data.** The product sends nothing to the vendor: telemetry and audit events go to
  the customer's own collector and store, and there is no licence check, update check or phone-home.
- **Support never needs PHI.** Logs, telemetry and issue text carry codes, IDs and positions, never values. The
  support bundle (`support-bundle`) holds no PHI, and the customer reads it before sending (HIPAA-OPERATIONS.md §9).
  So the vendor is not the customer's business associate by supporting the product (HIPAA.md §1).
- **No access to the customer's keys.** Every key is the customer's, in their KMS ([KEYS.md](KEYS.md)).

**Under a managed service none of this holds:** the vendor operates the pipeline, so it creates, receives,
maintains and transmits the customer's PHI, and is the customer's business associate (or subcontractor). That
service's agreements say who holds the keys, which of the vendor's staff may reach PHI, and which of the operator's
duties are the vendor's ([KNOWN-GAPS.md](KNOWN-GAPS.md) §4).

> **For review (V1): the vendor's role.** The product's owner and counsel to confirm that, under this document,
> the vendor offers no support that needs access to a customer's environment; and that managed services are
> offered only under agreements of their own (a BAA among them), which say how the commitments of §2 and §3 apply
> when the vendor operates the pipeline and applies fixes itself.

### 1.4 What it doesn't have yet

**The PoC is outside the standard process.** The items below are expected to be closed by incorporating the build
into the company's production pipeline, which includes static and dynamic security scans and the standard CI
pipeline; they are tracked in [KNOWN-GAPS.md](KNOWN-GAPS.md) §3 until then.

- **No release has been published.** The release registry, and where tarballs, signatures and `cosign.pub` are
  published, are not set up (DESIGN.md §14.4).
- **The transform stage isn't in the release script.** `sign-release.sh` builds, scans, signs and verifies the
  landing, split and convert stages; the transform stage, from its own repository, has a "Verify what you
  received" section in its README but no release run behind it yet.
- **Releases are built and signed on a workstation,** not in the standard CI pipeline, so there is no build
  provenance attestation (SLSA) yet.
- **No static or dynamic security scans, and no secrets scanning,** run on the PoC's builds: they are part of the
  standard process, not of the PoC's tooling.
- **FIPS 140 is not supported** ([HARDENING.md](HARDENING.md); each kit's README): no FIPS 140-validated module is
  used or tested.
- **No penetration test of the product by the vendor.** Customers test their own deployments (HIPAA.md §11.1 P13);
  a product fault a customer's test finds is the vendor's to fix.

## 2. Security fixes

> **For review (V2): how quickly fixes are released.** Proposed, measured from the vendor confirming the finding
> (or from a fix existing upstream, for a third-party component):
>
> | Severity (CVSS v3.1 or v4.0 base, as the vendor assesses it in the product) | Fixed release |
> |---|---|
> | Critical (9.0–10.0) | within 7 calendar days |
> | High (7.0–8.9) | within 14 calendar days |
> | Medium (4.0–6.9) | in the next scheduled release, or within 90 days |
> | Low | in a scheduled release |
>
> These leave the customer time inside the proposed Security Rule's windows: 15 calendar days for a critical
> risk and 30 for a high one, from identifying the need to patch or from a patch becoming available (90 FR 898,
> proposed § 164.308(a)(4)(ii)(C), checked 2026-11-25; UPGRADES.md §6). Answer: *open*.

> **For review (V3): which releases get fixes.** Proposed: the latest minor release of the current major version,
> and the latest minor release of the major version before it for 12 months after a new major version. Fixes come
> as a new patch release, never as a change to a published one. Answer: *open*.

> **For review (V4): how customers are told.** Proposed: a security advisory for every fixed vulnerability of
> Medium or above, naming the affected releases, the fixed release, the severity, and what to do meanwhile (a
> setting, a network policy), sent to each customer's named security contact and published with the release.
> Where a scanner would flag a component the product can't reach, a VEX statement (CycloneDX) saying so, published
> beside the SBOMs. Answer: *open*.

**Between releases,** a customer can scan a release's published SBOMs again with their own scanner: a
vulnerability published after the release is found that way, and the vendor's own rescans of supported releases'
SBOMs do the same (V5).

> **For review (V5): rescanning.** Proposed: the vendor rescans the SBOMs of every supported release at least
> weekly, and treats a new High or Critical finding as a report under V2. Answer: *open*.

## 3. Reporting a vulnerability

> **For review (V6): the reporting channel.** Proposed: a security mailbox (`security@<vendor domain>`), with a
> PGP key or an encrypted form; a `security.txt` (RFC 9116) on the vendor's site; acknowledgement within 2 business
> days, a first assessment within 7; coordinated disclosure, with credit if the reporter wants it. Reports must not
> include PHI: the product never needs PHI to show a fault. Answer: *open*.

> **For review (V7): a vulnerability disclosure policy.** Proposed: a published policy saying what may be tested
> (a reporter's own deployment only, never a customer's), safe harbour for good-faith research, and the timeline
> to public disclosure (90 days, or the fixed release, whichever is first). Answer: *open*.

## 4. Open questions

| ID | Question | For | Answer |
|---|---|---|---|
| V1 | Under this document, does the vendor offer no support needing access to a customer's environment? Are managed services offered only under agreements of their own, saying how §2 and §3 apply to them? | product owner, counsel | *open* |
| V2 | How quickly are fixes released, by severity? | product owner | *open* |
| V3 | Which releases get fixes, and for how long? | product owner | *open* |
| V4 | How are customers told: advisories, contacts, VEX? | product owner | *open* |
| V5 | How often does the vendor rescan supported releases? | product owner | *open* |
| V6 | How is a vulnerability reported, and how quickly acknowledged and assessed? | product owner | *open* |
| V7 | Is there a published disclosure policy, with safe harbour and a disclosure timeline? | product owner, counsel | *open* |
| V8 | Before the first release: where are releases and `cosign.pub` published, and when is the build incorporated into the production pipeline, building and signing in CI with provenance (DESIGN.md §14.4)? | product owner | *open* |
| V9 | For federal buyers: will the vendor give CISA's secure software development attestation, against NIST SP 800-218, to an agency that asks? OMB M-26-05 (2026-01-23) rescinded M-22-18 and M-23-16, so agencies may ask for it but needn't, and may require a current SBOM on request instead (HIPAA.md §8.4) | product owner, counsel | *open* |
| V10 | Is FIPS 140 support planned, and when (HIPAA.md §11.1 P8)? | product owner | *open* |

## Sources

- **Build and release:** DESIGN.md §14.1 to §14.5; `scripts/sign-release.sh`, `scripts/vuln-scan.sh`,
  `scripts/base-images.sh`; `docker/base-images.env`, `docker/vuln-accepted.yaml`, `renovate.json`; the parent
  POM's security overrides and `cyclonedx-maven-plugin`.
- **Documents:** HIPAA.md §1, §8.2 item 8, §8.4, §11.1 (P8, P13); HIPAA-OPERATIONS.md §9; each kit's README,
  "Verify what you received"; HARDENING.md; KEYS.md; UPGRADES.md; KNOWN-GAPS.md.
- **Rules and policy, checked on 2026-11-25:** the proposed Security Rule's patch windows in its Federal Register
  text (90 FR 898, 2025-01-06); OMB M-26-05 (2026-01-23), which rescinded M-22-18 and M-23-16.
