# Control API deployment and use

For the platform administrator who deploys a pipeline's control API, the security official who decides who may use
it, and the people who call it. It covers deploying the API, exposing it, connecting it to the identity provider,
mapping its permissions, registering sealing keys, and calling it. It is HIPAA.md §11.2's document D13, with P1. The
API itself is CONTROL-API.md and CONTROL-API-DESIGN.md. Who may hold which permission is [IDENTITY.md](IDENTITY.md).
What each call audits is [AUDIT-EVENTS.md](AUDIT-EVENTS.md) §6.3. The contract is
`connector-stage/src/main/resources/control/openapi-v1.yaml`, which every stage serves at `/api/v1/openapi.yaml`.

It describes a pipeline deployed from `deploy/reference/` and the kits' READMEs, with the `hipaa` compliance profile
on. The ingress and identity provider examples here are starting points, not configurations the product has been
tested with: the harness reaches the control APIs through port-forwards (CONTROL-API-DESIGN.md §15.11, §15.12).

## 1. What to deploy

A pipeline has **a control Deployment for each of its four stages**, `sdh-control-<stage>`. Each one runs from the
stage's own image, in control mode (`args: [control]`), beside the stage's workers:

| | |
|---|---|
| Rights | the stage's: the workers' service account, Secret, keys and ConfigMap, with nothing more granted (IDENTITY.md §2.1) |
| Settings | the workers' settings, then a ConfigMap of its own, `sdh-control-<stage>`, mounted at `/control` and read after them |
| Ports | 8443, TLS, for the API; 8081, plain, serving only `/livez` and `/readyz` for Kubernetes' probes |
| Replicas | 2, for availability. It keeps no state of its own: any replica answers any call, and a long operation is run by whichever holds its lease |
| Memory | 768 MiB a pod, half of it heap |
| Manifests | the Deployment, Service and NetworkPolicy in each kit's README, "Control API" |

**Deploy all four, the transform stage's included.** Each stage answers only for its own data:
- A failure is exported, retried or corrected on the stage it failed at.
- The patient lookup runs on the stages that hold records: convert and transform (HIPAA-OPERATIONS.md §4.1).
- Retention, a purge or an erasure is asked for and approved on each stage, for that stage's data. An item is
  purged at every stage it passed through (CONTROL-API-DESIGN.md §15.8).

Under the profile the CLI refuses the operations that need four eyes (`cli-four-eyes`). Without a stage's control
API, its data can then be purged or erased only by waiving that check. The transform kit's README doesn't yet have a
"Control API" section (§9), so follow the convert stage's, with the transform kit's names:
- the workers' Secret `transform-stage-env`;
- the keys Secret `transform-stage-keys`;
- the ConfigMaps `transform-stage-config` and `transform-stage-wstl`.

**Spread the replicas.** Put the two pods on different nodes and zones (`topologySpreadConstraints`), and give
each Deployment a PodDisruptionBudget of `minAvailable: 1`. The control API is needed most when the workers are in
trouble, and that is often when nodes are being replaced.

**People and automation use separate control Deployments.** A Deployment takes either tokens or client
certificates (`sdh.control.auth.mode`), not both. To serve both, add a second control Deployment per stage, for
example `sdh-control-<stage>-mtls`. It uses the same image, Secret and keys, with a ConfigMap of its own in `mtls`
mode. Both share the stage's database, so they see the same operations and sealing keys. The product hasn't been
run this way (§9). If automation only watches, it can use a token from the identity provider's client-credentials
grant instead, with `pipeline.view` alone.

## 2. TLS, the ingress and the network

### 2.1 The pod's certificate

TLS is always on: the profile refuses a control API without it (`control-tls`). Each Deployment reads its
certificate and key from the TLS Secret `sdh-control-<stage>-tls`, mounted at `/run/control`
(`sdh.control.tls.certificate`, `.key`; `.key-password` for an encrypted key).

- **Its name:** the name the caller of the pod checks. With tokens (§2.2), that caller is the ingress, which reaches
  the Service by `sdh-control-<stage>.<namespace>.svc`. With client certificates (§2.3), it is the client, which
  reaches the pod by the stage's own host name.
- **Its issuer:** the customer's internal CA, or cert-manager with one. Not a public CA: the API is never on the
  internet.
- **Renewal:** the pod reads the certificate when it starts. After the Secret is renewed, restart the Deployment
  (`kubectl rollout restart`), or have a controller such as Reloader do it. Renew well before expiry, since
  replicas are restarted one at a time.

### 2.2 With tokens: the ingress ends the client's TLS and starts its own

People reach the API through the customer's ingress, **inside its network only**, from the operators' path in
(VPN, bastion or zero-trust access; HIPAA.md §11.2 D1). The ingress routes `/pipelines/<pipeline>/stages/<stage>/`
to that stage's control Service. It must:

- **Strip the prefix.** Each pod serves `/api/v1/…` at its root, so `/pipelines/p1/stages/split/api/v1/status`
  reaches the split stage's pods as `/api/v1/status`.
- **Re-encrypt to the pod, and verify its certificate** against the CA that issued it, by the Service's name. Never
  plain HTTP behind the ingress: the API refuses it, and the token and results would cross the cluster in the clear.
- **Pass the `Authorization` header through, and never log it.** Don't log request or response bodies either. A
  patient lookup's identifiers are in its body, never in its URL (HIPAA-OPERATIONS.md §4.1), and the API never puts
  PHI in a path or query.
- **Stream, without buffering:** a sealed result is a chunked stream of JSON lines (§6.3), and so are the support
  bundle and an audit export. Turn off response buffering for these paths. Allow a read timeout long enough for the
  largest result: minutes, not the usual 60 seconds.
- **Limit what it lets in:** only the operators' networks, rate limits per client, and a request body limit of a
  few MiB (`correct` takes one record).

With the Gateway API, for example (one HTTPRoute and one BackendTLSPolicy per stage):

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: sdh-control-split }
spec:
  parentRefs: [{ name: <internal-gateway>, namespace: <ingress-namespace> }]
  hostnames: [<control-host>]
  rules:
    - matches: [{ path: { type: PathPrefix, value: /pipelines/<pipeline>/stages/split } }]
      filters:
        - type: URLRewrite
          urlRewrite: { path: { type: ReplacePrefixMatch, replacePrefixMatch: / } }
      backendRefs: [{ name: sdh-control-split, port: 8443 }]
      timeouts: { request: 30m }
---
apiVersion: gateway.networking.k8s.io/v1
kind: BackendTLSPolicy
metadata: { name: sdh-control-split }
spec:
  targetRefs: [{ group: "", kind: Service, name: sdh-control-split }]
  validation:
    caCertificateRefs: [{ group: "", kind: ConfigMap, name: sdh-control-ca }]
    hostname: sdh-control-split.<pipeline-namespace>.svc
```

Buffering, header logging and rate limits are set in each gateway's own policy resources.

**The `Location` an action answers** (`/api/v1/operations/{id}`), and a finished operation's `resultAt`, are paths on
the pod, without the ingress's prefix. A client behind the ingress adds the stage's prefix to them (§9).

### 2.3 With client certificates: the pod must see the certificate

In `mtls` mode the pod itself requires and checks the client's certificate, so nothing in front of it may end the
TLS connection. Route by host name instead of path: give each stage's mTLS Deployment a host name of its own, such
as `split.<pipeline>.control.<internal-domain>`, and either:
- pass TLS through on the gateway by SNI (a `TLSRoute` with a listener in `Passthrough` mode, or the ingress
  controller's own passthrough); or
- put an internal layer-4 load balancer in front of the Service.

The pod's certificate is then for that host name. Path routing, header rules and body limits aren't available,
since nothing in front of the pod can read the traffic.

### 2.4 The network

- **Into the control pods:** each kit's NetworkPolicy lets in only `<ingress-namespace>`, on 8443. That is the
  gateway's data plane namespace, or the load balancer's source ranges in `mtls` mode. The kubelet's probes reach
  8081 from the node.
- **Out of them:** what the stage's workers reach (the KMS, the object store, the database, the broker, the
  collector), plus the identity provider. In `jwt` mode each pod fetches the issuer's discovery document and its
  key set (JWKS). Where the pods may not reach the issuer, set `sdh.control.auth.jwt.jwk-set-uri` to a key set they
  can reach. The pods start while the identity provider is down; calls are refused until its keys can be fetched.
- **The workers** gain no inbound port: the control API is the only listener added (CONTROL-API-DESIGN.md §1).

## 3. Connecting the identity provider

### 3.1 Tokens, for people

Register the control API with the identity provider as an API, or resource server, with:

| | Set in the identity provider | Setting |
|---|---|---|
| Issuer | its issuer URI, exactly as the tokens' `iss` names it | `sdh.control.auth.jwt.issuer-uri` |
| Audience | an identifier for this API, put in the access token's `aud` | `sdh.control.auth.jwt.audience` |
| Principal | a claim that names each person uniquely and is never reused: `sub`, or an immutable ID (`oid` on Entra ID). Not an e-mail address | `sdh.control.auth.jwt.principal-claim` (default `sub`) |
| Roles | a claim listing the person's groups or app roles, in the access token | `sdh.control.auth.jwt.roles-claim` (default `groups`) |
| Lifetime | access tokens of an hour or less | `sdh.control.auth.jwt.max-lifetime` (default and, under the profile, longest `1h`: `control-token-lifetime`) |
| Signing | an asymmetric algorithm: RS256–512, PS256–512 or ES256–512. HMAC-signed and unsigned tokens are refused | — |
| MFA | required on every sign-in to the clients that get tokens for this audience; the product doesn't check it (IDENTITY.md §3.1, §9) | — |

- **The roles claim's values are what `sdh.control.permissions` lists** (§5). Some providers put group IDs in it,
  others names. Entra ID puts group object IDs in `groups`, and app role values in `roles`. App roles are easier to
  review, and don't overflow when a person is in many groups.
- **A token is checked** for its signature against the issuer's keys, its issuer, its audience, expiry and
  not-before (60 s of clock skew), its lifetime, and a principal. A refused call is audited as
  `control.unauthenticated` with the class of reason.
- **The clients** that get tokens for the API are the customer's: a command-line OIDC client on the operators'
  workstations (device code or authorization code with PKCE), and later the console. Allow only those. Don't let a
  long-lived refresh token stand in for the sign-in's MFA: limit refresh tokens to the provider's session.

### 3.2 Client certificates, for automation

- `sdh.control.auth.mtls.trusted-ca`: the CA that issues automation's client certificates. Use a CA for this alone,
  not the one that issues every workload certificate, since every certificate it issues can try to authenticate.
- `sdh.control.auth.mtls.roles`: the subjects let in, and their roles. A subject not listed is refused, even from the
  trusted CA. The subject is written in brackets, since it holds commas and equals signs:

  ```yaml
  sdh:
    control:
      auth:
        mode: mtls
        mtls:
          trusted-ca: /run/control-clients/ca.crt
          roles:
            "[CN=noc-automation,O=Example]": [sdh-viewers]
  ```

- The principal is the certificate's subject in canonical form, lower-cased: `cn=noc-automation,o=example`. That is
  how the audit names it, and how a sealing key is registered for it.
- **Revocation isn't checked** (§9). To take a certificate's access away before it expires, remove its subject from
  `roles` and restart the Deployment. Issue client certificates for short periods.

## 4. The settings

Every `sdh.control` setting, in the ConfigMap `sdh-control-<stage>`'s `application.yml`
(`ControlSettings`). The control API refuses to start if a setting it needs is missing or wrong, and says which.

| Setting | Default | |
|---|---|---|
| `sdh.control.port` | `8443` | the API's port |
| `sdh.control.probe-port` | `8081` | the probes' port, plain, `/livez` and `/readyz` only; must differ from the port |
| `sdh.control.tls.enabled` | `true` | the profile refuses `false` (`control-tls`) |
| `sdh.control.tls.certificate` | — | the certificate (chain), PEM; required |
| `sdh.control.tls.key` | — | its private key, PEM; required |
| `sdh.control.tls.key-password` | — | the key's passphrase, if it is encrypted; from the Secret, never the ConfigMap |
| `sdh.control.auth.mode` | `jwt` | `jwt` (tokens) or `mtls` (client certificates) |
| `sdh.control.auth.jwt.issuer-uri` | — | the issuer; required in `jwt` mode. Its discovery document gives the key set |
| `sdh.control.auth.jwt.jwk-set-uri` | — | the key set's URL, instead of discovering it from the issuer |
| `sdh.control.auth.jwt.audience` | — | required in `jwt` mode |
| `sdh.control.auth.jwt.principal-claim` | `sub` | §3.1 |
| `sdh.control.auth.jwt.roles-claim` | `groups` | §3.1 |
| `sdh.control.auth.jwt.max-lifetime` | `1h` | tokens issued for longer are refused; the profile refuses over `1h` |
| `sdh.control.auth.mtls.trusted-ca` | — | required in `mtls` mode |
| `sdh.control.auth.mtls.roles` | — | subjects and their roles, §3.2 |
| `sdh.control.permissions` | none | each permission and the roles granted it, §5 |
| `sdh.control.approval.ttl` | `24h` | a four-eyes request not decided within this expires |
| `sdh.control.approval.break-glass-ttl` | `1h` | the same for a plaintext break-glass restore, and how long its result is held; no longer than `ttl` |

## 5. Mapping the permissions

`sdh.control.permissions` maps each permission to the roles (the values of the roles claim, or of an mTLS subject's
`roles`) that hold it. **Nothing is granted unless it is listed.** The name of a permission that doesn't exist stops
the API from starting. Write each permission in brackets, so that its dot is kept:

```yaml
sdh:
  control:
    permissions:
      "[pipeline.view]": [sdh-viewers, sdh-operators]
      "[pipeline.operate]": [sdh-operators]
      "[phi.export]": [sdh-phi-export]
      "[phi.break-glass]": [sdh-break-glass]
      "[audit.read]": [sdh-auditors]
      "[audit.admin]": [sdh-audit-admins]
      "[keys.admin]": [sdh-key-admins]
      "[data.admin]": [sdh-data-admins]
```

- **What each permission allows,** operation by operation, and the suggested groups, are IDENTITY.md §4. The
  security official decides the mapping, in writing.
- **Give every stage of a pipeline the same mapping,** and the mTLS Deployments the same permissions for their
  subjects' roles. A different mapping on one stage is easy to miss in a review.
- **Each permission that needs four eyes needs at least two holders** (IDENTITY.md §4.2), or its requests can never
  be approved.
- **To check it:** `GET /api/v1/settings` on each stage shows the mapping as the stage read it, with secrets
  removed. Compare the four stages' answers.
- **A change** to the ConfigMap takes effect when the Deployment is restarted. Removing a person from a group takes
  effect when their current token expires: an hour at most.

## 6. Calling it

### 6.1 Every call

```bash
base=https://<control-host>/pipelines/<pipeline>/stages/split/api/v1
curl -sS --fail-with-body -H "Authorization: Bearer $(<token command>)" "$base/status"
```

- **Every path needs a credential**, the contract and health included. Only the probe port is open, and it says
  only whether the pod is up.
- **Every answer** carries `X-Request-Id`, which the call's audit event names. Give it when asking the vendor for
  support.
- **A refusal** is a problem document (`application/problem+json`) with a code: `401` for no valid credential;
  `403 permission-required`, `approval-required`, `own-request`, `not-your-request`, `sealing-key-not-yours`,
  `not-for-yourself`; `404 not-on-this-stage`, `operation-not-found`; `409 idempotency-key-reused`, `in-progress`,
  `wrong-state`, `refused`; `410 result-gone`. It never holds PHI: a failure is named by its class alone.

### 6.2 Actions

An action (`POST`, `PUT`, `DELETE`) needs an `Idempotency-Key` header of the client's choosing, and a JSON body
with a `reason`. **A reason or a ticket never holds PHI:** name the item by its ID, the request by the ticket
system's reference (HIPAA-OPERATIONS.md §1).

```bash
curl -sS --fail-with-body -X POST -H "Authorization: Bearer $(<token command>)" \
  -H "Idempotency-Key: $(uuidgen)" -H 'Content-Type: application/json' \
  -d '{"reason": "INC-1042: broker maintenance"}' "$base/pause"
```

- A retry with the same key and body, by the same principal, within 24 hours, gets the first answer back and does
  nothing again. Use a new key for a new request.
- **A long operation** (a reprocess run, a drain, a re-key) is answered `202` with `Location`. Poll
  `GET /operations/{id}`, with the stage's prefix (§2.2), until its `state` is `done`, `failed`, `cancelled`,
  `rejected` or `expired`. `GET /operations` lists a stage's operations, newest first.

### 6.3 Four eyes

`purge`, `erase`, `rekey`, `audit-prune`, `restore-repair`, `restore-release` and a plaintext `restore` are answered
`202` with the operation `awaiting-approval` (IDENTITY.md §5):

1. **The requester** asks, with a reason and, where the contract asks, a ticket.
2. **The approver,** another principal holding the same permissions, reads the request with
   `GET /operations/{id}`: its parameters, reason and ticket, never data. They then send
   `POST /operations/{id}/approve` or `/reject` with a `reason`, and no `Idempotency-Key`.
3. **A control pod runs it,** audited with both principals. The requester may `POST /operations/{id}/cancel` before
   then. A request not decided within `approval.ttl` (24 h; 1 h for break-glass) expires.
4. **A break-glass result** is fetched by its requester, once, from `GET /operations/{id}/result`, within
   `break-glass-ttl`. A second fetch is `410 result-gone`.

Agree in the runbook how an approver learns of a request: the ticket, or an alert on the `awaiting-approval` event.

### 6.4 Sealed results

A failure's content, a patient lookup and a sealed restore are sealed to one of the caller's own registered keys
(§7), named by its fingerprint in `sealTo`:

```bash
curl -sS --fail-with-body -X POST -H "Authorization: Bearer $(<token command>)" \
  -H 'Content-Type: application/json' \
  -d '{"reason": "INC-1042: failed record", "sealTo": "<fingerprint>"}' \
  -o result.ndjson "$base/failures/<itemId>/<seq>/export"
```

Save it only on the managed workstation that holds the private key, on an encrypted disk. The answer is JSON lines:
- a header naming the operation, `sealedTo` and the request;
- a line per part, `{"part": n, "jwe": "…"}`;
- a trailer, `{"end": true, "parts": n, "sha256": "…"}`.

A stream without that trailer was cut off, and is discarded. To open one:

```bash
set -euo pipefail
f=result.ndjson
tail -n 1 "$f" | jq -e '.end == true' >/dev/null      # else: cut off, discard
sed '1d;$d' "$f" | jq -r .jwe > parts.txt
[ "$(wc -l < parts.txt)" = "$(tail -n 1 "$f" | jq -r .parts)" ]
[ "$(tr -d '\n' < parts.txt | sha256sum | cut -d' ' -f1)" = "$(tail -n 1 "$f" | jq -r .sha256)" ]
while read -r jwe; do
  printf '%s' "$jwe" | step crypto jwe decrypt --key operator-private.pem
done < parts.txt > result.out
```

The trailer's hash is over the parts' JWEs, in order, as ASCII, with nothing between them. Each part is
`ECDH-ES+A256KW` (EC keys) or `RSA-OAEP-256` (RSA), with `A256GCM`. `step` asks for the key's passphrase once per
part, so a long result is easier to open with a JOSE library: the harness's `Unsealer` is the reference. Destroy the
output when the task is done (HIPAA-OPERATIONS.md §8).

## 7. Registering sealing keys

A PHI result is sealed only to a key registered in advance for the caller, by someone else. A stolen token then
can't have PHI sealed to the thief's key (CONTROL-API-DESIGN.md §7). Keys are kept in the pipeline's database
(`sdh_operator_keys`), so a key registered through any stage's API serves all four.

1. **The holder makes a key pair** on their managed workstation, with the private key encrypted
   (HIPAA-OPERATIONS.md §2.3). It must be EC P-256 or P-384, or RSA of at least 3072 bits. The private key never
   leaves the workstation.
2. **The holder sends the public key and its fingerprint** to the security official, by a channel that shows who
   sent it (a signed message, or in person). The fingerprint is the SHA-256 of the key's SubjectPublicKeyInfo, in
   lower-case hex:

   ```bash
   openssl pkey -pubin -in operator-public.pem -outform DER | sha256sum | cut -d' ' -f1
   ```

3. **The security official records it** in the fingerprint register: holder, principal, fingerprint, expiry, and
   who approved it (IDENTITY.md §7).
4. **A `keys.admin` registers it,** and isn't its holder: registering a key for oneself is
   `403 not-for-yourself`. The principal is the holder's name as the API authenticates them (§3.1, §3.2). Take it
   from a token issued for the API's audience, or from the `sdh.operator` of one of their calls, since some
   providers' `sub` differs per application. The expiry is at most a year away:

   ```bash
   jq -n --arg p '<principal>' --rawfile k operator-public.pem --arg e '<expiry, e.g. 2027-11-25T00:00:00Z>' \
     '{principal: $p, publicKey: $k, expiresAt: $e, reason: "CHG-2210: sealing key"}' \
   | curl -sS --fail-with-body -X POST -H "Authorization: Bearer $(<token command>)" \
       -H "Idempotency-Key: $(uuidgen)" -H 'Content-Type: application/json' -d @- "$base/operator-keys"
   ```

5. **The `keys.admin` checks** that the fingerprint answered is the one in the register. The registration is audited
   (`control.operator_key_register`), with its fingerprint and principal.

**Renewal:** before a key expires, the holder makes a new pair, and steps 2 to 5 are repeated. The old key is
revoked once the new one is registered. An expired key is refused for sealing.

**Revocation:** when a key may be lost, or its holder leaves or no longer needs it (IDENTITY.md §8), a `keys.admin`
sends `DELETE /operator-keys/{fingerprint}` with a reason, and the register is updated. Sealing to it is refused from
then on. What was already sealed to it stays readable by whoever has the private key, so its sealed files are
destroyed too.

**Review:** `GET /operator-keys` lists every key (holder, fingerprint, who registered it, expiry, revocation). The
quarterly access review compares it with the register (IDENTITY.md §7).

The CLI, the emergency path, takes the public key on standard input and doesn't use the registered keys
(IDENTITY.md §3.2).

## 8. Checking a deployment

Before real PHI, and after any change to the control API's settings or the ingress:

- [ ] `GET /compliance` on each stage shows no finding and no waiver for `control-tls`, `control-token-lifetime` or
  `cli-four-eyes`.
- [ ] A call with no token, an expired token, and a token for another audience are each refused with `401`, and
  audited as `control.unauthenticated`.
- [ ] A call from outside the operators' networks doesn't reach the ingress, and a pod can't be reached on 8443 from
  any other namespace.
- [ ] A viewer's `POST /pause` is `403 permission-required`, audited with `decision=denied`.
- [ ] `GET /settings` shows the same permission mapping on every stage, as the security official approved it.
- [ ] A four-eyes request can't be approved by its requester (`403 own-request`).
- [ ] A sealed export to another person's key is `403 sealing-key-not-yours`, and one to the caller's own key opens
  on their workstation, with its trailer checked.
- [ ] The ingress's logs hold no `Authorization` header and no body.
- [ ] `GET /operator-keys` matches the fingerprint register.
- [ ] `audit-verify` is clean afterwards (HIPAA-OPERATIONS.md §3.2).

The harness's access and break-glass scenarios check the same against a harness pipeline, with its own token
issuer (CONTROL-API-DESIGN.md §14). They don't check the customer's ingress or identity provider.

## 9. Findings

- **The transform kit has no "Control API" section,** and the harness has never deployed the transform stage's
  control API (CONTROL-API-DESIGN.md §15.11, §15.12). The stage has control mode (`TransformStageApplication`).
  Without its control API:
  - its failures can't be exported or corrected through the API;
  - nor can the patient lookup run on gold;
  - under the profile, its data can be purged or erased only with `cli-four-eyes` waived.
- **`Location` and `resultAt` ignore the ingress's prefix.** They are paths on the pod (`/api/v1/operations/{id}`),
  so a client behind an ingress that strips `/pipelines/<p>/stages/<s>` must add it back itself. A setting for the
  public base path, or honouring `X-Forwarded-Prefix`, would fix it.
- **Client certificates aren't checked for revocation:** no CRL or OCSP. A revoked certificate is refused only once
  its subject is removed from `auth.mtls.roles` and the pods restarted (§3.2).
- **A control Deployment takes tokens or certificates, not both,** so people and automation on one stage need two
  Deployments (§1). The product hasn't been run that way.
- **No ingress has been tried.** The harness reaches the control pods by port-forward. The prefix stripping,
  re-encryption, streaming and passthrough of §2 are untested.
- **CONTROL-API-DESIGN.md §2's example is out of date:** it has an `mtls.principal` setting that wasn't built (the
  principal is always the canonical subject), and permission names without the brackets the kits' READMEs use.

## Sources

| Statement | Source |
|---|---|
| The control Deployment, Service, network policy, ports, memory | each kit's README, "Control API" (`landing-connector`, `file-connector`, `convert-stage` `src/dist/README.md`); CONTROL-API-DESIGN.md §1 |
| Settings, defaults and the start-up checks | `connector-stage` `control/ControlSettings` (`check`) |
| TLS, client certificates, the probe port | `ControlConfiguration` (the web server customizer, `controlSecurity`) |
| Token checks and signing algorithms | `ControlConfiguration` (`jwtDecoder`, `validator`, `SIGNATURES`) |
| Subjects in canonical form, in brackets | `ControlSettings.canonical`; `ControlServerTest.letsInAListedClientCertificate` |
| Paths, idempotency, problems, sealed streams, four eyes | `control/openapi-v1.yaml`; `ControlRoute`; `ControlService`; `Refusal` |
| Opening a sealed result | `control-harness` `Unsealer` |
| Sealing keys | `OperatorKeys` (`MAX_LIFETIME`, `MIN_RSA_BITS`), `OperatorKeyCommands`; CONTROL-API-DESIGN.md §7 |
| Compliance checks | `connector-runtime` `ComplianceCheck` (`CONTROL_TLS`, `CONTROL_TOKEN_LIFETIME`, `CLI_FOUR_EYES`) |
| Purge and erasure per stage | CONTROL-API-DESIGN.md §15.8 |
| Transform control mode | `healthcare-data-transformer` `TransformStageApplication` |
