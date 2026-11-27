# Connecting an identity provider to the control API

For the customer's identity administrator and platform administrator: what to set up in the identity provider,
what to set in the pipeline, where the control API is, and how to check it works. It is the step-by-step part of
[CONTROL-API-DEPLOYMENT.md](CONTROL-API-DEPLOYMENT.md): §3 there gives each setting, and §5 the permission mapping.
Who may hold which permission is [IDENTITY.md](IDENTITY.md) §4; the security official decides it, in writing,
before you start.

**What has been tried.** The control API has been run only with the harness's own token issuer
(`control-harness` `TestIssuer`). None of the providers in §4 has been connected to it in our environments. Their
steps follow the providers' documentation as of 2026-11; check them against your provider's current documentation,
and run §6 before real PHI.

## 1. What you are connecting

```
operator's workstation                    customer's network                  pipeline namespace
┌──────────────────────┐  1. sign in, MFA  ┌────────────────────┐
│ OIDC command-line    │ ────────────────► │ identity provider  │ ◄──── 4. key set (JWKS) ────┐
│ client (device code  │ ◄──────────────── │ (issuer)           │                             │
│ or code + PKCE)      │  2. access token  └────────────────────┘                             │
│                      │                   ┌────────────────────┐  5. re-encrypted  ┌─────────┴──────────┐
│ curl / scripts       │ ── 3. HTTPS ────► │ internal ingress   │ ────────────────► │ sdh-control-<stage>│
└──────────────────────┘  Bearer token     │ /pipelines/<p>/    │   Bearer token    │ 2 pods, port 8443  │
                                           │  stages/<s>/…      │                   └────────────────────┘
                                           └────────────────────┘
```

- **The identity provider** signs people in, enforces MFA, and issues a short-lived **access token** (a signed JWT)
  for the control API's **audience**, naming the person and their groups or app roles.
- **The control API** never sees a password. It checks the token's signature against the provider's published
  keys, and its issuer, audience, expiry and lifetime, and then grants what `sdh.control.permissions` lists for the
  token's roles. It makes no call to the provider per request; it fetches the key set when it needs it.
- **One registration serves every pipeline and stage.** All four stages of every pipeline can share one audience
  and one issuer. Give each pipeline its own groups only if different people run different pipelines.

The API takes tokens (`jwt` mode, for people) or client certificates (`mtls` mode, for automation); this document
covers tokens. Client certificates are CONTROL-API-DEPLOYMENT.md §3.2.

## 2. Where the control API is

There is no single control endpoint. **Each stage of each pipeline has its own control API**: a Deployment and a
Service named `sdh-control-<stage>` in the pipeline's namespace, on port 8443, TLS. The stages are `landing`,
`split`, `convert` and `transform`. Nothing outside the cluster can reach them until you expose them through your
internal ingress (CONTROL-API-DEPLOYMENT.md §2.2), which gives them one host name and a path per stage:

```
https://<control-host>/pipelines/<pipeline>/stages/<stage>/api/v1/<operation>
```

| | |
|---|---|
| `<control-host>` | the host name you give the ingress route, on your internal domain; resolvable only from the operators' path in (VPN, bastion or zero-trust access) |
| `<pipeline>` | the pipeline's name, as in its namespace and its settings (`${PIPELINE}` in `deploy/reference/`) |
| `<stage>` | `landing`, `split`, `convert` or `transform` |
| The contract | `GET …/api/v1/openapi.yaml` on any stage: every operation, its permission and its answers |
| A first call | `GET …/api/v1/status`, which needs only `pipeline.view` |

To find what a cluster has:

```bash
kubectl get deploy,svc -A \
  -l 'app in (sdh-control-landing,sdh-control-split,sdh-control-convert,sdh-control-transform)'
```

Before the ingress exists, a platform administrator can reach one stage from their own workstation with
`kubectl port-forward svc/sdh-control-split 8443:8443 -n <pipeline-namespace>` and call
`https://localhost:8443/api/v1/status`. The pod's certificate won't name `localhost`, so this is for setting up
only; people use the ingress.

## 3. Before you start

Decide, and write down in the change record:

1. **The audience:** one identifier for the control API, such as `sdh-control` or `api://sdh-control`. Some
   providers choose it for you (§4).
2. **The groups or app roles**, one for each line of the approved mapping (IDENTITY.md §4.2). The examples use
   `sdh-viewers`, `sdh-operators`, `sdh-phi-export`, `sdh-break-glass`, `sdh-auditors`, `sdh-audit-admins`,
   `sdh-key-admins` and `sdh-data-admins`. Each permission that needs four eyes needs at least two people.
3. **The principal claim:** the claim that names each person uniquely and is never given to anyone else. It is
   how the audit names them, and how their sealing keys are registered, so changing it later orphans both. Never
   an e-mail address or a login name that can be reused.
4. **The operators' client:** the command-line tool that gets tokens on the operators' workstations (§5). It is a
   **public client** (no secret), using the device authorization grant or the authorization code grant with PKCE.
5. **The control host name** and its internal certificate (CONTROL-API-DEPLOYMENT.md §2).

## 4. In the identity provider

Whatever the provider, the result must be an access token that:

| Claim | Must be | Pipeline setting |
|---|---|---|
| `iss` | exactly the issuer URI, including or omitting a trailing `/` exactly as the provider does | `sdh.control.auth.jwt.issuer-uri` |
| `aud` | the audience, alone or in a list | `sdh.control.auth.jwt.audience` |
| `iat`, `exp` | present, and no more than an hour apart; the API refuses a token issued for longer, whatever its time left | `sdh.control.auth.jwt.max-lifetime` (`1h`, the longest the profile allows) |
| the principal claim | present, unique, immutable | `sdh.control.auth.jwt.principal-claim` |
| the roles claim | a **top-level** claim, a JSON list of strings (or one space-separated string), holding the values the mapping lists | `sdh.control.auth.jwt.roles-claim` |
| `alg` | RS256–512, PS256–512 or ES256–512 | — |

The roles claim is read at the top level only: a nested claim such as Keycloak's `realm_access.roles` can't be
named. Map groups or roles into a top-level claim instead.

And the provider must, for every sign-in to the operators' client:
- **require MFA**, through its own policy. The API doesn't read `amr` or `acr` (IDENTITY.md §9);
- end the session after the customer's idle and absolute limits (automatic logoff);
- not let a long-lived refresh token stand in for signing in: refresh tokens no longer than the sign-in session.

### 4.1 Microsoft Entra ID

1. **Register the API.** App registrations → New registration, `sdh-control-api`, single tenant. Under *Expose an
   API*, set the Application ID URI (for example `api://sdh-control`) and add a scope, `control.access`, that
   admins and users can consent to.
2. **Ask for version 2 tokens.** In the API registration's manifest, set `requestedAccessTokenVersion` to `2`.
   Then the issuer is `https://login.microsoftonline.com/<tenant-id>/v2.0` and `aud` is the API registration's
   **application (client) ID**, a GUID, not the Application ID URI. With version 1 tokens the issuer is
   `https://sts.windows.net/<tenant-id>/` and `aud` is the URI. Whichever you choose, set both settings to match.
3. **App roles, not groups.** Under *App roles*, add one role per line of the mapping (display name, value such as
   `sdh-operators`, allowed member types *Users/Groups*). In Enterprise applications → `sdh-control-api` → *Users
   and groups*, assign each role to its group; turn on *Assignment required*. App roles arrive in the `roles`
   claim. Group claims arrive as object IDs, and are left out of the token past a limit of groups (the *overage*),
   which the API can't follow; app roles have neither problem.
4. **The lifetime.** Entra ID gives an access token a random lifetime of 60 to 90 minutes by default, so most of
   its tokens would be refused. Create a token lifetime policy with `AccessTokenLifetime` of `01:00:00` (Microsoft
   Graph, `tokenLifetimePolicy`) and assign it to the API's service principal. Microsoft documents that tenants
   using Conditional Access *sign-in frequency* can't override the default variation, and that the policy isn't
   honoured for clients that negotiate continuous access evaluation; check a real token (§6) either way.
5. **The operators' client.** A second registration, `sdh-control-cli`, a public client: under *Authentication*,
   *Allow public client flows* (for the device code flow), or a mobile and desktop redirect URI
   `http://localhost` for code with PKCE. Under *API permissions*, add `sdh-control-api`'s `control.access`, and
   grant admin consent.
6. **MFA.** A Conditional Access policy for the operators' groups, with `sdh-control-api` as the target resource,
   granting access only with MFA (phishing-resistant strength where you can).

Settings: `issuer-uri: https://login.microsoftonline.com/<tenant-id>/v2.0`, `audience: <api-client-id>`,
`principal-claim: oid`, `roles-claim: roles`. `oid` is the person's object ID in the tenant: unique and never
reused. The scope a client asks for is `api://sdh-control/control.access`.

### 4.2 Okta

1. **A custom authorization server.** Security → API → Authorization Servers → Add, with the audience
   `sdh-control`. The org authorization server can't issue tokens for your own APIs. Its issuer is
   `https://<org>.okta.com/oauth2/<server-id>`, shown on the server's page.
2. **A scope,** `control.access`.
3. **A groups claim.** Claims → Add: name `groups`, include in the *access token*, value type *Groups*, filter
   *Starts with* `sdh-`, so only these groups go in the token.
4. **An access policy** for the operators' client, and a rule allowing its grant types (device authorization, or
   authorization code), the scope `control.access`, an access token lifetime of **1 hour** or less, and refresh
   tokens no longer than the session.
5. **The operators' client.** Applications → Create App Integration → OIDC → *Native application*, with the device
   authorization grant or authorization code with PKCE, assigned to the operators' groups.
6. **MFA.** An authentication policy on that application requiring two factors (phishing-resistant where you
   can).

Settings: `issuer-uri: https://<org>.okta.com/oauth2/<server-id>`, `audience: sdh-control`, `principal-claim: uid`,
`roles-claim: groups`. **Not `sub`:** in an Okta access token `sub` is by default the person's login, often an
e-mail address; `uid` is their Okta user ID, which isn't reused.

### 4.3 Keycloak

1. **A client for the operators,** `sdh-control-cli` in the realm: *Client authentication* off (public), *OAuth 2.0
   Device Authorization Grant* on, or *Standard flow* with PKCE method `S256`.
2. **The audience.** A client scope `sdh-control`, with an *Audience* mapper whose included custom audience is
   `sdh-control`, added to access tokens; add the scope to the client as a default scope. Without it, Keycloak's
   access tokens don't name the API.
3. **Groups.** Realm groups `sdh-viewers` and the rest. In the same client scope, a *Group Membership* mapper:
   token claim name `groups`, *Full group path* **off** (otherwise the values are `/sdh-viewers`), added to the
   access token.
4. **The lifetime.** Realm settings → Tokens → *Access Token Lifespan*: the default five minutes is fine; never
   over an hour. Set the SSO session idle and maximum to the customer's logoff limits.
5. **MFA.** Require OTP or WebAuthn in the browser and device flows (a required action, or a conditional step in
   the authentication flow), for the operators' groups at least.

Settings: `issuer-uri: https://<keycloak-host>/realms/<realm>`, `audience: sdh-control`, `principal-claim: sub`
(the user's ID, a UUID), `roles-claim: groups`.

### 4.4 Amazon Cognito

A Cognito user pool's access token carries `client_id` and **no `aud`**, unless it is requested with a resource
binding (the `resource` parameter, for a resource server you define). The control API checks `aud`, so only
resource-bound tokens work: define a resource server with the identifier `sdh-control` and have the operators'
client ask for `resource=sdh-control`. Groups arrive in `cognito:groups`. This path is the least certain of the
four: check that your client tool can send the parameter.

Settings: `issuer-uri: https://cognito-idp.<region>.amazonaws.com/<user-pool-id>`, `audience: sdh-control`,
`principal-claim: sub`, `roles-claim: cognito:groups`.

### 4.5 Other providers

Any provider works that issues **JWT access tokens for an API of your own**, with an asymmetric signature and a
discovery document at `<issuer>/.well-known/openid-configuration` (PingFederate, Auth0, ForgeRock, AD FS and
others). A provider whose access tokens are opaque, or are meant only for its own APIs, can't be used directly:
federate it through one that issues them. Where the pods can't reach the discovery document, set
`sdh.control.auth.jwt.jwk-set-uri` to a key set they can reach; the issuer is still checked.

## 5. In the pipeline

1. **The settings,** in each stage's ConfigMap `sdh-control-<stage>` (`application.yml`), the same on all four:

   ```yaml
   sdh:
     control:
       tls: { certificate: /run/control/tls.crt, key: /run/control/tls.key }
       auth:
         mode: jwt
         jwt:
           issuer-uri: https://<issuer>          # exactly the tokens' iss
           audience: sdh-control                 # exactly what the tokens' aud holds
           principal-claim: oid                  # §4: oid, uid or sub
           roles-claim: roles                    # §4: roles, groups or cognito:groups
       permissions:                              # the approved mapping; nothing is granted unless listed
         "[pipeline.view]": [sdh-viewers, sdh-operators]
         "[pipeline.operate]": [sdh-operators]
         "[phi.export]": [sdh-phi-export]
         "[phi.break-glass]": [sdh-break-glass]
         "[audit.read]": [sdh-auditors]
         "[audit.admin]": [sdh-audit-admins]
         "[keys.admin]": [sdh-key-admins]
         "[data.admin]": [sdh-data-admins]
   ```

   The values in `permissions` are those the roles claim carries: role values, group names, or (for Entra ID
   group claims) object IDs. The brackets keep each permission's dot.
2. **The egress.** The control pods' NetworkPolicy must let them reach the issuer on 443, for its discovery
   document and key set. Where the provider is outside the cluster's allowed egress, use `jwk-set-uri` (§4.5).
3. **Apply and restart:** `kubectl apply` the ConfigMaps, then `kubectl rollout restart deploy/sdh-control-<stage>`
   for each stage. A pod that can't read a setting it needs refuses to start and names it. A pod that can't reach
   the provider starts, and refuses calls until it can fetch the keys.
4. **The ingress,** CONTROL-API-DEPLOYMENT.md §2.2. It passes the `Authorization` header through and never logs it.

## 6. Getting a token, and checking it

### 6.1 A token on the operator's workstation

Use the customer's approved OIDC command-line client. Without one, the device authorization grant needs only
`curl` and `jq` (the endpoints come from the provider's discovery document):

```bash
issuer=https://<issuer>; client=<operators-client-id>; scope='openid <api-scope>'
conf=$(curl -sS "$issuer/.well-known/openid-configuration")
device=$(curl -sS -d "client_id=$client" --data-urlencode "scope=$scope" \
  "$(jq -r .device_authorization_endpoint <<<"$conf")")
echo "Open $(jq -r .verification_uri <<<"$device") and enter $(jq -r .user_code <<<"$device")"
while sleep "$(jq -r '.interval // 5' <<<"$device")"; do
  answer=$(curl -sS -d "client_id=$client" -d "device_code=$(jq -r .device_code <<<"$device")" \
    -d grant_type=urn:ietf:params:oauth:grant-type:device_code "$(jq -r .token_endpoint <<<"$conf")")
  token=$(jq -r '.access_token // empty' <<<"$answer") && [ -n "$token" ] && break
done
```

`<api-scope>` is `api://sdh-control/control.access` on Entra ID, `control.access` on Okta, and nothing more on
Keycloak, whose default scope adds the audience. Keep the token in a shell variable, never in a file or the shell
history, and don't paste it into a ticket: it is a credential for an hour.

### 6.2 Read the token before calling

```bash
jq -R 'split(".")[1] | gsub("-";"+") | gsub("_";"/") | . + ("=" * ((4 - length % 4) % 4)) | @base64d | fromjson
  | {iss, aud, lifetime: (.exp - .iat), oid, uid, sub, roles, groups}' <<<"$token"
```

Check `iss` and `aud` against the settings character for character, `lifetime` at 3600 or less, the principal
claim present, and the roles claim holding the values the mapping lists.

### 6.3 Call it

```bash
base=https://<control-host>/pipelines/<pipeline>/stages/split/api/v1
curl -sS --fail-with-body -H "Authorization: Bearer $token" "$base/status"
```

Then run CONTROL-API-DEPLOYMENT.md §8's checklist on each stage: no token, an expired token and a token for another
audience refused with `401`; a viewer's `POST /pause` refused with `403 permission-required`; the same mapping on
every stage in `GET /settings`; and each refusal in the audit (`control.unauthenticated`, or the operation's event
with `decision=denied`). Record the results with the change.

## 7. When it doesn't work

The API's `401` says only `unauthenticated`, and its audit event names the failure's class, never a claim's value,
so read the token (§6.2) first.

| What you see | Likely cause | Fix |
|---|---|---|
| `401` on every call; the token looks right | `iss` differs from `issuer-uri`, often by a trailing `/` or a version path (Entra ID `v2.0`) | copy `iss` from a real token |
| `401`; `aud` holds something else | Entra ID: version 2 tokens carry the client ID; Keycloak: no audience mapper; Cognito: no resource binding | §4.1 step 2, §4.3 step 2, §4.4 |
| `401` for some tokens, not others | the lifetime is over an hour: Entra ID's default 60–90 minutes | §4.1 step 4 |
| `401` with an HS256 token | an HMAC-signed token | sign with the provider's asymmetric key |
| `401` until a pod restarts | the pods couldn't reach the issuer's key set, or the provider rotated keys and they can't fetch the new ones | §5 step 2 |
| `403 permission-required` for someone who should have it | the roles claim is missing, nested, a full group path (`/sdh-operators`), group IDs where the mapping has names, or past Entra ID's group overage | §4; compare §6.2's output with `GET /settings` |
| `403` only on one stage | that stage's mapping differs | the same ConfigMap on all four (§5) |
| A person removed from a group still gets in | their current token is still valid | at most an hour (`max-lifetime`); to stop them now, disable the account and remove their sealing keys (IDENTITY.md §8) |
| The audit names the wrong person, or changes their name | the principal claim is an e-mail or login, or was changed | §3 step 3; re-register their sealing keys under the new name |

## 8. Findings

- **No real identity provider has been connected.** The harness uses its own test issuer; §4's steps are from the
  providers' documentation, untested here.
- **Entra ID's default token lifetime is over the API's limit.** Without a token lifetime policy, most Entra ID
  tokens are refused, and tenants using sign-in frequency can't set one. A `max-lifetime` above an hour would need
  a waiver of `control-token-lifetime`; a better fix is to check the time left (`exp`) instead of `exp − iat`, or
  to allow for the provider's variation. Recorded in KNOWN-GAPS.md §5.
- **The roles claim can't be nested,** so providers that put roles under an object (Keycloak's `realm_access`) need
  a mapper. A claim path setting would remove the step.
- **Cognito needs resource-bound tokens** for an `aud`; accepting `client_id` as the audience for Cognito would be
  an alternative.
- **A refusal doesn't say why,** even to an administrator: the audit names only the failure's class. A reason class
  (issuer, audience, lifetime, signature, no principal), still naming no claim's value, would shorten §7.

## Sources

| Statement | Source |
|---|---|
| The token checks, the lifetime as `exp − iat`, the signing algorithms, key set discovery | `connector-stage` `control/ControlConfiguration` (`jwtDecoder`, `validator`, `lifetime`, `SIGNATURES`) |
| The roles claim: a top-level list or space-separated string | `ControlConfiguration.roles`; `ControlSettings.Jwt` |
| The refusal's audit event | `ControlConfiguration.unauthenticated`; AUDIT-EVENTS.md §6.3 |
| Control Deployments, Services, ports, paths | each kit's README, "Control API"; CONTROL-API-DEPLOYMENT.md §1, §2 |
| The harness's issuer | `control-harness` `TestIssuer` |
| Entra ID: 60–90 minute default, token lifetime policies and their limits | [Microsoft: access tokens](https://learn.microsoft.com/en-us/entra/identity-platform/access-tokens); [Microsoft: configurable token lifetimes](https://learn.microsoft.com/en-us/entra/identity-platform/configurable-token-lifetimes) |
| Okta: custom authorization servers, `uid` and `sub`, the groups claim | [Okta: authorization servers](https://developer.okta.com/docs/concepts/auth-servers/); [Okta: claims and scopes](https://developer.okta.com/docs/concepts/oauth-claims/); [Okta: groups claim on a custom authorization server](https://developer.okta.com/docs/guides/customize-tokens-groups-claim/add-groups-claim-custom-as/) |
| Cognito: `client_id` and no `aud` unless resource-bound | [AWS: the access token](https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-using-the-access-token.html); [AWS: verifying JWTs](https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-using-tokens-verifying-a-jwt.html) |
