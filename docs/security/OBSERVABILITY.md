# Wiring observability: the stages, the collector and your tools

For the customer's platform administrator and whoever runs their monitoring: what each pipeline pod sends, what to
set on the stages, how to deploy the OpenTelemetry Collector that receives it, where to send each kind of signal,
what to watch and alert on, and how to check it works. Where the audit events must end up, and why, is
[HIPAA-OPERATIONS.md](../HIPAA-OPERATIONS.md) §2.7: this document gives the whole collector, and points there for
the audit store. What each audit event means is [AUDIT-EVENTS.md](AUDIT-EVENTS.md); the design is DESIGN.md §12.

**What has been tried.** The sandbox sends all three signals to Grafana's LGTM image through a collector
(`deploy/sandbox/infra/telemetry.yaml`). The AWS PoC sends metrics to Prometheus's OTLP receiver
(`deploy/aws/k8s/infra/telemetry.yaml`), and drops traces and logs. The other back ends in §5 follow their own
documentation and haven't been tried here.

## 1. What the pipeline sends

```
pipeline namespace(s)                            monitoring namespace          your tools
┌───────────────────────────┐                    ┌──────────────────────────┐
│ landing, split, convert,  │ OTLP/HTTP protobuf │ OpenTelemetry Collector  │ logs ────► SIEM / audit store
│ transform workers         │ ─────────────────► │ 2 replicas, port 4318    │ metrics ─► Prometheus, Mimir…
│ sdh-control-<stage> pods  │ /v1/logs           │ receivers → processors → │ traces ──► Tempo, Jaeger, X-Ray…
│ (every pod, push only)    │ /v1/metrics        │ exporters                │
│                           │ /v1/traces         │                          │
└───────────────────────────┘                    └──────────────────────────┘
```

Every pod of every stage, the control pods included, **pushes** to one endpoint, `sdh.telemetry.otlp.endpoint`,
over **OTLP/HTTP with protobuf**: `/v1/logs`, `/v1/metrics` and `/v1/traces` are appended to it. There is no gRPC
exporter and nothing to scrape: no pod serves a `/metrics` page.

| Signal | What it is | How much |
|---|---|---|
| **Logs** | **Only audit events**, as log records named `sdh.audit.<event>`, with allow-listed attributes and the event name as the body. No other log line is exported | a few per item, sent at once |
| **Metrics** | the stages' own (`sdh.*`, §6), the JVM's (`jvm.*`, `process.*`, `system.*`), and a few about the OpenTelemetry SDK itself | about 70 JVM series a pod, and the stage's own, every 60 s |
| **Traces** | from the landing node (`land.group`, `land.item`) and the split stage (one per file); the convert and transform stages send none yet. Every one sampled | a few spans per item |

**None of it holds PHI.** Every attribute passes an allow-list at the source and again at export, exception
messages are never recorded (only their classes), and free-text log lines never leave the pod (DESIGN.md §12). So
a stage may send plain HTTP to a collector **inside the cluster**; the profile allows it. Beyond the collector, use
TLS, because the audit events and the metrics describe what the pipeline did and when.

**The pods' standard output** holds the product's own log lines and the audit events again (logger `sdh.audit`).
None of it holds a document's content (DATA-FLOWS.md), but library lines aren't filtered as the export is. If you
collect container logs with a node agent, keep them under the same access as the audit store, and don't treat them
as the audit record: the journal and the export are.

## 2. On the stages

### 2.1 The settings

In each stage's `application.yml` (the workers' ConfigMap; the control pods read the same file):

| Setting | Default | |
|---|---|---|
| `sdh.telemetry.otlp.endpoint` | none | the collector's OTLP/HTTP base URL, such as `http://otel-collector.<monitoring-ns>.svc:4318`. **Required under the `hipaa` profile** (`audit-export`). Unset, nothing is exported |
| `sdh.telemetry.otlp.headers` | none | request headers, for a collector that needs a token, such as `Authorization`. Take the value from a Secret, through an environment variable, never in the ConfigMap |
| `sdh.telemetry.otlp.timeout` | `10s` | per export request |
| `sdh.telemetry.metric-interval` | `60s` | how often metrics are sent. Shorter gives finer graphs and more series points |
| `sdh.telemetry.jvm-metrics` | `true` | `false` stops the JVM's metrics |

```yaml
sdh:
  telemetry:
    otlp:
      endpoint: "http://otel-collector.sdh-monitoring.svc:4318"
      # headers: { Authorization: "${OTLP_AUTHORIZATION}" }    # only for a collector outside the cluster
```

For a collector outside the cluster, use `https://`. The exporter checks the collector's certificate against the
JVM's default trust store, so a certificate from your internal CA needs that CA added to it; this hasn't been tried
(§9). A collector inside the cluster, relaying onward with TLS, avoids the question.

### 2.2 Name each pod, and its pipeline

Each stage reports its `service.name`: `landing-connector`, `file-connector` (the split stage), `convert-stage` or
`transform-stage`. Its control pods use the same name. **Every kit Deployment, workers and control, sets in its
pod template** (fill in `<pipeline>`; keep it if you write your own manifests):

```yaml
env:
  - name: POD_NAME
    valueFrom: { fieldRef: { fieldPath: metadata.name } }
  - name: OTEL_RESOURCE_ATTRIBUTES
    value: "service.instance.id=$(POD_NAME),service.namespace=<pipeline>"
```

- **`service.instance.id`, one per pod, is not optional.** Without it every pod of a stage writes the same series:
  two pods' counters interleave, Prometheus reads each drop as a reset, and rates come out tens of times too high.
  We found that on the PoC (PERFORMANCE.md §10).
- **`service.namespace=<pipeline>`** tells two pipelines' stages apart, since they share service names.
- Instead of the variables, the collector's `k8sattributes` processor can add the pod's name and namespace, given
  read access to pods through its own service account. The variables need no extra rights.

### 2.3 The network

The pipeline's NetworkPolicies must let every stage pod, control pods included, reach the collector on 4318
(NETWORK.md). `deploy/reference/network-policies.yaml` (`to-platform`) allows 4318 to the platform namespace
(`${PLATFORM_NAMESPACE}`); the collector's own policy must let the pipeline namespaces in, as the PoC's do for
namespaces labelled `sdh.smiledigitalhealth.com/infra-client`.

## 3. The collector

Use the OpenTelemetry Collector's **contrib** distribution (we test with `otel/opentelemetry-collector-contrib`
0.162.0), or a provider's build of it such as ADOT on AWS. Check exporter options against the version you deploy.

### 3.1 Deploying it

- **Two replicas** on different nodes, behind a Service `otel-collector` on 4318, with a PodDisruptionBudget of
  `minAvailable: 1`. A stage's export is best effort: what is lost while no collector answers is still in the audit
  journal (HIPAA-OPERATIONS.md §3.2), but not in the metrics or traces.
- **A persistent volume** per replica for the logs exporter's queue (a StatefulSet, or one volume each), so audit
  events waiting for the store survive a restart.
- **Memory:** 256 MiB was enough for one pipeline at the PoC's rates; set `memory_limiter` to 80% of the limit.
- **Hardened** like the stages: non-root, read-only root filesystem, no capabilities, seccomp `RuntimeDefault`
  (the sandbox's manifest has all four).
- **Its own NetworkPolicy:** in, from the pipeline namespaces on 4318; out, only to the back ends' endpoints and
  DNS.

### 3.2 A configuration with all three pipelines

This sends audit logs to the SIEM's OTLP intake, metrics to Prometheus, and traces to Tempo. §5 gives the exporters
for other back ends; swap them in without changing the rest.

```yaml
receivers:
  otlp:
    protocols:
      http: { endpoint: "0.0.0.0:4318" }   # the stages use HTTP only; leave gRPC off
processors:
  memory_limiter: { check_interval: 1s, limit_percentage: 80, spike_limit_percentage: 20 }
  batch: {}
extensions:
  health_check: { endpoint: "0.0.0.0:13133" }
  file_storage/audit: { directory: /var/lib/otelcol/audit }   # on the persistent volume
exporters:
  otlphttp/audit:                                 # the audit store: HIPAA-OPERATIONS.md §2.7 for the choices
    endpoint: https://siem.example.internal/otlp
    headers: { Authorization: "${env:SIEM_TOKEN}" }
    tls: { ca_file: /etc/otelcol/ca/ca.crt }
    sending_queue: { enabled: true, storage: file_storage/audit }
    retry_on_failure: { enabled: true, max_elapsed_time: 0 }   # 0: keep retrying, never drop
  otlphttp/prometheus:
    metrics_endpoint: http://prometheus.sdh-monitoring.svc:9090/api/v1/otlp/v1/metrics
  otlphttp/tempo:
    endpoint: http://tempo.sdh-monitoring.svc:4318
service:
  extensions: [health_check, file_storage/audit]
  pipelines:
    logs:    { receivers: [otlp], processors: [memory_limiter, batch], exporters: [otlphttp/audit] }
    metrics: { receivers: [otlp], processors: [memory_limiter, batch], exporters: [otlphttp/prometheus] }
    traces:  { receivers: [otlp], processors: [memory_limiter, batch], exporters: [otlphttp/tempo] }
```

The rules that matter:

- **The logs pipeline keeps everything.** No `filter`, sampling or `nop` exporter on it: dropping an `sdh.audit.*`
  record is an audit gap. Everything on it is an audit event, so it needs no routing.
- **Metrics and traces may be thinned,** if volume costs too much: a tail-sampling processor on traces, or a
  `filter` dropping `jvm.*` on metrics. Never drop `sdh.*` metrics you alert on (§6).
- **Each back end gets its own exporter and queue,** so an outage of one doesn't hold up the others.
- **Don't use `nop`** for logs under the profile, as the PoC still does (HIPAA-OPERATIONS.md §2.7).

## 4. Prometheus and Grafana, as tried on the PoC

Prometheus 3 takes OTLP directly; nothing scrapes the stages.

- Start it with `--web.enable-otlp-receiver`, and in `prometheus.yml` promote the pod's name to a label:

  ```yaml
  otlp:
    promote_resource_attributes: [service.instance.id, service.namespace, k8s.namespace.name]
  ```

- `service.name` becomes the `job` label, and `service.instance.id` the `instance` label.
- **Names change on the way in:** dots become underscores, a counter gains `_total`, and a unit is added:
  `sdh.items.completed` is `sdh_items_completed_total`, `sdh.commit.duration` is
  `sdh_commit_duration_seconds_bucket`, `_sum` and `_count`, and `sdh.process.cpu_time` is
  `sdh_process_cpu_time_seconds_total`. Check a few in Prometheus before writing alert rules.
- Grafana reads Prometheus (and Tempo for traces) as data sources. The sandbox's LGTM image bundles all of them;
  it suits a test cluster, not production.

## 5. Other back ends

| Back end | Metrics | Traces | Identity |
|---|---|---|---|
| **Grafana Mimir, Cortex, Thanos** | `prometheusremotewrite`, or `otlphttp` to Mimir's OTLP endpoint | `otlphttp` to Tempo | a tenant header and token from a Secret |
| **Grafana Cloud** | `otlphttp` to the stack's OTLP gateway (all three signals) | the same | basic auth: instance ID and token, from a Secret (`basicauth` extension) |
| **AWS** | `prometheusremotewrite` with the `sigv4auth` extension to Amazon Managed Service for Prometheus; or `awsemf` to CloudWatch metrics | `awsxray` to X-Ray | an IAM role for the collector's service account (IRSA or EKS Pod Identity), limited to writing; VPC endpoints for the services in private subnets |
| **Azure Monitor** | `azuremonitor` to Application Insights | the same | the connection string in a Secret, or Entra authentication where the exporter supports it |
| **Google Cloud** | `googlemanagedprometheus` | `googlecloud` (Cloud Trace) | Workload Identity, with a Google service account allowed to write metrics and traces |
| **Datadog** | `datadog` | the same | an API key in a Secret |
| **Splunk Observability** | `signalfx` | `otlphttp` to its trace endpoint | an access token in a Secret |
| **Elastic** | `otlphttp` to Elastic's OTLP intake, or `elasticsearch` | the same | an API key in a Secret |

The audit logs go to a store the pipeline's own staff can't change, chosen in HIPAA-OPERATIONS.md §2.7, whatever
the metrics and traces use. It may be the same product (Splunk, Elastic, a Loki tenant) if access to it is kept
apart. Every link leaving the cluster uses TLS, checked against the customer's CA.

## 6. What to watch

The stages' own metrics. The names are as they leave the pod; §4 gives Prometheus's form.

| Metric | Type | Stage | Shows |
|---|---|---|---|
| `sdh.intake.up` | gauge (0/1) | landing | whether the source can be read |
| `sdh.intake.last_poll_age` | gauge (s) | landing | time since the source was last listed |
| `sdh.intake.outage_seconds` | gauge (s) | landing | how long the source has been unreachable |
| `sdh.landing.backlog`, `sdh.landing.oldest_waiting_age` | gauge | landing | items seen and not yet landed, and the oldest's age |
| `sdh.landing.items` | counter | landing | items landed and announced |
| `sdh.inbound.oldest_file_age` | gauge (s) | split | the oldest file waiting |
| `sdh.items.in_progress`, `sdh.item.progress_ratio` | gauge | split | files being split, and how far |
| `sdh.items.completed`, `sdh.items.failed` | counter | split | files done; failed by error code and stage |
| `sdh.records.stored`, `sdh.records.published`, `sdh.records.failed` | counter | all | records committed, sent on, and kept in their original form after failing |
| `sdh.stage.failures`, `sdh.stage.failures_open`, `sdh.stage.failures_oldest_age` | counter, gauges | all | failures recorded; open now; the oldest open one's age |
| `sdh.stage.version_differs`, `sdh.stage.superseded_on_arrival`, `sdh.stage.reprocessed` | counter | record stages | records seen again under another version, stored after a newer one, or reprocessed |
| `sdh.commit.duration`, `sdh.publish.duration`, `sdh.stage.phase_duration` | histogram (s) | all | time per commit, per send, and per phase of an item |
| `sdh.push.requests`, `sdh.push.wait_duration` | counter, histogram | landing (HTTP push) | pushes by result, and their wait |
| `sdh.process.cpu_time`, `sdh.process.cpu_wait`, `sdh.process.gc_time` | counter (s) | all | the container's CPU, time throttled, time in GC |
| `jvm.memory.used`, `jvm.gc.live.data.size`, `jvm.threads.live` | gauge | all | heap by pool, the heap a stage really needs, threads |

### 6.1 Alerts to start with

Tune the thresholds to your feeds after a few weeks; these are starting points.

| Alert | Condition | Why |
|---|---|---|
| Source unreachable | `sdh.intake.up` = 0 for 5 min, or `sdh.intake.outage_seconds` > 600 | data isn't arriving |
| Landing stalled | `sdh.intake.last_poll_age` > 5 × the poll interval | the landing node has stopped |
| Backlog growing | `sdh.landing.oldest_waiting_age` or `sdh.inbound.oldest_file_age` above your latency target (minutes) | the pipeline is falling behind |
| Failures open | `sdh.stage.failures_open` > 0 for an hour, or `sdh.stage.failures_oldest_age` > 1 day | records waiting for someone to correct them (HIPAA-OPERATIONS.md, failures) |
| Failures rising | rate of `sdh.stage.failures` or `sdh.items.failed` above its usual | a source or mapping problem |
| No progress | rate of `sdh.records.stored` = 0 for 15 min on a stage while the one before it stores | a stage is stuck |
| CPU throttled | rate of `sdh.process.cpu_wait` / rate of `sdh.process.cpu_time` > 0.25 | raise the CPU limit |
| Heap near full | `jvm.gc.live.data.size` / `jvm.memory.max` (heap) > 0.8 | raise the memory before it fails |
| Telemetry missing | no series from a stage's `job` for 10 min | the export or the collector is down; audit events are waiting in the journal |

**Security alerts come from the audit events, in the SIEM,** not from metrics: HIPAA-OPERATIONS.md §3.1 lists the
events to review and alert on (`connector.fatal`, `lease.lost`, `compliance.*`, `admin.*`,
`control.unauthenticated` and the rest).

### 6.2 Traces

A trace follows one item through one stage: at the landing node `land.group` and `land.item`; at the split stage
`ingest.item` with `split.batch`, `storage.commit` and `publish.batch` beneath it. Root spans carry `sdh.item_id`
and `sdh.result`; a failed item's root span has status `ERROR` with `sdh.error_code`. To find an item, search the
trace store for `sdh.item_id`, which its audit events also carry. The convert and transform stages send no traces:
their timings are in `sdh.stage.phase_duration`. Traces don't join across stages.

## 7. Checking it works

Once after setup, and after any change to the collector or a back end:

1. **The collector is up:** `kubectl get pods -l app=otel-collector` ready on both replicas; its health check on
   13133 answers.
2. **Metrics arrive from every pod:** in Prometheus,
   `count by (job, instance) (sdh_process_cpu_time_seconds_total)` names every stage pod and control pod, each
   once.
3. **Audit events arrive:** call `GET /api/v1/status` on a stage, and find its `sdh.audit.control.status` event in
   the audit store within a minute (`sdh.audit.admin.status` through the CLI). HIPAA-OPERATIONS.md §2.7, "Checking
   it works", continues with the counts and the outage test.
4. **A trace arrives:** land one test document; find its `sdh.item_id` in the trace store (landing and split).
5. **An alert fires:** make a test pipeline's source unreachable, and see the "Source unreachable" alert.
6. **Nothing else leaks:** search the back ends for a test document's marker text and its file name; neither may be
   found.
7. **Under the profile:** `GET /api/v1/compliance` on each stage shows no `audit-export` finding.

## 8. Without Kubernetes

On hosts (the distribution kit), run the collector on each host or one host nearby, and set
`sdh.telemetry.otlp.endpoint` to it. Set `OTEL_RESOURCE_ATTRIBUTES` in each stage's service unit, with the host
name as `service.instance.id`. The collector configuration of §3.2 applies unchanged, with the file storage on
local disk.

## 9. Findings

- **`service.instance.id` was missing from the kits** until 2026-11-27; only the AWS PoC's patches set it, and a
  stage with more than one pod reported wrong counts. The kits' Deployments now set it (§2.2), and the sandbox,
  built from them, fills in the pipeline. A deployment from older kit READMEs needs it added.
- **The pipeline's name is filled in by hand.** A stage reports only its `service.name`, and the kits add
  `service.namespace=<pipeline>`. A `sdh.pipeline` resource attribute from the settings would make it automatic.
- **No dashboards or alert rules are shipped.** §6.1's alerts are a starting point written here, not tested rules.
  A Grafana dashboard and Prometheus rule file in the kits would save each customer writing them.
- **Traces stop at the split stage, and don't join across stages:** the convert and transform stages create no
  spans, and a record's path from landing to gold is followed by `sdh.item_id`, not one trace.
- **A collector with a private CA's certificate is untried:** the OTLP exporter uses the JVM's default trust store,
  and no kit says how to add a CA to it for this link.
- **Only Prometheus's OTLP receiver and Grafana LGTM have been run.** The other back ends of §5 are untested.

## Sources

| Statement | Source |
|---|---|
| Endpoint, paths, headers, timeout, interval, JVM metrics | `connector-runtime` `CommonSettings.Telemetry`, `.Otlp`; `telemetry/TelemetryConfiguration`, `JvmMetricsExport`; DESIGN.md §12.1, §12.2 |
| Only audit events exported as logs; the allow-lists | `AuditOnlyLogRecordExporter`, `AllowListSpanExporter`; DESIGN.md §12 |
| `audit-export` under the profile | `ComplianceRules` (`OTLP_ENDPOINT`); CONFIGURATION.md |
| Service names | each stage's `application.yml` (`spring.application.name`) |
| Metric names and types | `connector-core` `telemetry/Telemetry`, `ProcessMetrics`; `FailureStore`; `landing-connector` `LandingConfiguration` |
| `service.instance.id` and the interleaved counters | `deploy/aws/k8s/pipeline/*-patch.yaml`; PERFORMANCE.md §10 |
| Prometheus's OTLP receiver, promoted attributes | `deploy/aws/k8s/infra/telemetry.yaml` |
| The collector, LGTM | `deploy/sandbox/infra/telemetry.yaml`; DESIGN.md §12.2 `FullStackIT` |
| The audit store and its rules | HIPAA-OPERATIONS.md §2.7, §3.1 |
