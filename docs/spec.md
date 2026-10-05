# Observability Pipeline: v1.0 Specification

Status: draft for build. Date: 2026-10-03. Companion to `design.md`, which explains the reasoning.

## 0. Conventions

MUST means required for v1.0. SHOULD means expected unless there is a recorded reason. Values marked *(default)* were proposed during design and can change without revisiting a design decision. Everything else was decided. Items tagged VER-n are verification tasks listed in section 13.

## 1. Pinned versions

- Python 3.13, uv workspace (one `pyproject.toml` per service, a committed `uv.lock`).
- FastAPI, SQLAlchemy 2.x, psycopg 3, httpx for the three services, run with uvicorn.
- Kafka broker 4.2 or later. `confluent-kafka` (librdkafka) client for the job, version pinned in `uv.lock`.
- OpenTelemetry Python SDK and instrumentation packages (`opentelemetry-instrumentation-*`), pinned to exact versions in `uv.lock`.
- OpenTelemetry Collector contrib image, pinned to an exact tag recorded in the repository (the Kafka components and `deltatocumulative` live in contrib; see VER-2).
- VictoriaMetrics single-node, Jaeger v2, Grafana, Postgres, k6: pinned image tags.

### 1.1 What you install

Running the stack needs only Docker (FR-C1). Everything else is a container image, so none of it is installed on the host. Building and testing outside Docker needs the rest.

| Tool | Needed for | Required |
| --- | --- | --- |
| Docker Engine or Docker Desktop, with Compose v2 (`docker compose`) | Running the whole stack: Kafka, Postgres, Collector, Jaeger, VictoriaMetrics, Grafana, the three services and the windowing job | MUST, for running |
| Git | Cloning the repository | MUST |
| uv | Python toolchain: installs Python 3.13, resolves the workspace, runs TST-1 to TST-7 on the host (`uv run pytest`) | MUST for development, not for running |
| k6 | Running the load and both experiments (section 10). A host install, or the `grafana/k6` image through Docker *(default)* | MUST for the experiments |
| `curl` | The README step that confirms spans arrive (see `design.md` 4.3) and quick manual requests | SHOULD |

Not installed on the host: Kafka, Postgres, the Collector, Jaeger, VictoriaMetrics, Grafana, and the Python dependencies of the services (locked in `uv.lock` and installed into the service images, see FR-S4).

The README MUST repeat this list with the minimum Docker memory allocation. That figure comes from FR-C5, once measured, and is not proposed here.

## 2. Repository layout

The repository is named `tracing-platform` *(default)*. It is a single uv workspace.

```
tracing-platform/
pyproject.toml               workspace root, Python 3.13, uv.lock
services/gateway
services/orders
services/inventory
streams/red-metrics          the windowing job (plain confluent-kafka consumer)
deploy/                      docker-compose.yml, collector config,
                             Grafana provisioning, init scripts
experiments/                 k6 scripts and experiment runbooks
docs/design.md, docs/spec.md, docs/adr/
README.md
```

## 3. Runtime (Compose)

- FR-C1: `docker compose up` MUST start the whole stack with no manual steps beyond installing Docker.
- FR-C2: Kafka runs as a single KRaft broker. A one-shot init container creates the topics in section 6 before dependent services start.
- FR-C3: Services SHOULD declare health checks, and `depends_on` SHOULD use health conditions so startup order is deterministic.
- FR-C4: Postgres is seeded at startup with 100 orders of 3 to 6 items each *(default)*.
- FR-C5: Peak memory of the full stack MUST be measured once and recorded in the README.

## 4. Services

- FR-S1: gateway exposes `GET /api/orders` and `GET /api/orders/{id}`, which call orders, and `GET /api/ping`, which returns immediately and calls nothing downstream.
- FR-S2: orders exposes `GET /orders` (first 20 orders *(default)*, with items loaded lazily so one query for orders is followed by one query per order, the N+1 pattern; SQLAlchemy lazy loading provides it) and `GET /orders/{id}`.
- FR-S3: `GET /orders/{id}` in orders calls inventory `GET /inventory/{sku}` once per item. The k6 detail request MUST use a fixed order ID whose item count is documented, so expected downstream counts are computable.
- FR-S4: All three services run under `opentelemetry-instrument` (OpenTelemetry Python auto-instrumentation for FastAPI, httpx, and SQLAlchemy). Environment sets `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, and `OTEL_EXPORTER_OTLP_PROTOCOL` explicitly instead of relying on defaults, and a metric export interval of 15 seconds *(default)*.
- FR-S5: At least one service (orders *(default)*) MUST contain manual spans using the OpenTelemetry API. SHOULD include one span executed on a separate `ThreadPoolExecutor` with context explicitly propagated (`contextvars.copy_context().run`, or `opentelemetry.context` attach and detach).
- FR-S6: Each service reads `FAULT_ERROR_EVERY_N` (integer, 0 disables) and `FAULT_LATENCY_MS` (integer, 0 disables) at startup. When N is set, a shared counter fails every Nth request with HTTP 500. The counter MUST be incremented under a lock or in a single event-loop step, and each service MUST run a single uvicorn worker process, otherwise per-process counters break the exact-count oracle. Requests to `/api/ping` and to health endpoints MUST be excluded from the counter.
- FR-S7: When a downstream call fails, gateway MUST return a 5xx so the gateway SERVER span carries status ERROR.

## 5. Collector

One Collector instance with these pipelines (exact keys verified in the weeks 3 and 9 phases, see VER-1 to VER-3):

- FR-O1: Traces pipeline: OTLP receiver (gRPC 4317 and HTTP 4318) to exporters for Jaeger and for Kafka topic `otel.spans.raw`, encoding `otlp_proto`, partitioned by trace ID.
- FR-O2: Application metrics pipeline (interim): OTLP receiver to Prometheus remote write exporter pointing at VictoriaMetrics.
- FR-O3: RED metrics pipeline: Kafka receiver on topic `otel.red.metrics`, encoding `otlp_proto`, then `deltatocumulative`, then Prometheus remote write to VictoriaMetrics.
- FR-O4: A `memory_limiter` processor SHOULD precede other processors.

## 6. Kafka topics

- FR-K1: `otel.spans.raw`: 3 partitions, replication factor 1, retention 1 hour *(default)*. Key is the trace ID. Value is an OTLP traces message as written by the Collector, which may contain several spans.
- FR-K2: `otel.red.metrics`: 1 partition, replication factor 1 *(default)*. Value is an OTLP metrics message.
- FR-K3: `red-metrics.state`: 1 partition, replication factor 1, compacted. Holds the last-emitted-window watermark (FR-J9).

## 7. Job `red-metrics`

A plain `confluent-kafka` consumer and transactional producer. There is no stream-processing framework, so windowing, grace, suppression, and state recovery are written by hand. The window logic MUST be a pure class (spans and stream time in, closed windows out, no Kafka imports) so it can be tested without a broker (section 11).

- FR-J1: Consumer group `red-metrics`. Offsets are committed only through `send_offsets_to_transaction`, and output is produced inside the same transaction (`transactional.id` fixed, `isolation.level=read_committed`). Exactly-once applies to the topic-to-topic hop only and is not switchable.
- FR-J2: Step 1 splits each OTLP message into individual spans, reading `service.name` from the resource (spans without it use `unknown_service`), and skips spans whose end time is zero or earlier than their start, counting them in a log line or counter.
- FR-J3: Step 2 derives the key `service|span name|span kind|status code` and the duration in milliseconds, and assigns the span to a window by its end time (event time), not by the Kafka record timestamp.
- FR-J4: The job reads all three partitions of `otel.spans.raw` in one process, so a single in-memory aggregation sees every span. No repartition topic is needed. Stream time is the maximum span end time seen so far across all partitions.
- FR-J5: Step 3 aggregates per key into tumbling windows of 15 seconds with 30 seconds of grace, holding a span count and a count per histogram bucket.
- FR-J6: Step 4 emits each window exactly once, when stream time passes window end plus grace. Spans arriving for an already-emitted window are dropped and counted (FR-J8). The buffer is unbounded in v1.0, recorded as a known limitation.
- FR-J7: Step 5 converts each emitted window to OTLP metrics with delta temporality (FR-M1), using `opentelemetry-proto`, and produces them to `otel.red.metrics`.
- FR-J8: Records dropped for arriving after grace MUST be counted and logged, with the count per window.
- FR-J9: State recovery. Open-window state lives in memory, so a crash loses it. The job MUST commit, per partition, only the offset of the earliest span still contributing to an open window (the low watermark), and MUST record the end of the last emitted window in the same transaction (a record on a compacted `red-metrics.state` topic). On restart it replays from the committed offsets and skips windows at or before the recorded watermark. See VER-4.

## 8. Output metric contract

- FR-M1: Two metrics, both delta temporality, with start time equal to the window start and end time equal to the window end: `red.span.count` (monotonic sum, unit `{span}`) and `red.span.duration` (explicit-bucket histogram, unit `ms`).
- FR-M2: Attributes on both: `service.name`, `span.name`, `span.kind`, `status.code`.
- FR-M3: Histogram bounds in ms: 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000 *(default)*.
- FR-M4: The final series and label names in VictoriaMetrics are produced by the remote-write conversion. They MUST be recorded in the README once observed (VER-6), and dashboards and oracles use the observed names.

## 9. Dashboard

- FR-D1: One Grafana dashboard, provisioned from files, showing request rate, error rate, and p50 and p95 duration per service and operation.
- FR-D2: Every panel MUST filter on `span_kind=SERVER`.
- FR-D3: Grafana has Jaeger and VictoriaMetrics as provisioned data sources.

## 10. Load and experiments

**k6 script.**

- EXP-L1: Scenario `load` uses a shared-iterations executor with a configurable even N. Iteration number modulo 2 selects `GET /api/orders` or `GET /api/orders/{id}` with the fixed order ID, so each gets exactly N/2 requests.
- EXP-L2: Scenario `heartbeat` calls `GET /api/ping` at 1 request per second from time zero, for a duration at least 90 seconds longer than the load phase. Its operation is excluded from every oracle.

**Oracle.** With faults off, the SERVER span count for gateway on each of the two operations equals N/2, and the sum of both equals N. With `FAULT_ERROR_EVERY_N=E` set on orders, the ERROR SERVER span count on orders equals floor(M/E), where M is the number of requests that reached orders, and gateway ERROR counts match (FR-S7).

**Experiment 1: job crash mid-window.** Run the load with heartbeat. While a window is open, kill the `red-metrics` container without a graceful stop, wait 20 seconds, restart it, and let the load finish. After the heartbeat has pushed stream time past the last window plus grace, sum the delta counts for gateway SERVER spans. The experiment records expected N, observed total, and the per-window distribution. The hypothesis is an exact match; any difference is itself the finding and must be explained.

**Experiment 2: Collector restart.** Run steady load with heartbeat, restart the Collector, and record what the cumulative counter series does (reset, gap, duplication), the increase over the run compared with N, and consumer-group lag on `otel.red.metrics` before and after. No outcome is presupposed.

Both experiments MUST be scripted so a reader can rerun them, and the README reports results with the exact commands used.

## 11. Unit tests

Using pytest against the pure window class (FR-J, section 7), no broker, with stream time passed in explicitly:

- TST-1: A span inside a window is counted exactly once.
- TST-2: A span arriving within grace after its window's end is counted in its original window.
- TST-3: A span arriving after grace is dropped and not counted.
- TST-4: Each window is emitted once per key, and only after stream time passes window end plus grace.
- TST-5: Duration histogram bucket boundaries are assigned correctly (a value equal to a bound lands in the documented bucket).
- TST-6: Spans differing in service, name, kind, or status produce separate keys.
- TST-7: After a simulated restart that replays from the low-watermark offset, windows at or before the recorded watermark are not emitted again, and open windows are rebuilt with the same counts (FR-J9).

## 12. Acceptance criteria for v1.0

- AC-1: `docker compose up` starts the stack from a clean checkout.
- AC-2: After a k6 run of N requests, once the heartbeat has advanced stream time beyond the last real window plus grace, the span-derived request count in VictoriaMetrics for gateway SERVER spans equals N. Without ongoing traffic the final window does not close, so the heartbeat is part of the criterion; the result is visible at least 45 seconds of event time after the last real request.
- AC-3: With the error counter set, the error oracle in section 10 holds.
- AC-4: Experiment 1 is documented with exact counts.
- AC-5: Experiment 2 is documented with the observed counter behavior.
- AC-6: TST-1 to TST-7 pass.
- AC-7: The README has an architecture diagram, the measured memory figure, a trace screenshot showing the N+1 SQL spans, and links to the ADRs.
- AC-8: The repository is tagged v1.0.

## 13. Documentation deliverables

README with architecture, one-command run, experiment results, and measured numbers. ADRs written when each decision is made, in this order: Compose-only runtime; own services with deterministic fault injection; OpenTelemetry Python auto-instrumentation; Python with a hand-written windowing job over Kafka Streams, Quix Streams, or Flink; trace-ID keying and the single-process aggregation; event-time windows, grace, and emit-once (including the stream-time consequence); delta temporality with Collector conversion; transactional exactly-once and its limits, including state recovery by low-watermark commit; the interim direct application-metrics path; Python 3.13 and version pinning.

## 14. Verification tasks

Each must be checked against the pinned versions in the phase that first depends on it.

- VER-1 (weeks 3 to 4): the name and behavior of the Kafka exporter's trace-ID partitioning option.
- VER-2 (weeks 3 and 9): that the chosen contrib image includes the Kafka receiver and exporter, `deltatocumulative`, and Prometheus remote write, and that `deltatocumulative` handles histograms.
- VER-3 (week 9): how the remote-write exporter treats delta metrics, which is the reason the conversion processor exists.
- VER-4 (weeks 5 to 6): that the low-watermark commit and last-emitted-window record (FR-J9) give exact counts after a kill and restart, using `confluent-kafka` transactions.
- VER-5 (week 8): that late-span drops are counted correctly per window (FR-J8).
- VER-6 (week 10): final series and label names after remote-write conversion.
- VER-7 (week 1): Jaeger v2 accepts OTLP directly and the pinned image tag works in Compose.
- VER-8 (week 1): that `opentelemetry-instrument` with FastAPI, httpx, and SQLAlchemy exports traces and metrics with the pinned versions, and that spans arrive.
- VER-9 (week 3): the VictoriaMetrics remote-write endpoint path and port.
