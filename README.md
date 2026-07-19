# Conduit

*A self-hosted, backend workflow orchestration engine.*

Conduit runs multi-step, asynchronous workflows defined as JSON state machines. A workflow is a **definition** (a state machine), a run is an **execution**, and an **event-sourced engine** drives transitions durably across a fleet of horizontally-scaled workers over Postgres + Redis.

You POST a state-machine definition, start executions against it with some input, and the engine dispatches each Task to a worker, drives Choice branching, waits, retries with backoff, times tasks out, fans out Parallel/Map into child executions and fans their results back in, dead-letters poison tasks, and recovers from worker crashes — all as an append-only event log you can replay and audit.

---

## Capabilities

- **State types:** Task, Pass, Choice, Wait, Succeed, Fail, Parallel, Map.
- **Event-sourced:** every transition is an immutable event; execution state is `replay(events)`. Full ordered history per run.
- **Task execution over Redis Streams:** one stream per resource, competing consumer groups, user-written handlers.
- **Choice:** `StringEquals`, `Numeric*`, `BooleanEquals`, `IsPresent`, `And`/`Or`/`Not`, dot-path `Variable`, `Default`.
- **Wait:** static `Seconds` or `SecondsPath`; a DB-backed scheduler resumes it.
- **Retry with exponential backoff**, per-Task `TimeoutSeconds`, and `Catch` error routing.
- **DLQ:** a task that exhausts retries with no matching Catch is dead-lettered; the execution fails.
- **Parallel & Map:** fan out to child executions (first-class rows), fan in an ordered aggregate output.
- **Effectively-once:** per-attempt idempotency keys + attempt-scoped guards dedup redelivered results/timers. Exactly-once *effects* still require idempotent handlers.
- **Crash recovery:** a reaper reclaims (`XCLAIM`) task entries a dead worker never acked; engine restarts resume from the replayed log.
- **Control-plane REST API** for definitions, executions, history, task inspection, stop, and DLQ inspection.

---

## Run it

**Prereqs:** Docker (Compose v2) and a JDK 21.

```bash
# 1. Start infra (Postgres + Redis).
docker compose up -d

# 2. Run the engine + sample worker on the host.
./gradlew bootRun
#    -> http://localhost:8080   (health at /actuator/health)

# Tear down (add -v to also drop the volumes).
docker compose down
```

The engine reads Postgres/Redis from standard Spring env vars, defaulted in `application.properties` to the compose ports. Liquibase applies the schema on boot; `ddl-auto=validate` checks the mappings.

> **One Postgres on 5432.** If you also have a native Postgres bound to `127.0.0.1:5432`, `localhost` resolves to it (loopback bind beats Docker's wildcard) and the app talks to *that*, not the container. Run a single Postgres, or override `SPRING_DATASOURCE_URL`.

**Full stack in containers** (engine + worker + infra, no host JDK):

```bash
docker compose --profile app up --build
```

Config knobs (env var → property):

| Env var | Default | Purpose |
|---|---|---|
| `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5432/conduit` | Postgres |
| `SPRING_DATA_REDIS_HOST` / `_PORT` | `localhost` / `6379` | Redis |
| `CONDUIT_STREAMS_AUTOSTART` | `true` | Background worker + result-consumer + scheduler + reaper loops |

---

## Quick example

```bash
BASE=http://localhost:8080

# Create a definition -> {id, version}
DEF=$(curl -s -X POST $BASE/workflow-definitions -H 'Content-Type: application/json' -d '{
  "name": "greeter",
  "definition": {
    "StartAt": "Say",
    "States": {
      "Say":  { "Type": "Pass", "Result": { "msg": "hi" }, "Next": "Done" },
      "Done": { "Type": "Succeed" }
    }
  }
}')
ID=$(echo "$DEF" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')

# Start an execution -> {executionId}
EXEC=$(curl -s -X POST $BASE/workflow-definitions/$ID/executions \
  -H 'Content-Type: application/json' -d '{ "input": { "n": 1 } }')
RUN=$(echo "$EXEC" | sed -n 's/.*"executionId":"\([^"]*\)".*/\1/p')

# Inspect
curl -s $BASE/executions/$RUN            # status SUCCEEDED, output {"msg":"hi"}
curl -s $BASE/executions/$RUN/history    # ordered event log
```

A workflow whose first state is a **Task** parks until a worker reports the result. With the sample worker running (`ocr-handler`, `echo-handler`) the background loops carry it end-to-end; otherwise start a worker (see [Writing a worker](#writing-a-worker)).

---

## API reference

All bodies are JSON. Errors: `400 {"errors":[...]}` for a malformed/invalid definition, `404 {"error":"..."}` for a missing resource.

| Method & path | Purpose |
|---|---|
| `POST /workflow-definitions` | Create a definition (validated); new version if the name exists |
| `GET /workflow-definitions` | List definitions |
| `GET /workflow-definitions/{id}` | Describe one definition |
| `POST /workflow-definitions/{id}/executions` | Start an execution |
| `GET /executions?workflowDefinitionId=&status=` | List executions (optional filters) |
| `GET /executions/{id}` | Describe an execution (projection) |
| `GET /executions/{id}/history` | Ordered event history (audit trail) |
| `GET /executions/{id}/tasks` | Task + timer rows behind the run (debug) |
| `POST /executions/{id}/stop` | Abort a running execution; cascades to children |
| `GET /dlq` | Inspect the dead-letter stream |
| `GET /actuator/health` | Health |

### Definitions

**Create** — validates the DSL graph (StartAt exists, every `Next` resolves, a terminal is reachable, no instant-state cycle, per-state required fields). A new definition for an existing `name` gets the next `version`.

```bash
curl -X POST $BASE/workflow-definitions -H 'Content-Type: application/json' -d '{
  "name": "ingest",
  "definition": { "StartAt": "...", "States": { ... } }
}'
# 201 -> {"id":"01K...","version":1}
```

**Describe** → `{ id, name, version, definition, createdAt }`. **List** → array of the same.

### Executions

**Start** — `{ "name"?: string, "input": <json> }`. `name` is optional (defaults to the generated id) and is unique per definition, so it doubles as an idempotency key. The engine enters the first state and dispatches immediately.

```bash
curl -X POST $BASE/workflow-definitions/$ID/executions -H 'Content-Type: application/json' \
  -d '{ "input": { "doc": "invoice.pdf" } }'
# 201 -> {"executionId":"01K..."}
```

**Describe** → the projection (a cache of the replayed state):
```json
{ "id": "...", "workflowDefinitionId": "...", "name": "...",
  "status": "RUNNING", "currentState": "Ocr",
  "input": {...}, "output": null, "error": null,
  "startedAt": "...", "stoppedAt": null }
```
`status` ∈ `RUNNING | SUCCEEDED | FAILED | TIMED_OUT | ABORTED`.

**History** → the authoritative log, ordered by `seq`:
```json
[ { "seq": 0, "type": "EXECUTION_STARTED", "stateName": null, "payload": {"input": {...}}, "createdAt": "..." },
  { "seq": 1, "type": "STATE_ENTERED", "stateName": "Ocr", "payload": {}, "createdAt": "..." }, ... ]
```

**List** — filter by `workflowDefinitionId`, `status`, both, or neither:
```bash
curl "$BASE/executions?workflowDefinitionId=$ID&status=RUNNING"
```

**Tasks** — the operational `tasks`/timer rows for a run:
```bash
curl $BASE/executions/$RUN/tasks
# [ { "stateName":"Ocr","type":"TASK","status":"QUEUED","attempt":1,"resource":"ocr-handler", ... },
#   { "stateName":"W","type":"TIMER","timerKind":"WAIT","nextRunAt":"...", ... } ]
```

**Stop** — appends `ExecutionAborted`, sets status `ABORTED`, and cascades to running children. Already-terminal runs are untouched.
```bash
curl -X POST $BASE/executions/$RUN/stop     # -> 200, ExecutionView (ABORTED)
```

### DLQ

```bash
curl $BASE/dlq
# [ { "id":"<stream-id>","executionId":"...","stateName":"A","attempt":"2","error":"...","cause":"..." } ]
```
Inspection only; redrive is not yet implemented.

---

## DSL reference

Top-level `StartAt` + a `States` map. Each state has a `Type` and either `Next` or `End: true` (terminals need neither). No full JSONPath — dot paths (`$.a.b`) only for Choice `Variable`, Map `ItemsPath`, Wait `SecondsPath`. Whole output → next input (no path filters).

| State | Key fields | Behavior |
|---|---|---|
| **Task** | `Resource`, `Parameters?`, `TimeoutSeconds?`, `Retry?`, `Catch?`, `Next/End` | Dispatch to the resource stream and park. Success → output becomes the data, go `Next`. Failure → Retry, else Catch, else fail (→ DLQ). |
| **Pass** | `Result?`, `Next/End` | Inject static `Result` or pass input through. Instant. |
| **Choice** | `Choices[]`, `Default?` | First matching rule → its `Next`; no match → `Default`, else fail `States.NoChoiceMatched`. Instant. |
| **Wait** | `Seconds` \| `SecondsPath`, `Next/End` | Park until the delay elapses; the scheduler resumes it. |
| **Succeed** | — | Terminal success; output = current data. |
| **Fail** | `Error`, `Cause` | Terminal failure. |
| **Parallel** | `Branches[]`, `Retry?/Catch?`, `Next/End` | One child execution per branch (same input). All succeed → output = `[branch outputs]`; any fail → Catch, else fail. |
| **Map** | `ItemsPath`, `Iterator`, `MaxConcurrency?`, `Retry?/Catch?`, `Next/End` | One child per array item (item = child input). Output = `[iteration outputs]` in order. |

**Retry** — `[{ "ErrorEquals": ["..."|"States.ALL"], "IntervalSeconds": n, "MaxAttempts": n, "BackoffRate": r }]`; backoff = `IntervalSeconds * BackoffRate^(attempt-1)`.
**Catch** — `[{ "ErrorEquals": ["..."], "Next": "Handler" }]`; the error (`{Error, Cause}`) is injected as the handler's data.
**Choice operators** — `StringEquals`, `NumericEquals/GreaterThan/LessThan/GreaterThanEquals/LessThanEquals`, `BooleanEquals`, `IsPresent`, plus `And`/`Or`/`Not` of nested rules.
**Built-in errors** — `States.ALL`, `States.Timeout`, `States.TaskFailed`, `States.NoChoiceMatched`.

Example combining several:

```json
{
  "StartAt": "Ocr",
  "States": {
    "Ocr": {
      "Type": "Task", "Resource": "ocr-handler", "TimeoutSeconds": 30,
      "Retry": [{ "ErrorEquals": ["TransientError"], "IntervalSeconds": 5, "MaxAttempts": 3, "BackoffRate": 2.0 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "HandleFailure" }],
      "Next": "CheckType"
    },
    "CheckType": {
      "Type": "Choice",
      "Choices": [{ "Variable": "$.docType", "StringEquals": "invoice", "Next": "ProcessInvoice" }],
      "Default": "ProcessGeneric"
    },
    "ProcessInvoice": { "Type": "Pass", "Next": "Done" },
    "ProcessGeneric": { "Type": "Pass", "Next": "Done" },
    "HandleFailure":  { "Type": "Fail", "Error": "IngestFailed", "Cause": "ocr exhausted retries" },
    "Done": { "Type": "Succeed" }
  }
}
```

---

## Writing a worker

A worker registers a handler per resource; the runtime long-polls `task:<resource>`, runs the handler, and reports on the shared `results` stream. Handlers must be idempotent (a retry is a new attempt).

```java
@Component
class MyWorker {
    MyWorker(WorkerRuntime worker, ObjectMapper mapper) {
        worker.register("ocr-handler", input -> {
            String doc = input.get("doc").asText();
            return mapper.createObjectNode().put("text", ocr(doc));   // becomes the next state's input
        });
    }
}
```

- The `Resource` in a Task is, by convention, the Redis stream name (`task:<Resource>`); there is no registry. An unmanned resource → the task times out (if `TimeoutSeconds` set).
- Background loops (worker poll, result consumer, scheduler, reaper) run automatically unless `conduit.streams.autostart=false`.
- HTTP/system calls are just handler code using `RestClient` — no new engine concept. (A built-in `http:invoke` dispatcher is a v2 extension point behind `TaskDispatcher`.)

---

## How it works

Three planes: a **control plane** (REST), a single-instance **engine plane** (replay → decide → append → dispatch), and a **data plane** (Redis Streams) carrying tasks out to workers and results back.

```
REST ─▶ Control plane ─▶ append events (1 tx) ─▶ Postgres (source of truth)
                                                    │  replay
                                                    ▼
                                              Engine (single instance)
                                              replay → decide() [pure] → append → dispatch
                                                    │                         ▲
                                          XADD task:<resource>          XADD results
                                                    ▼                         │
                                              Redis Streams ◀──poll/ack──▶ Worker fleet (N)
                        Scheduler (SKIP LOCKED claim of due timers) · Reaper (XCLAIM stuck entries)
```

- **Event-sourced truth.** `execution_events` is append-only with a dense per-execution `seq`; `ExecutionState = replay(events)`. The `executions` projection columns (`status`, `current_state`, `output`) are a cache written in the same transaction as the append.
- **Pure decision core.** `Engine.decide(graph, state, trigger) → (events, commands)` is side-effect-free and unit-tested with zero infra. All IO lives in the dispatcher at the edges.
- **One writer per execution.** Every trigger runs the same critical section under a `SELECT … FOR UPDATE` row lock on the execution: replay → decide → append (`seq++`, `UNIQUE(execution_id, seq)` backstop) → project, in one transaction. Commands dispatch **after** commit, so a rolled-back decision emits no side effects. Concurrency across *different* executions is unbounded.
- **Effectively-once.** Task/timer rows carry per-attempt idempotency keys (`executionId:stateName:attempt`); results/timers for a superseded attempt or a state the machine moved past are dropped. Workers dedup by the same key. Engine-side state is exactly-once regardless; exactly-once *effects* need idempotent handlers.
- **Scheduler.** A poll loop claims due `TIMER` rows with `SELECT … FOR UPDATE SKIP LOCKED` + a `SCHEDULED→QUEUED` status flip (exactly-one claim under overlapping cycles), then fires the timer's trigger (Wait resume / retry re-dispatch / timeout).
- **Crash recovery.** A worker that dies after reading a task but before acking leaves a pending stream entry; the reaper `XCLAIM`s idle entries and reprocesses them. A restarted engine resumes purely from the replayed log.

### Data model

Postgres is the source of truth.

- **`workflow_definitions`** — `id · name · version · definition (jsonb)`, `UNIQUE(name, version)`.
- **`executions`** — a run: `status`, `current_state`, `input/output/error (jsonb)`, `parent_execution_id / parent_branch_index / branch_state / root_execution_id` (child executions), `UNIQUE(workflow_definition_id, name)`.
- **`execution_events`** — append-only log: `execution_id · seq · type · state_name · payload (jsonb)`, `UNIQUE(execution_id, seq)`. Authoritative.
- **`tasks`** — operational (not truth): dispatch rows (`type=TASK`, resource/input/attempt) and timer rows (`type=TIMER`, `timer_kind`, `next_run_at`), keyed by a unique `idempotency_key`.

---

## Tech stack

Java 21 · Spring Boot 3.5 · Spring Web · Spring Data JPA · Liquibase · Postgres · Redis Streams (Spring Data Redis / Lettuce) · Lombok · ULID · JUnit 5 + Testcontainers (Postgres + Redis).

```bash
./gradlew test        # unit (pure engine, DSL) + integration (Testcontainers) — needs Docker
./gradlew build       # compile + test + jar
```

---

## Out of scope (v2)

Built-in/system resources (`http:invoke`); resource registry + input JSON-Schema validation; path-based data flow (InputPath/ResultPath/OutputPath, full JSONPath); DLQ redrive; Map `MaxConcurrency` throttling (parsed, currently unthrottled); nested Parallel/Map beyond one level; Retry on Parallel/Map (Catch/fail only); multi-instance engine coordination; auth/RBAC; metrics/observability.
