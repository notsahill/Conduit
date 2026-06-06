# Conduit

*A self-hosted, backend workflow orchestration engine.*

Conduit executes multi-step, asynchronous workflows defined as state machines. Workflows are **state machines** (JSON definitions), runs are **executions**, and an **event-sourced engine** drives state transitions durably across a fleet of horizontally-scaled workers.

This is a learning/portfolio project. The goal is a clean, correct implementation of the core distributed-systems patterns — event sourcing, effectively-once execution, retries with backoff, fan-out/fan-in, crash recovery — over Postgres + Redis.

---

## Design decisions

| Area | Decision |
|---|---|
| Definition language | Simplified custom JSON DSL, Step Functions–shaped (no full JSONPath; dot paths only where unavoidable) |
| State types | Task, Choice, Pass, Wait, Succeed, Fail, Parallel, Map |
| Engine model | Event-sourced + replay (current state derived from an immutable event log) |
| Worker model | Redis Streams message queue (one stream per resource, consumer groups) |
| Scheduler | DB scan of a `next_run_at` column via an in-process poll loop (no Quartz/cron, no broker-native delay) |
| Engine scaling | Single engine instance; workers scale horizontally |
| Parallel / Map fan-out | Child executions (recursive, first-class rows) |
| Control-plane API | Step Functions–style REST |
| Data flow | Whole output → next input (no path filters in v1) |
| Resource model | Convention-only: `Resource` (plain name) = Redis stream name; no registry. Unmanned resource → task times out. |
| Capabilities | DSL-only: all knobs (`TimeoutSeconds`, `Retry`, reserved `Parameters`) live per-Task in the definition |
| Task execution | A user-written worker handler does the work (including HTTP calls). Built-in resources (`http:invoke`) deferred to v2, unblocked by the dispatch abstraction. |

**Guiding principles**
- **Event-sourced truth.** Every transition is an immutable event; state is derived by replay. Recovery is deterministic, audit is free.
- **Pure decision core.** The engine's `decide()` is side-effect-free and unit-testable with zero infra. All IO lives at the edges.
- **Effectively-once.** Idempotency keys + status guards on both worker and engine sides — no duplicate side effects.
- **Open for extension.** Resource dispatch is pluggable so a built-in HTTP/system-resource layer can be added later without rework.

---

## Architecture

Three planes: a **control plane** (REST), a single-instance **engine plane** (replay → decide → append → dispatch), and a **data plane** (Redis Streams) carrying tasks to workers and results back.

```
                    ┌─────────────────────────────────────────────┐
   REST client ───▶ │ Control Plane (Spring MVC controllers)        │
                    │  CreateWorkflowDefinition / StartExecution... │
                    └───────────────┬──────────────────────────────┘
                                    │ append events (1 tx)
                                    ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ Postgres (source of truth)                                     │
   │  workflow_definitions · executions · execution_events · tasks   │
   └───────▲───────────────────────────┬──────────────────────────┘
           │ replay                     │ claim due / runnable
           │                            ▼
   ┌───────┴────────────────┐   ┌──────────────────────────┐
   │ Engine (single inst.)  │   │ Scheduler (poll loop)     │
   │  - replay events→state │   │  - scan next_run_at<=now  │
   │  - decide() [PURE]     │   │  - enqueue due tasks/timers│
   │  - append events       │   └──────────────────────────┘
   │  - dispatch commands   │
   └───────┬────────────────┘
           │ TaskDispatcher.dispatch()         result XADD
           ▼                                          │
   ┌──────────────────┐   poll/ack   ┌────────────────┴────────┐
   │ Redis Streams    │◀────────────▶│ Worker fleet (N, scale)  │
   │  task:<resource> │              │  - dedup idempotency_key │
   │  results · dlq   │              │  - run handler, report   │
   └──────────────────┘              └─────────────────────────┘
```

### Components (each a bounded, single-purpose module)

1. **API layer** — REST controllers + DTOs.
2. **DSL parser/validator** — definition JSON → in-memory `WorkflowGraph`; validates the graph at create time.
3. **Event store** — append-only `execution_events`; `replay(events) → ExecutionState`.
4. **Engine core** — pure `decide(state, triggerEvent) → (newEvents[], commands[])`; no IO.
5. **Dispatcher** — executes commands: route Task via `TaskDispatcher`, write `tasks` rows, schedule timers, spawn children, complete executions.
6. **Result consumer** — reads the `results` stream, appends `TaskSucceeded`/`TaskFailed`, triggers the engine.
7. **Scheduler/poller** — `@Scheduled` loop scanning due timers + retryable tasks.
8. **Reaper** — `@Scheduled` `XAUTOCLAIM` of stuck stream entries (worker crash recovery).
9. **Worker SDK + sample worker** — long-poll, idempotent execute, report.
10. **DLQ handler** — poison tasks → `dlq` stream; execution failed/inspectable.

### Resource-dispatch abstraction (v2 extension point)

The engine never touches Redis directly. On Task entry it builds `TaskContext {resource, input, parameters?}` and hands it to a router:

```
interface TaskDispatcher { void dispatch(TaskContext ctx); }

RoutingTaskDispatcher                      // picks by resource scheme
 ├─ default (plain name)  -> RedisStreamDispatcher    (v1: XADD task:<resource>)
 └─ scheme:* (e.g. http:invoke, built-in:*) -> BuiltinTaskDispatcher  (v2)
```

`Parameters` is reserved on the Task model, DSL, and `tasks` row from day one (parsed, stored, passed through, unused in v1), so the built-in layer needs no migration later.

---

## Data model

Postgres is the source of truth. `execution_events` is authoritative; `executions` projection columns and `tasks` are derived/operational. Repurposes the existing tables under the new vocabulary.

**`workflow_definitions`** — immutable definition per version
```
id VARCHAR(26) PK · name · version INT · definition JSONB · created_at
UNIQUE(name, version)        -- edit = new version row; executions pin their version
```

**`executions`** — a run
```
id VARCHAR(26) PK · workflow_definition_id FK · name
status   -- RUNNING|SUCCEEDED|FAILED|TIMED_OUT|ABORTED   (projection)
current_state (projection) · input JSONB · output JSONB · error JSONB
parent_execution_id FK NULL · parent_branch_index INT NULL · root_execution_id FK
started_at · stopped_at
UNIQUE(workflow_definition_id, name)   -- StartExecution idempotency
```
`status`/`current_state`/`output` are written in the SAME transaction as the event append.

**`execution_events`** — append-only source of truth
```
id BIGSERIAL PK · execution_id FK · seq INT · type · state_name NULL · payload JSONB · created_at
UNIQUE(execution_id, seq) · INDEX(execution_id, seq)
```

**`tasks`** — dispatch + timer + dedup bookkeeping (operational, not truth)
```
id VARCHAR(26) PK · execution_id FK · state_name
type    -- TASK | TIMER  (TIMER = Wait OR retry backoff delay)
status  -- SCHEDULED|QUEUED|RUNNING|COMPLETED|FAILED|DLQ
idempotency_key UNIQUE   -- executionId:stateName:attempt
attempt · max_attempts · next_run_at · redis_entry_id
resource NULL · parameters JSONB NULL (reserved) · input JSONB · created_at · updated_at
INDEX(status, next_run_at) · INDEX(execution_id)
```

**Event catalog** (replay vocabulary)
```
ExecutionStarted   StateEntered          StateExited
TaskScheduled      TaskStarted           TaskSucceeded
TaskFailed         TaskTimedOut          RetryScheduled
WaitStarted        WaitCompleted         ChoiceEvaluated
ParallelStarted    ParallelBranchFailed  ParallelSucceeded
MapStarted         MapIterationFailed    MapSucceeded
ExecutionSucceeded ExecutionFailed       ExecutionAborted   ExecutionTimedOut
```
No compensation/saga events. Error handling is Retry + Catch only.

**Derived `ExecutionState`** (in-memory replay output, not a table): `status`, `currentStateName`, `currentData` (the JSON flowing output→input), `retryAttempts` (per state), `pendingChildren` (`{childId → done?}` for fan-in), `waitingTimerId`.

---

## DSL

Declarative JSON: top-level `StartAt` + `States` map; each state has `Type` + `Next`/`End:true`. No full JSONPath — dot paths (`$.foo.bar`) only for Choice `Variable`, Map `ItemsPath`, Wait `SecondsPath`.

```json
{
  "StartAt": "Ocr",
  "States": {
    "Ocr": {
      "Type": "Task",
      "Resource": "ocr-handler",
      "TimeoutSeconds": 30,
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
    "HandleFailure": { "Type": "Fail", "Error": "IngestFailed", "Cause": "ocr exhausted retries" },
    "Done": { "Type": "Succeed" }
  }
}
```

| State | Key fields | Behavior |
|---|---|---|
| **Task** | `Resource`, `Parameters?`, `TimeoutSeconds?`, `Retry?`, `Catch?`, `Next/End` | Dispatch via `TaskDispatcher`; park. Success → output becomes data, go `Next`. Failure → Retry, else Catch, else fail. |
| **Choice** | `Choices[]` (`Variable` dot-path, operator, `Next`), `Default` | First matching rule → its `Next`; no match → `Default` (or fail `States.NoChoiceMatched`). Instant. |
| **Pass** | `Result?`, `Next/End` | Inject static `Result` or pass input through. Instant. |
| **Wait** | `Seconds` \| `SecondsPath`, `Next/End` | Write TIMER row `next_run_at = now + secs`; park; scheduler resumes. |
| **Succeed** | — | Terminal success; output = current data. |
| **Fail** | `Error`, `Cause` | Terminal failure. |
| **Parallel** | `Branches[]`, `Retry?/Catch?`, `Next/End` | One child execution per branch (same input). All succeed → output = `[branch outputs]`. Any fail → Retry/Catch, else fail. |
| **Map** | `ItemsPath`, `Iterator`, `MaxConcurrency?`, `Retry?/Catch?`, `Next/End` | One child execution per array item (item = child input). Output = `[iteration outputs]` in order. |

Choice operators (v1): `StringEquals`, `NumericEquals/GreaterThan/LessThan/GreaterThanEquals/LessThanEquals`, `BooleanEquals`, `IsPresent`, plus `And`/`Or`/`Not`.

Parallel and Map share one fan-out/fan-in/recovery path; they differ only in child count and child input source.

---

## Execution loop & error handling

```
trigger (execution started | result arrived | timer due | child done)
  └─ load execution_events ──▶ replay() ──▶ ExecutionState
       └─ decide(state, triggerEvent)        # PURE, no IO
            ├─ newEvents[]   (StateEntered, TaskScheduled, ...)
            └─ commands[]    (EnqueueTask, ScheduleTimer, SpawnChildren, CompleteExecution)
       └─ ONE transaction: append newEvents (seq++) + update executions projection
       └─ dispatcher runs commands
```
Instant states (Choice/Pass) chain within one trigger until reaching a parking state (Task/Wait/Parallel/Map) or a terminal.

- **Retry** — on `TaskFailed`, match `ErrorEquals`; if attempt < `MaxAttempts`, schedule TIMER `delay = IntervalSeconds * BackoffRate^attempt`; emit `RetryScheduled`.
- **Catch** — retries exhausted / no Retry match → first matching `Catch` → jump to its `Next` (error injected into data).
- Neither → execution FAILED.
- Error names: worker strings + built-ins `States.ALL`, `States.Timeout`, `States.TaskFailed`.
- **DLQ** — task exhausting retries with no Catch → `dlq` stream; execution fails (inspectable; optional redrive).

---

## Worker protocol (Redis Streams)

One stream per resource; workers join a consumer group (competing consumers, independent scaling).

```
stream:  task:<Resource>      group: <Resource>-workers
results: shared "results" stream, group "engine"
dlq:     shared "dlq" stream
```

Worker loop:
```
1. XREADGROUP <Resource>-workers consumer=<id> COUNT 1 BLOCK 5s
2. dedup: idempotencyKey already COMPLETED -> XACK, skip     (effectively-once)
3. run registered handler(input)
4. XADD results { taskId, executionId, status, output|error, cause }
5. XACK task:<Resource> <entryId>
```

- **Timeout** — dispatch with `TimeoutSeconds` also writes a TIMER row; fires before result → `TaskTimedOut`.
- **Crash recovery** — worker dies after read, before ack → entry stuck pending; reaper `XAUTOCLAIM` (idle > N s) reassigns. Late/duplicate result for a terminal task → ignored.
- **Idempotency key** = `executionId:stateName:attempt`; checked by worker (before side effect) and engine (before applying result).
- HTTP calls = a handler that uses `RestClient` internally — no new engine concept.

---

## Control-plane API (REST)

```
POST   /workflow-definitions                  CreateWorkflowDefinition  {name,definition} -> {id,version}
GET    /workflow-definitions                  ListWorkflowDefinitions
GET    /workflow-definitions/{id}             DescribeWorkflowDefinition

POST   /workflow-definitions/{id}/executions  StartExecution  {name?,input} -> {executionId}
GET    /executions/{id}                 DescribeExecution
GET    /executions/{id}/history         GetExecutionHistory   (ordered audit trail)
GET    /executions?stateMachineId=&status=   ListExecutions
POST   /executions/{id}/stop            StopExecution  (cascades to children)

GET    /executions/{id}/tasks           debug: task rows + attempts
GET    /dlq                             inspect dead-letter entries
POST   /dlq/{taskId}/redrive            re-enqueue a poison task
GET    /actuator/health|metrics|prometheus
```
Definitions are validated at create (reachability, unknown `Next`, terminal exists, duplicate state names, per-state schema); a new definition for an existing name creates a new version. Resource existence is NOT checked (convention-only).

Metrics (Micrometer → Prometheus): queue depth per stream, executions by status, task attempts/failures, retry count, engine loop latency.

---

## Tech stack

Java 21 · Spring Boot 3.5.x · Spring Web · Spring Data JPA · Liquibase · Postgres · Redis Streams (Spring Data Redis / Lettuce) · Lombok · ULID · Micrometer + Prometheus · JUnit 5 + Testcontainers (Postgres + Redis).

---

## Implementation plan

Phased so each milestone is runnable/testable before the next. The pure engine core is built and unit-tested before any infra.

### Phase 0 — Foundation
- Add deps: Spring Data Redis, Testcontainers (Postgres + Redis).
- Fix `application.properties` (` Liquibase` → `# Liquibase`); add Redis config.
- Rename existing entities/tables to the new vocabulary: `workflows → executions`, `workflow_events → execution_events`, `task_messages → tasks` (`workflow_definitions` kept, gains `version`). New Liquibase changesets reflecting the schema above (add `version`, parent/root columns, `tasks.type/next_run_at/redis_entry_id/resource/parameters`).
- Populate the empty `Step` enum or remove it (replaced by DSL state types); align `current_step` removal.
- **Done when:** app boots, migrations apply, `ddl-auto=validate` passes.

### Phase 1 — DSL parser & validator
- `WorkflowGraph` / `State` model classes (one per state type) + Jackson polymorphic deserialization on `Type`.
- Graph validator: `StartAt` exists, every `Next` resolves, a terminal is reachable, no duplicate names, per-state required fields.
- **Done when:** unit tests parse/validate good and bad definitions. No infra.

### Phase 2 — Event store & pure engine core
- `execution_events` append + `replay(events) → ExecutionState`.
- Pure `decide(state, triggerEvent) → (newEvents, commands)` for **linear states first**: Task (dispatch + success/fail), Pass, Succeed, Fail.
- Command + event types.
- **Done when:** engine unit tests drive a linear Task→Pass→Succeed machine entirely in memory (fake events in, events/commands out). No Redis/DB.

### Phase 3 — Control-plane API
- Controllers + DTOs: CreateWorkflowDefinition, DescribeWorkflowDefinition, ListWorkflowDefinitions, StartExecution, DescribeExecution, GetExecutionHistory.
- Persist on start: `ExecutionStarted` event + projection row in one tx.
- **Done when:** can create a machine and start an execution via HTTP; history readable.

### Phase 4 — Redis dispatch + workers (first end-to-end)
- `TaskDispatcher` abstraction + `RoutingTaskDispatcher` + `RedisStreamDispatcher` (XADD `task:<resource>`).
- Result consumer (`XREADGROUP results` → append `TaskSucceeded`/`TaskFailed` → trigger engine).
- Worker SDK (`register(resource, handler)`, poll/ack/report/dedup) + sample worker with stub handlers.
- **Done when:** a linear Task workflow runs end-to-end across a real worker over Redis.

### Phase 5 — Durability: scheduler, retries, timeouts, DLQ, recovery
- `@Scheduled` poller scanning `tasks` for due TIMER + retryable rows.
- Wait state; Retry with exponential backoff; `TimeoutSeconds`; Catch; DLQ on exhaustion.
- Reaper `XAUTOCLAIM` for stuck entries; idempotency dedup on both sides.
- **Done when:** kill-a-worker, duplicate-delivery, and exhaust-retries scenarios behave correctly; engine restart resumes from replay.

### Phase 6 — Choice
- Choice evaluation (operators + `And`/`Or`/`Not`, dot-path `Variable`), `Default`.
- **Done when:** branching workflow routes correctly per input; unit + integration tests.

### Phase 7 — Parallel & Map (fan-out / fan-in)
- Child-execution spawn, parent parking, fan-in aggregation (ordered for Map), `MaxConcurrency`, branch/iteration failure → Retry/Catch.
- **Done when:** Parallel and Map workflows complete with correct aggregated output and failure handling.

### Phase 8 — Operations & observability
- StopExecution (cascade), ListExecutions filters, `/dlq` + redrive, `/executions/{id}/tasks`.
- Micrometer metrics + Prometheus endpoint; sample Grafana dashboard JSON.
- **Done when:** metrics expose execution/queue/retry signals; stop + redrive work.

### Phase 9 — Hardening
- Testcontainers integration tests across all state types; crash-recovery and idempotency tests; a basic load test (target throughput on a small VM).
- README walkthrough a stranger can follow; example workflows.
- **Done when:** the success criteria below all pass in CI.

---

## Success criteria

1. Define a state machine via API, start an execution, run to completion across worker(s).
2. Sequential Task→Task and Choice branching work end-to-end.
3. Parallel and Map fan out to child executions and fan in correctly (ordered output for Map).
4. Failed tasks retry with exponential backoff; exhausted + uncaught → DLQ + execution FAILED.
5. `Catch` redirects to a handler state on a matched error.
6. Wait state delays via the scheduler and resumes.
7. Worker killed mid-task → task reclaimed (`XAUTOCLAIM`) and completes; no duplicate side effect.
8. Duplicate task/result delivery produces no duplicate effect.
9. Engine restarted mid-execution → resumes from replayed events; no lost progress.
10. `GetExecutionHistory` returns a complete, ordered audit trail.
11. Prometheus metrics expose execution/queue/retry signals.

---

## Out of scope (v2)

Built-in/system resources (`http:invoke`); resource registry + capability defaults + input JSON-Schema validation; path-based data flow (InputPath/ResultPath/OutputPath, full JSONPath); worker self-registration + heartbeat; multi-instance engine coordination (optimistic/advisory locking); definition versioning migration tooling; auth/RBAC; encryption-at-rest; multi-region.
