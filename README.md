# Conduit
*A distributed workflow orchestration engine*
 
> Working title — feel free to swap. Other candidates: Forge, Loom, Pulse, Relay.
 
---
 
## Overview
 
Conduit is a self-hosted workflow orchestration engine that reliably executes multi-step, asynchronous workflows across distributed worker services. It handles the hard parts of distributed execution — retries, failure recovery, idempotency, state persistence — so application developers can focus on writing business logic instead of plumbing.
 
Think of it as a lightweight, self-hosted analogue to AWS Step Functions or Temporal. Not as feature-rich, but built from first principles to demonstrate the core engineering patterns that make modern workflow systems work.
 
---
 
## The problem
 
Real-world backend systems often involve operations that span multiple steps, services, and time horizons:
 
- Document ingestion (upload → OCR → validate → classify → persist → notify)
- Payment processing (authorize → capture → ledger entry → reconcile → notify)
- Onboarding flows (verify → enrich → provision → email → audit)
- AI pipelines (extract → embed → analyze → summarize → persist)
Implementing these as synchronous API chains creates fragile systems. A single failed step kills the workflow. Recovery requires manual intervention. Retry logic gets duplicated across services. There's no visibility into what's running, stuck, or failed. Scaling individual steps independently becomes impossible.
 
Conduit solves this by treating workflows as first-class entities with persistent state, durable execution, and observable progress.
 
---
 
## What it does
 
### Core capabilities
 
**Workflow definition.** Developers define workflows as a sequence of named steps with inputs, outputs, and dependencies. Definitions are stored as durable artifacts and reusable across executions.
 
**Reliable asynchronous execution.** Each step runs as an independent task on a worker. Steps are dispatched via a message queue, ensuring decoupled execution and natural backpressure under load.
 
**Automatic retry with exponential backoff.** Failed tasks are retried with increasing delays (5s → 15s → 45s → ...) until they succeed or exhaust their retry budget.
 
**Dead-letter queue for poison messages.** Tasks that fail repeatedly are routed to a DLQ for inspection rather than retried infinitely.
 
**Idempotent execution.** Workers detect and safely handle duplicate task deliveries, so side effects like payments, emails, and DB writes never repeat.
 
**Crash recovery.** Workflow state is durably persisted. When a worker or the orchestrator crashes, in-flight workflows resume from their last known state — no manual recovery required.
 
**Observable execution.** Every workflow exposes its current state, step history, retry count, and timing. A metrics dashboard surfaces queue depth, throughput, error rates, and worker health in real time.
 
**Horizontal worker scaling.** Workers are stateless and can be added or removed independently to handle load.
 
### What it deliberately does not do
 
- No graphical workflow editor or visual DSL — workflows are defined in code or JSON
- No multi-region replication or cross-datacenter coordination
- No enterprise features like RBAC, audit logging, or encryption-at-rest
- Not a production-grade replacement for Temporal, Airflow, or Step Functions
These omissions are intentional. The goal is to deeply implement the core patterns, not reproduce a commercial product.
 
---
 
## How it works
 
### High-level mechanism
 
A workflow execution flows through three logical layers:
 
**The orchestrator** receives a request to start a workflow, persists initial state to the database, and dispatches the first task to the queue. As steps complete, it advances workflow state and dispatches dependent steps.
 
**The queue** decouples the orchestrator from workers. Tasks sit in the queue until a worker picks them up. This buffers traffic spikes and lets workers scale independently.
 
**Workers** consume tasks, execute the corresponding business logic, and emit completion or failure events back to the system. They are stateless — all coordination happens through the queue and database.
 
### Key engineering patterns
 
**Durable state through event sourcing.** Every workflow state transition is written as an immutable event to the database. Current state is derived from replaying these events. Recovery becomes deterministic, and auditing becomes trivial.
 
**Effectively-once execution via idempotency keys.** Distributed systems cannot guarantee exactly-once delivery. Conduit instead guarantees *effectively-once* execution: every task carries a unique idempotency key, and workers deduplicate based on this key before executing side effects.
 
**Saga-style compensation for partial failures.** When a multi-step workflow fails midway, Conduit executes compensating actions in reverse order to undo prior side effects. If step 4 fails after steps 1–3 succeeded, each of steps 1–3 has a compensation handler that runs to roll back its work.
 
**Backpressure through queue depth monitoring.** When queue depth grows beyond healthy thresholds, the orchestrator slows down accepting new workflows and emits signals upstream to throttle.
 
### The reference workflow
 
To make the system concrete, Conduit ships with one demo workflow: **document processing**.
 
1. An upload triggers a new workflow execution
2. An OCR step extracts text from the document
3. A validation step verifies the extracted data structure
4. A classification step tags the document type
5. A persistence step writes the structured data to the database
6. A notification step sends a webhook or email to the requester
Each step runs on a separate worker. Failures trigger automatic retries; persistent failures route to the DLQ. Crashes during execution resume cleanly after restart. The demo workflow is the proof that all the engineering primitives work end-to-end.
 
---
 
## Success criteria
 
The project is "done" when all of the following are true:
 
1. A workflow can be defined, started via API, and executed to completion across distributed workers
2. A worker can be killed mid-execution and the workflow resumes correctly after restart
3. Failed tasks retry with exponential backoff and route to DLQ after exhausting retries
4. Duplicate task deliveries do not produce duplicate side effects
5. The system runs in a deployed environment (not just local Docker) with a public URL
6. A live Grafana dashboard shows real-time workflow metrics
7. Load tests demonstrate the system handles ≥100 workflows/minute on a 4-core VM
8. The README contains a clear architecture diagram and a walkthrough a stranger can follow
---
 
## Out of scope (for v1)
 
- Workflow versioning and migration
- Conditional branching and parallel step execution (v1 is sequential only)
- Multi-tenancy with execution isolation
- Authentication and authorization
- Encrypted state storage
- Multi-region deployment
These become candidate v2 goals if the project lands well and there's appetite to keep building.
 
---
 
## Why this matters
 
Workflow orchestration sits at the intersection of distributed systems, reliability engineering, and async architecture — three areas that distinguish senior engineers from mid-level ones. Building one from scratch demonstrates the author understands not just how to *use* Temporal or Step Functions, but how they work internally and why they make the tradeoffs they do.
 
The patterns implemented here — event sourcing, idempotency, sagas, backpressure, exponential backoff — are the same patterns that show up in payment systems, distributed databases, and large-scale data pipelines. Mastering them in a small, self-built system makes them legible at any scale.
