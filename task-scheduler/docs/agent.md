# Phase 8 AI Agent Orchestrator

The agent consumes the **durable Postgres DLQ inbox** created by the manual-commit
Kafka DLQ consumer. It does not start a competing Kafka group or depend on observing
a message before its incident has been committed. New received OPEN incidents are
clustered, diagnosed, and acted on with persistent recovery state.

## Run and verify

From `task-scheduler`, retaining the existing `.env`:

```sh
source .venv/bin/activate
python -m pip install -e '.[dev]'
make up
make migration-check
make check-agent
make test
make test-integration
```

`make up` applies revision **0005**, builds the agent image, and waits for its health
check. The default provider is **MockLLM**, which makes no external API calls. The
agent health and metrics endpoints are published locally on port 8006:

```sh
curl -fsS http://localhost:8006/health
curl -fsS http://localhost:8006/metrics
```

The live checker requires MockLLM, AGENT_ENABLED=true, and the kill switch off. It
submits two simulated reports with identical invalid delays. It verifies one
representative diagnosis, shared cluster identity, scheduled recovery, corrected
payload flags, successful execution, both generations of attempts, and token/cost
audit. A further uncorrectable task verifies escalation and the notification stub.
The cohort uses a unique normalized error per run to avoid earlier demo circuits.
Default recovery backoff is 30 seconds, so the check normally takes about 35 seconds.
It uses only newly submitted task IDs and does not change agent policy settings.

For three workers and two schedulers:

```sh
docker compose up -d --wait --wait-timeout 180 --scale worker=3 --scale scheduler=2 api worker scheduler dlq agent
python scripts/check_worker.py --expected-workers 3
python scripts/check_agent.py
```

To exercise two agent processes without a host-port collision:

```sh
docker compose run --rm --no-deps agent
```

Run that command in another terminal while the Compose agent is active. The service
uses the shared Postgres claims and Redis budget; the one-off container publishes
no host ports. Its internal listener does not conflict with the main agent container.

## LLM boundary and tools

`services/agent/llm/base.py` defines `LLMClient.complete(messages, tools, schema)`.
A completion contains Anthropic-shaped text/tool-use blocks, provider request ID,
input/output tokens, attempt count, and attempts whose usage is unknown.

`AnthropicClient` uses async HTTPX with an owned transport and configured timeout.
It retries network failures and 408/409/429/5xx responses a bounded number of times,
honors bounded Retry-After seconds/dates, and otherwise applies exponential full
jitter. It uses the Messages API and a `submit_diagnosis` tool carrying the JSON
schema, so tool calling and final structured output use one provider interface.
Known non-transient HTTP failures fail promptly. All agent egress still passes the
provider-independent guard; a caller never executes a model-selected action.

The adapter's transport, retry, timeout, tool schema, repair protocol, and usage
handling are tested with mocked HTTP. No paid Claude request was made for this
phase. To enable real requests, edit `.env` with a valid API key, select an active
model and matching budget rates, then recreate the agent. For example:

```dotenv
LLM_PROVIDER=anthropic
LLM_MODEL=claude-sonnet-5-5
LLM_API_KEY=<your-key>
LLM_INPUT_USD_PER_MILLION=2
LLM_OUTPUT_USD_PER_MILLION=10
```

```sh
docker compose up -d --build --wait agent
```

The pre-existing `.env` may still select Sonnet 4.5; choose the desired model
explicitly when enabling real calls. Rates are configurable, not fetched at runtime.
Check the provider's current [model lifecycle](https://platform.claude.com/docs/en/about-claude/model-deprecations)
and [pricing](https://platform.claude.com/docs/en/about-claude/pricing) before changing
models. The default settings retain conservative 3/15 USD per-million input/output
rates compatible with the existing configuration; set the rates for your model.
The implemented wire contract follows [Messages/tool use](https://platform.claude.com/docs/en/api/messages/create)
and [API error handling](https://platform.claude.com/docs/en/api/errors).

`MockLLM` accepts deterministic scripted diagnoses, malformed output, and tool
blocks for tests. Its default offline classifier distinguishes the six categories.
For a report's negative or nonnumeric delay it can return a complete replacement
with seconds=0 and retained report data. It does not invent an email recipient or
other redacted business value. Mock token accounting is deterministic nominal
accounting for exercising budgets; no provider billing occurs.

The collector reads:

- The representative's task payload and latest N attempt errors/stack traces.
- Recent failures of the same type/signature in the configured time window.
- The task type's terminal success rate over tasks created in the last 24 hours.
- Worker error/trace records persisted in `task_attempts`.

Tools are `get_task_history`, `get_similar_failures`, `get_task_type_stats`, and
`get_worker_logs`. Arguments are validated and scoped to the representative task,
its task type, and its signature. Unknown tools, arbitrary task IDs, log paths, and
extra arguments are rejected. Worker logs are the persisted execution evidence;
there is no Docker socket, shell access, or arbitrary file reader.

## Clustering and recovery

A cluster key consists of task type, normalized failure signature, and a fixed UTC
window (60 seconds by default). Windows use the failure event timestamp. A short
2-second debounce admits the initial cohort; late members within the same window
reuse the saved diagnosis. Adjacent fixed windows are distinct even at a boundary.
The same signature across different task types never shares a diagnosis.

Postgres stores cluster membership, representative, ready time, claim token,
expiry, diagnosis, guard reason, usage, and any circuit intent. A unique key and
SKIP LOCKED claim prevent concurrent diagnoses. The default lease is 180 seconds,
longer than the bounded 60-second diagnosis pipeline. Expired claims are recoverable
by another agent. A saved DONE diagnosis is reused on restart before applying any
new member actions. Independent agents may process different clusters concurrently.

A crash after a provider has responded but before its result commits can require a
replacement call after lease expiry. Exactly-once external billing cannot be
promised; the abandoned budget reservation remains conservatively charged. Claim
tokens fence stale owners and committed diagnoses/actions remain idempotent.

Revision 0005 sets **historical incidents ineligible** and enrolls new incidents by
default, so deployment does not silently recover an old manual queue. To explicitly
backfill an old incident, an operator can set its `agent_eligible=true` after review.
Missing-task/orphan incidents escalate locally without spending LLM tokens or
becoming representatives for valid tasks.

## Action policy

The model must return all fields in the strict `Diagnosis` schema:
`root_cause`, `category`, `confidence`, `suggested_fix`, `safe_to_auto_retry`,
`modified_payload`, and `recommended_backoff_s`. Categories are transient,
bad_input, dependency_down, bug, resource_exhaustion, and unknown. Numeric bounds,
strict booleans, finite JSON, required fields, and rejection of extra fields are
validated independently of the provider.

Policies are evaluated in this order:

1. Guard failure, kill switch, or confidence below 0.85 escalates.
2. Three distinct failed tasks in a cluster open a task-type circuit and escalate.
3. A task that already used its one automatic recovery escalates.
4. Automatic recovery requires safe_to_auto_retry=true.
5. High-confidence transient/dependency failures requeue with unchanged payload and
   a delay at least the configured minimum, the recommendation, and twice the last
   recorded retry delay. A delay above the configured cap escalates.
6. High-confidence bad input requires a changed complete replacement payload.
   Only members whose sanitized payload exactly matches the representative qualify.
   Differing payloads, redacted/private fields, or truncated evidence escalate.
7. Bug/resource/unknown categories and all remaining cases escalate.

Circuit opening also passes the confidence gate. The independent repeated-failure
count uses distinct existing tasks, not Kafka delivery count or orphan audits.

Actions row-lock the task and incident and recheck status/generation. They commit
`AI_DIAGNOSED` followed by recovery or `ESCALATED`, the diagnosis/action/reasoning,
and a decision audit together. Resolved/manual-retried/cancelled incidents are
skipped with an audit, preventing races from undoing an operator's decision.

Recovery increments the execution generation and AI recovery count, resets current
attempts, clears result/error, and retains previous attempts. Corrected payloads set
`tasks.ai_modified=true`, also exposed by the task API and dispatch envelope. A
future recovery commits SCHEDULED and the existing `delayed_tasks` ledger; the
scheduler restores its Redis index and creates a pending outbox event when due.
A zero-delay configuration instead commits PENDING plus an immediate outbox event.
Production defaults use a 30-second minimum and 3600-second maximum delay.

`dead_letters.ai_diagnosis` stores the validated diagnosis plus action, reasoning,
cluster ID, prompt version, provider/model, timestamp, and shared cluster usage.
Every member references the same representative usage; do not sum those duplicated
per-member values as independent model calls. A manual payload override clears the
current AI-modified flag; historical diagnoses and the lifetime AI retry count remain.

Escalation sets the task and incident ESCALATED and writes an
`ESCALATION_NOTIFICATION` audit event to the **local webhook/Slack stub**. No external
webhook is called and no Slack message is sent in this phase.

## Guardrails and audit

Each LLM turn (including repair) passes `guardrails.py`. Maximum steps default to
4, with at most four read-only tool calls per turn, bounded tool output, and a
bounded total diagnosis timeout. Invalid JSON/schema receives exactly one repair
attempt, within the same step/budget limits. Another invalid output, refusal,
provider error, step exhaustion, or timeout becomes a valid unknown/zero-confidence
fallback and escalates.

Recursive redaction removes sensitive field values, known configured secrets,
common PII fields, emails, phone/SSN/card patterns, auth tokens, URL credentials,
and IP addresses from evidence before egress. It covers initial context, tool
results, later model turns, repair, and audit. Original task data stays in Postgres;
a transient retry uses that original payload. A redacted placeholder is never
written back as a correction. Large evidence becomes a bounded sanitized preview
with a hash. Prompts explicitly mark task/log content as untrusted data and forbid
credential recovery, arbitrary tools, shell execution, or invented private values.
Pattern-based redaction is not a universal detector of every possible private datum;
keep domain-specific sensitive values in recognized fields or extend the redactor.

A shared Redis-time UTC-hour budget atomically reserves worst-case input/output
usage for **all configured provider attempts before egress**. The input bound uses
UTF-8 request bytes plus conservative framing slack; max_tokens bounds output.
Known unused usage is refunded once. Failed/unknown attempts and process crashes
retain conservative charges until the hour expires. Both token and integer
micro-dollar limits must fit; Redis failure blocks the call. Defaults are one
million tokens and 5 USD per hour across agent replicas. Usage/cost are operational
estimates under configured rates, not a provider invoice reconciliation.

`AGENT_KILL_SWITCH=true` blocks all model calls and automatic recovery and escalates
new incidents with an audit reason. It is checked again at action time. Set it in
`.env` and run `docker compose up -d --wait agent` to update the process environment.
`AGENT_ENABLED=false` instead pauses inbox processing and leaves OPEN incidents for
later processing. Both modes remain healthy when dependencies are available.

`agent_audit` retains append-only events for cluster assignment/claims, collected
context, pre-egress reservation and prompt hash, response/usage, tools, schema
repair, validated diagnosis, skipped decisions, actions, and notification stubs.
Decision rows commit with state changes. Payloads in this audit are redacted;
application logs never intentionally dump raw model requests or credentials.

Example read-only inspection:

```sh
make psql
```

```sql
SELECT id, task_type, status, representative_id, usage FROM agent_clusters ORDER BY created_at DESC LIMIT 10;
SELECT event, details FROM agent_audit ORDER BY created_at DESC LIMIT 30;
SELECT task_id, ai_diagnosis FROM dead_letters WHERE ai_diagnosis IS NOT NULL ORDER BY created_at DESC LIMIT 10;
```

## Circuit breaker

`shared/circuit.py` owns one Redis hash per task type. OPEN traffic is parked in the
durable scheduler ledger, with its Kafka input acknowledged and **zero attempts
consumed**. Other types sharing the partition can proceed. After cooldown one worker
atomically gets a HALF_OPEN probe. Other workers remain parked. The probe renews
with the task lease; a successful owned probe closes the circuit and a failed probe
starts another cooldown. Expired probes permit takeover. Epoch/token checks prevent
old attempts from closing a newer circuit. The scheduler must be running to release
parked tasks, as in the standard Compose stack.

Circuit intent is also stored with its cluster and restored after Redis loss using
the remaining DB deadline. Reconciliation is bounded and cursor-based; restoring
an older cluster cannot shorten a newer trip. Default cooldown is 30 seconds.

## Tests and changed files

Added `tests/unit/test_agent_guardrails.py` and `tests/integration/test_agent.py`.
They cover every category, repair success/failure, strict schema validation, tool
scope/protocol/steps, redaction, low confidence, kill-switch races, both shared hourly
limits, corrected execution/history, concurrent clustering and late-member reuse,
expired claim recovery, orphans, durable backoff, worker parking, single probes,
probe fencing, and circuit restoration ordering. Claude HTTP tests use HTTPX mock
transport, not a live account. Existing shared migration tests cover schema reversal
and the new mappings. The full suite includes all earlier phases.

Created:

- `services/agent/{__init__,__main__,main,core,collector,diagnosis,policy,guardrails,audit,schemas}.py`
- `services/agent/llm/{__init__,base,anthropic,mock}.py`
- `services/agent/prompts/diagnosis.v1.md`, `services/agent/Dockerfile`
- `shared/circuit.py`, `migrations/versions/0005_agent_orchestration.py`
- `scripts/check_agent.py` and the two test files above
- `docs/agent.md`

Changed:

- `shared/config.py`, `models.py`, `tables.py`
- `services/worker/executor.py`, `retry_engine.py`
- `services/scheduler/core.py`, `services/api/dlq.py`
- `docker-compose.yml`, `.env.example`, `Makefile`, `pyproject.toml`, `README.md`
- `tests/integration/test_shared.py`, `docs/architecture.md`, `docs/dlq.md`

## Recorded validation

The full suite passed **272 tests**, including **52 new agent tests**. Ruff and
format checks passed, Compose configuration validated, and Alembic reported
`0005 (head)` with no schema drift. The deployed MockLLM checker passed all five
checks: one diagnosis for two DLQ tasks, AI-corrected success with retained history,
delayed recovery/accounting, safe escalation/stub audit, and readiness/metrics.
The live stack was also verified with three workers and two schedulers.
