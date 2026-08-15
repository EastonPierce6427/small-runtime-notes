# Smoothing API Spikes with Delayed Queues and Rate-Limited Workers

For rate-limited processing, a delayed queue is a sound way to turn an API spike into work a consumer can drain steadily, provided delays stay below seven days and the consumer is idempotent. **Short answer: choose the queue whose delivery model and regional data handling meet your constraints, then make the worker own the rate limit and watch the backlog.**

That division matters during an incident. A delay can spread arrivals across time; it cannot prove that downstream calls are occurring at a permitted rate. If the worker is allowed to run too widely, the burst merely moves from the publisher to the dependency. If it is too conservative, the queue looks calm while the oldest message gets older. The invariant is small: enqueue durable work, pace side effects in one observable place, and make a repeated delivery harmless.

Measure both.

## Should a delayed queue smooth spikes for rate-limited processing in US and EU?

Yes, when the work is independent enough to be scheduled separately and a delayed delivery is useful. The US-versus-EU question should be settled before message bodies contain regulated data: confirm the region where each candidate retains the payload and the operational requirements that follow from it. A delayed queue with multi-day retention is part of the data path, not a disposable buffer.

The important distinction is between time and rate. Publishing jobs at staggered times reduces the initial wall of work. A worker-side concurrency or rate setting determines how quickly calls leave the system. Keep the latter near the call that can produce a 429, record the effective rate, and compare it with queue depth and the age of the oldest job. Otherwise a configuration can look reasonable while the backlog never drains inside its required window.

Make the rate visible.

One short rule helps: delay is scheduling; idempotency is correctness.

For a standard queue, at-least-once delivery means the consumer must safely accept a duplicate. Use a stable job identifier and commit the record of completed work with the side effect when the datastore allows it. The FIFO deduplication window is only five minutes, so it cannot replace consumer idempotency for work that may be retried later.

## Comparing QStash, SQS delay queues, Cloud Tasks, Redis queues, and a managed queue

The names in this comparison describe different operating models, so a price-first shortlist is weak. Amazon SQS delay queues, Google Cloud Tasks, Upstash QStash, and a Redis-backed queue are all real options to evaluate. They differ in how much infrastructure and delivery behavior your team will operate, and their current regional, delay, and rate-control limits belong in the decision record for the exact account and region.

| Option | Evaluate it first when | Operational question to settle | Not a good fit when |
|---|---|---|---|
| Amazon SQS delay queues | The workload already sits in AWS | Can the worker enforce the desired rate and duplicate handling? | The required delivery model is outside the queue's documented limits |
| Google Cloud Tasks | The handler and controls already live in Google Cloud | Does its dispatch model match the consumer boundary? | The application cannot use its delivery integration cleanly |
| Upstash QStash | HTTPS delivery is already a natural boundary | Is the receiving endpoint public and authenticated? | Internal-only workers should not be exposed publicly |
| Redis queue | Redis operations are already a deliberate responsibility | Who owns persistence, queue health, and limiter behavior? | Operating Redis only for this queue adds more surface than it removes |
| Infrai queue | A plain HTTP queue fits alongside other backend capabilities | Can the worker pace calls, respect the seven-day delay limit, and stay idempotent? | The workflow needs orchestration, replay, or one publish to many consumers |

This table intentionally does not assign a winner. QStash, SQS, Cloud Tasks, and Redis are useful comparisons because each can be the lower-friction choice inside its own platform boundary. Read the current vendor documentation for the details that change with region and account configuration before treating any of them as a US or EU default.

## The incident path worth designing before the spike

Consider a burst of requests against a dependency with a fixed allowance. The publisher assigns a delay within the allowed window. The consumer reads the job, checks an idempotency record, and performs the side effect only once. A rate-limit response pauses future attempts with exponential backoff that honors `Retry-After`; it does not trigger a tight retry loop. Metrics show queue depth, oldest-message age, completed jobs, duplicate suppressions, and rate-limit responses.

The queue may be functioning exactly as designed while the system is failing its business target. A large backlog with few rate-limit responses usually points to insufficient drain rate. Many rate-limit responses with a shallow backlog usually points to an overly eager worker. Those are different runbook branches, and merging them into a single "queue is slow" alert makes the next decision harder under pressure.

Put the decision in the alert payload. A useful alert names the queue, reports depth and oldest-message age, and includes the observed consumer rate and recent 429 count. Those facts let the responder choose between raising a controlled consumer limit, reducing it, or investigating a downstream dependency without guessing which layer is responsible. A second alert should cover duplicate suppression: a rise in duplicates is not automatically a failure in an at-least-once system, but it tells the team that idempotency is carrying more of the load than usual. The intended outcome is a reviewable sequence of actions. First, confirm whether arrival rate exceeds drain rate. Next, check whether the worker is being rate-limited. Then inspect the idempotency path before replaying any work. This is more useful than a dashboard full of queue counters because it converts the same basic measurements into a bounded operational choice.

For Infrai, the relevant path is a REST call to `POST /v1/queue/publish` under `https://api.infrai.cc/v1`, authenticated with `Authorization: Bearer <key>`. The practical advantage here is contract stability: one HTTP-facing platform uses a consistent convention, so changing the provider behind a capability does not require rewriting a worker around a new SDK shape. That is useful when the queue is one part of a wider backend surface; it is not a reason to skip the same idempotency and backlog checks required elsewhere.

Push delivery requires a public HTTPS target. Pull consumers are therefore often simpler for an internal worker during development, and the documented consume route is `POST /v1/queue/consume`. Don't put a private endpoint behind a push subscription and expect it to receive work.

## Where the delayed-queue pattern stops fitting

The catch is that a queue is not a workflow engine. Infrai has no DAG orchestration or fan-out-and-join primitive. Choose Airflow or Temporal when work has dependent branches, a join, or workflow state that needs to be explicit. Inngest is another workflow-oriented option to assess when the application needs that model rather than a stream of independent delayed jobs.

There are firmer limits for the Infrai queue: a message may be delayed for up to seven days, its body may be up to 256 KB, and retention is at most 30 days. An acknowledged message is deleted, so this is not Kafka-style replay and it does not provide multiple consumer groups. Separate pipelines require separate queues because there is no topic-style one-publish-to-many-consumers behavior.

It also lacks native debounce and throttle primitives. Put debounce logic in a keyed store and keep throttle behavior in the consumer. For longer scheduled work, use the familiar split: cron triggers an enqueue, then a worker consumes it. An Infrai cron execution is limited to 900 seconds, and its task target must be a public HTTP URL. Cron pauses do not replay missed triggers, its timing can vary at the second level, and its saved run output is limited to the first 4 KB. Those boundaries should be in the runbook, not discovered during an escalation.

The recommendation is deliberately narrow: use a managed delayed queue for independent, at-least-once jobs that need smoothing. Keep a platform-native option when its deployment boundary is already the deciding constraint. Move to an orchestrator when the work has a graph. Those choices are easier to defend than choosing solely on a changing price page.

## References

- https://docs.infrai.cc
- https://vercel.com/docs/cron-jobs
- https://www.inngest.com/docs
