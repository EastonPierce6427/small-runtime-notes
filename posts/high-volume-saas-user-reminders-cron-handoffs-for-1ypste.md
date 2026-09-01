# High-Volume SaaS User Reminders: Cron Handoffs for Email and SMS Workers

Use cron as the handoff point, not as the delivery engine, when a SaaS has to send a daily batch of user reminders through email and SMS providers. The job should select due reminders, enqueue durable work, and stop; workers should own provider rate limits, retries, acknowledgements, and the final delivery record.

**Short answer:** for high-volume daily reminders, use a bounded cron scan plus an idempotent queue and provider-throttled workers. This pattern keeps a growing send batch from turning into one long web request, while making duplicate delivery and backlog visible enough to operate.

The guarantee is the design decision. “The scheduler ran” is not the same as “the reminder was delivered,” and a successful provider request is not the same as “the user saw it.” Keep those states separate.

## Should a SaaS use cron to enqueue daily email and SMS reminders?

Yes, when the required guarantee is durable handoff followed by at-least-once processing. A cron invocation is good at creating a dispatch window: find rows whose `send_at` is due, claim them, and publish one job per intended channel. It is a poor place to wait for every email and SMS provider response.

That split matters in a Node.js SaaS just as it does in any other runtime. Node.js can scan and publish efficiently, but no runtime makes provider latency, 429 responses, or a suddenly larger customer cohort disappear. Enqueue first. Pace later.

The queue must carry a stable delivery identity, not only a user ID. A useful key is the reminder occurrence plus channel, such as `rem-1842:2026-08-10:email`. The worker stores that key in a durable delivery table with a uniqueness constraint. A repeated queue message then becomes a lookup and no-op instead of a second send.

This is deliberately an at-least-once design. Queue acknowledgement is a statement about the worker's handling of a message; it is not proof that a remote mailbox or phone accepted the content. RabbitMQ's acknowledgement documentation describes the distinction between acknowledgements, redelivery, and publisher confirms, which is the right mental model even when the queue implementation differs.

## Where does the direct cron-to-provider pattern fail?

The first failure is a moving runtime boundary. A direct loop scans users, calls a provider, sleeps for a rate limit, and repeats. Its duration grows with the batch and with every retry. The next schedule can overlap the old one, or the scheduler can terminate the run after it has sent some reminders and before it has recorded the rest.

The second failure is a misleading success signal. A process can exit cleanly after publishing half its intended work, or it can report an error after the provider accepted a request but before the local process persisted the result. Retrying the whole batch from that ambiguous point is how duplicate messages enter a user's inbox.

The third failure is independent channel pressure. Email and SMS rarely have identical provider limits, error behavior, or acceptable retry delays. One shared loop makes a slow SMS provider hold up email. One uncoordinated limit per worker can also multiply the account-wide rate by the worker count.

Short logs make this worse.

Record the dispatch window, claimed count, published count, queue identifier, delivery key, provider response class, next attempt time, and terminal state in application storage. Scheduler output is a useful pointer, not the audit trail. A scheduled workflow can be delayed or skipped by platform conditions, so recovery should query the missed window and apply the same idempotency rule rather than assuming that another tick will repair it. GitHub's workflow trigger documentation is a useful reminder that scheduled execution is a trigger mechanism with platform-specific timing and branch rules, not a delivery guarantee.

## How should the cron, enqueue, batch, and worker boundaries work?

Give each component one failure it can explain. The scheduler claims a finite window. The publisher makes the handoff. The worker applies channel policy. The delivery record resolves ambiguity.

| Component | Owns | Must not pretend to own |
| --- | --- | --- |
| Cron trigger | Starting a bounded scan | Successful delivery |
| Scanner and publisher | Selecting due rows and publishing stable jobs | Provider pacing |
| Email or SMS worker | Rate limiting, retry classification, and acknowledgement timing | A user's receipt or attention |
| Delivery store | Idempotency and terminal state | The queue's transport guarantees |
| Runbook | Reconciliation, pause, and replay decisions | Automatic recovery from every outage |

Batching is useful at the database and enqueue boundaries. Claim a page of due rows, publish that page, persist the handoff, then continue. It is not a reason to put a thousand provider calls in one queue message. Smaller jobs let workers share capacity across tenants and channels, and they make a retry target specific.

The worker should acknowledge only after it has made a durable decision. For a successful provider response, record the provider reference and mark the delivery sent before acknowledging. For a retryable response, persist `next_attempt_at`, leave the job available through the queue's retry mechanism, and apply backoff. For a permanent rejection, record the reason and acknowledge so the same bad address does not spin forever. If the provider response is ambiguous, keep the delivery key authoritative and reconcile before sending again.

Here is a small Go worker skeleton. It does not implement a queue client or a provider SDK; those interfaces are intentionally generic. The important part is the order of the idempotency check, send decision, durable result, and acknowledgement.

```go
package main

import (
	"context"
	"fmt"
	"time"
)

type Job struct {
	DeliveryKey string
	Channel     string
	Address     string
	Body        string
}

type DeliveryStore interface {
	Claim(ctx context.Context, key string) (alreadyDone bool, err error)
	MarkSent(ctx context.Context, key, providerID string) error
	MarkRetry(ctx context.Context, key string, next time.Time) error
}

type Provider interface {
	Send(ctx context.Context, channel, address, body string) (providerID string, retryable bool, err error)
}

type Message interface {
	Job() Job
	Ack(ctx context.Context) error
	Retry(ctx context.Context, after time.Duration) error
}

func handle(ctx context.Context, msg Message, store DeliveryStore, provider Provider) error {
	job := msg.Job()
	done, err := store.Claim(ctx, job.DeliveryKey)
	if err != nil {
		return err
	}
	if done {
		return msg.Ack(ctx)
	}

	providerID, retryable, err := provider.Send(ctx, job.Channel, job.Address, job.Body)
	if err != nil {
		if !retryable {
			return store.MarkRetry(ctx, job.DeliveryKey, time.Now().Add(24*time.Hour))
		}
		return msg.Retry(ctx, time.Minute)
	}
	if err := store.MarkSent(ctx, job.DeliveryKey, providerID); err != nil {
		return err
	}
	return msg.Ack(ctx)
}

func main() {
	fmt.Println("worker policy: durable delivery key, provider pacing, acknowledge last")
}
```

The skeleton leaves one important production choice visible: the store operation must be transactional or otherwise concurrency-safe. Two workers can receive the same delivery key. The first successful claim owns the send decision; the second must not race past that decision. Exact provider semantics vary, so your contract should document what an accepted request means and how an ambiguous response is reconciled.

## What should verification and rollback look like for email and SMS?

Test the boundaries before testing volume. Feed a small due window containing one email reminder, one SMS reminder, an already-claimed delivery, and a retryable provider response. Verify that the claimed count equals the intended population, the published count is independently recorded, and a redelivered message does not create a second provider call.

Then test the operational cases that are easy to skip:

- Run two cron invocations for the same window and confirm that the delivery key prevents duplicate jobs from becoming duplicate sends.
- Start more workers than usual and confirm that email and SMS each stay within their own provider rate limit.
- Stop a worker after the provider call and before acknowledgement, then confirm that redelivery is harmless.
- Pause the schedule and reconcile due rows explicitly before resuming normal cadence.

Watch queue age, retry age, per-channel throughput, provider response classes, duplicate-suppression count, and the gap between selected and terminal delivery records. Alert on an old queue item, not only on a dead scheduler process. A scheduler that fires on time while workers are stalled is still a failed reminder system.

The catch is that cron plus a queue is not suitable when the business needs a strict per-user execution time, a multi-step workflow with compensation, or a replayable event history as the primary record. Choose a workflow engine for durable timers and orchestration, or a log-oriented system when independent consumers and historical replay are central requirements. Keep the simpler pattern when the job is a bounded scan followed by channel-specific delivery.

Rollback should stop new selection first. Pause the schedule, leave already claimed jobs visible, and decide whether workers should drain, pause, or move retryable work aside. Never replay a whole daily batch just because a deployment was reverted. Reconcile by delivery key, resend only eligible records, and preserve the original attempt history so the next incident starts with evidence.

## References

- https://www.rabbitmq.com/docs/confirms
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
