# Nodejs Background Jobs Under Load: Queue Retry Policy and Exponential Backoff

Short answer: for outbound healthtech webhooks, acknowledge a queue message only after the delivery result has a durable next state, reuse one idempotency key across every attempt, retry transient failures with capped exponential backoff and jitter, and move exhausted or permanent failures to a dead-letter queue (DLQ).

Start with five attempts and delays near 15 seconds, 45 seconds, 2 minutes, and 6 minutes. Those numbers are a launch policy, not a law. Tighten them only after observing the receiver's recovery time and the clinical workflow's latency budget. A reminder feed can usually wait; a time-sensitive handoff may need a faster first retry even if it creates more queue traffic.

I've been paged by missed jobs and duplicate deliveries. The useful lesson is plain: **the retry timer is secondary to the delivery state machine**. A worker must be able to crash after sending a webhook without turning recovery into a second business event.

Retries spend capacity.

Before choosing delays, write down the maximum acceptable delivery age for each webhook class and the amount of retry traffic the worker pool may consume while a destination is unavailable. Then walk one concrete event through the failure: `care-plan.updated:evt_7f3` enters the queue, the first request reaches the destination, and the worker loses its chance to record the result. The queue exposes the job again. A fast retry may restore a missed delivery, but it may also repeat an effect that already happened; a thousand events aimed at the same recovering destination can then compete with healthy destinations for workers. The runbook needs three controls before this occurs: a stable idempotency key for the effect, a per-destination concurrency limit for isolation, and an attempt budget that ends in quarantine. These controls answer different questions. Idempotency contains ambiguity. Concurrency protects unrelated work. The attempt ceiling prevents delayed messages from becoming permanent background load. Set the early delays from the workflow's latency allowance, and set the later cap from how much stale traffic the service can afford to carry. This is the real latency-versus-cost decision — a formula cannot make it on the team's behalf.

Keep sensitive clinical content out of retry metadata and routine logs. The queue needs enough information to locate the protected payload, choose a policy, and correlate attempts. It doesn't need a patient's name or the full webhook body. A practical log line can contain `delivery_id=delivery_7f3`, `attempt=4`, `decision=dead_letter`, and a low-cardinality failure class. That is specific enough for a runbook without copying the event itself into every operational system.

Classify before retrying. A temporary outcome enters backoff. A permanent outcome goes directly to quarantine. An unknown outcome deserves a conservative, bounded retry because the sender cannot prove non-delivery, but the idempotency contract must already be in place. If the team cannot state which failures belong to each class, enabling automatic redrive is premature.

## Can a state machine keep Nodejs queue retry, exponential backoff, and DLQ handling simple?

The application can be written in Node.js while the policy remains language-independent: compute the next state from the attempt number and failure class, then let a thin queue adapter apply that decision. Separate policy from transport so a unit test can cover every transition without waiting on a real delayed message.

Use a capped exponential schedule with jitter. A simple form is `min(cap, base * 2^attempt)`, adjusted within a small deterministic jitter window. The cap prevents one bad destination from creating an ever-growing delay, while jitter avoids releasing every failed delivery at the same instant. Deterministic jitter, derived from the stable event key, also makes a failed test repeatable.

The implementation below makes no assumption about a particular queue. Its output is one of three commands: schedule another message, acknowledge completion, or quarantine the delivery. The queue adapter must make the selected handoff durable before acknowledging the current message; the exact transaction boundary depends on the queue and delivery store chosen by the team.

```go
package retry

import (
	"crypto/sha256"
	"encoding/binary"
	"time"
)

type FailureClass int

const (
	Temporary FailureClass = iota
	Permanent
)

type Action int

const (
	Schedule Action = iota
	Quarantine
)

type Job struct {
	DeliveryID    string
	IdempotencyKey string
	Attempt       int
}

type Decision struct {
	Action Action
	Delay  time.Duration
	Next   Job
}

type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func Decide(job Job, class FailureClass, policy Policy) Decision {
	if class == Permanent || job.Attempt+1 >= policy.MaxAttempts {
		return Decision{Action: Quarantine, Next: job}
	}

	next := job
	next.Attempt++
	delay := policy.BaseDelay
	for n := 0; n < next.Attempt && delay < policy.MaxDelay; n++ {
		delay *= 2
	}
	if delay > policy.MaxDelay {
		delay = policy.MaxDelay
	}

	return Decision{
		Action: Schedule,
		Delay:  addJitter(delay, job.IdempotencyKey),
		Next:   next,
	}
}

func addJitter(delay time.Duration, key string) time.Duration {
	sum := sha256.Sum256([]byte(key))
	percent := int64(binary.BigEndian.Uint16(sum[:2])%21) - 10
	return delay + time.Duration(percent)*delay/100
}
```

Treat `MaxAttempts` as total tries, including the initial delivery, and name it that way in configuration. Off-by-one errors hide easily when one component calls the first try attempt zero and another calls it attempt one. For the sample five-attempt policy, test the exact sequence and assert that the fifth failure produces quarantine rather than another delayed message.

There is one more boundary: publishing a replacement and acknowledging the source as two unrelated operations can lose work or create extra copies during a crash. Prefer a queue feature that changes message visibility or schedules retry state without a client-side republish. If the transport requires republishing, persist an outbox record with a unique transition key, publish from that record, and make repeated publication harmless. The names vary; the invariant doesn't.

## Allocate queue capacity by delivery deadline

Give each logical event a stable key such as `care-plan.updated:evt_7f3`. Store a delivery record keyed by the destination and that event key. Each queue message carries the same key, the current attempt, and an opaque reference to the payload. The receiver should use the key to suppress repeated effects; the sender should also use its delivery record to avoid knowingly dispatching an event already marked complete. Two layers matter because neither side controls every failure boundary.

Fast retries reduce recovery latency when a receiver has a brief interruption. They also spend worker time and request volume on a destination that may remain unavailable. Slow retries reduce that pressure but can leave a healthtech integration stale long after the receiver has recovered. **Use the business deadline to set the early schedule, then use dependency recovery data to set the cap.**

| Job class | First retry | Later behavior | Stop condition |
|---|---:|---|---|
| Time-sensitive handoff | Short | Grow quickly with jitter | Deadline or attempt ceiling |
| Routine synchronization | Moderate | Grow to a longer cap | Attempt ceiling |
| Invalid destination or payload | None | Quarantine immediately | Permanent classification |

The catch is that a small queue-and-DLQ design is not suitable when one delivery expands into a durable, multi-step clinical workflow with branching, compensation, human approval, or state that must remain queryable for months. Use a workflow engine for that shape. Stick with a queue when the unit of work is one independently retryable side effect and the operational team can explain its terminal state in a sentence.

I'm not sure what retry cap is right for an integration until its latency objective and receiver behavior are visible. Your mileage may vary — especially when destinations range from hospital systems to small partner services — so keep the policy configurable by destination class, not by individual customer. Per-customer tuning becomes configuration debt and makes incidents harder to compare.

## Version configuration before rollout and rollback

Test the state machine before testing the adapter. The minimum matrix covers immediate success, one temporary failure followed by success, a permanent failure, exhaustion at the exact attempt ceiling, and duplicate execution with the same idempotency key. Add a crash test immediately after the remote call and another immediately after scheduling the next attempt. Those two points expose the ambiguity that happy-path tests miss.

Then run a controlled delivery against a receiver that records keys. Send `care-plan.updated:evt_7f3` twice and verify one business effect, two observable requests, and one completed sender record. Force the sample through attempts 1 through 5 and verify that only the terminal copy enters the DLQ.

No guesswork.

Deploy policy changes as versioned configuration. New deliveries take the new version; already queued deliveries retain the version that created them. Watch completion latency, attempts per completed delivery, queue age, DLQ entries, and duplicate-suppression counts. An increase in suppressions can mean the receiver is protecting the business effect while the sender's handoff boundary is still noisy.

Rollback means stopping new retries under the new policy, not deleting evidence. Revert the policy version for new work, pause automated DLQ redrive, and let in-flight messages finish under their recorded version unless continuing would deepen impact. Inspect a bounded DLQ sample, correct the classification or dependency, then redrive a small batch while watching both completion and repeat-failure rates. Keep the original event key during every redrive.

A DLQ is quarantine, not success and not a trash can. Every entry needs an owner, a reason, a retained idempotency key, and a defined disposition: correct and redrive, resolve manually, or expire under the system's data policy. If nobody reviews it, the queue has merely converted a visible failure into a quieter missed webhook.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- https://docs.bullmq.io/

## Further reading

The queue documentation above provides the primary material for dead-letter handling and delayed retry behavior. Revisit both when selecting a transport, because the safe acknowledgment and redrive mechanics depend on the concrete queue contract.
