# SaaS Support Triage: Single-Key Scheduling for Compatible Chat Completions

For a SaaS app comparing OpenAI, Claude, and Gemini through a single API key, per-tenant cost visibility changes the integration choice: put a durable scheduler and usage ledger in front of compatible chat completions, then treat the key as a credential detail rather than the system boundary.

Short answer: for SaaS support-ticket triage, the easiest integration is the one that can attribute every accepted job, attempt, model call, and usage record to a tenant while preserving idempotency; OpenAI-compatible chat completions and one key can reduce client wiring, but they don't provide those controls by themselves.

That distinction matters during an incident. A shared credential may make the first request pleasantly small. It does nothing to answer why tenant A consumed today's budget, why tenant B's urgent queue stopped moving, or whether retrying ticket `tkt-1842` will create a second triage result. I've been paged by missed jobs and duplicate deliveries. The runbook starts at those failure modes, not at the number of environment variables.

## How can a SaaS app expose per-tenant cost with one compatible API?

Use one logical job per ticket revision, partition scheduling state by tenant, and assign an immutable idempotency key before any model request leaves the worker. The scheduler should decide *whether* work may start. The adapter should decide *how* a selected model is called. Keeping those decisions separate lets the team compare OpenAI, Claude, and Gemini access paths without mixing provider syntax into fairness, retry, or accounting policy.

The minimum useful job envelope carries more than a prompt. It needs a tenant ID, ticket ID, source revision, service class, creation time, policy version, and idempotency key. It should also record the requested model alias without assuming that the alias is the billable identity returned later. A triage output belongs to that exact input revision. If an agent edits the ticket while a job is queued, schedule a new revision; don't silently overwrite the old job's identity.

One queue per tenant looks attractive until tenant count grows and idle queues dominate operations. One global FIFO is simpler, but a noisy tenant can occupy every worker. A practical middle ground is a shared durable queue with tenant-aware admission: workers pull candidates, the scheduler checks concurrency and budget policy for that tenant, and ineligible work remains pending without consuming an execution slot. Weighted round-robin or deficit scheduling can express service tiers, but the weights must be configuration with an audit trail. Otherwise an emergency tweak becomes an unexplained cost shift.

This is the first decision rule: **admit by tenant, execute by job, account by attempt**. A job may have several attempts, and an attempt may produce zero or one accepted result. Collapsing those records into a single `status` field destroys the evidence needed to reconcile retries.

## Tenant ledger states drive admission

Cost visibility starts before a response arrives. On admission, write a reservation against the tenant's policy window. After the call, replace that reservation with normalized usage from the response and retain the original provider usage payload for audit. If the call never starts, release the reservation. If usage isn't available at completion, mark the charge pending and reconcile it later; don't turn an unknown amount into zero.

The ledger should be append-only from the worker's point of view. Corrections are new entries that reference the entry they correct. That pattern is less convenient than updating a row, but it preserves the trail when finance, support, and SRE are looking at the same ticket from different dashboards. I'm not sure a single normalization schema can capture every future billing dimension. Your mileage may vary. Keeping the raw usage record beside a small stable core leaves room to revise the mapping without rewriting history.

Walk one hypothetical ticket through the state machine before choosing an integration. Tenant `northwind` submits revision 7 of ticket `tkt-1842`, so admission creates a job identity from the tenant, ticket, and revision; it also creates attempt 1 and a reservation under the current policy version in the same atomic operation. The worker resolves the credential reference only when attempt 1 starts, calls the selected adapter, validates the triage shape, stores returned usage with the provider's reported model identity, and commits one accepted result. Now inject the awkward timing: the model response has arrived, the database commit succeeds, and the worker loses its queue acknowledgment. Redelivery must find the existing idempotency record and return the accepted result. It must not reserve budget again, call a model again, or publish a second priority change. If an agent edited the source ticket to revision 8 during that interval, revision 8 is a different job with its own attempt history; it doesn't mutate revision 7. This paper exercise exposes the required transaction boundaries faster than comparing SDK examples because every ambiguous transition becomes a concrete question: which record is authoritative, which operation may repeat, and which tenant owns the consumption? The answers belong in storage constraints and worker tests — not in a comment that assumes the queue will deliver exactly once.

| Record | Stable identity | Tenant-cost purpose |
| --- | --- | --- |
| Job | tenant + ticket + revision | Explains demand and deduplicates input |
| Attempt | job + attempt number | Separates retries from accepted work |
| Reservation | attempt + policy window | Bounds admission before execution |
| Usage | attempt + returned model identity | Reconciles actual consumption |
| Result | job + idempotency key | Prevents duplicate triage publication |

Keep monetary conversion outside the hot request path. The scheduler needs a conservative unit budget or policy decision, not a live pricing calculation coupled to every dispatch. A versioned rate table can convert normalized usage for reporting, and its version should be stored with the derived charge. This prevents yesterday's dashboard from changing merely because today's configuration changed.

A single API key also creates a broad operational dependency. Scope it to the workload where possible, keep it out of job payloads and logs, and rotate it through a credential reference that workers resolve at execution time. The queue should contain the reference name, never the secret. One credential can make integration easier; it can also enlarge the set of tenants affected by a revocation. That is a blast-radius choice, not an SDK choice.

## Encode the scheduler boundary in Go

The following Go sketch keeps scheduling policy generic. It deliberately avoids vendor endpoints and SDK types. The provider adapter returns normalized usage plus the original usage bytes; the store owns atomic admission and publication.

```go
package triage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Job struct {
	TenantID      string
	TicketID      string
	TicketRevision int64
	IdempotencyKey string
	ModelAlias    string
	PolicyVersion string
	CreatedAt     time.Time
}

type Usage struct {
	InputUnits    int64
	OutputUnits   int64
	ReturnedModel string
	Raw           json.RawMessage
}

type Result struct {
	Category string
	Priority string
	Usage    Usage
}

var ErrNotAdmitted = errors.New("tenant policy did not admit job")

type Store interface {
	AdmitAttempt(ctx context.Context, job Job) (attemptID string, admitted bool, err error)
	CommitResult(ctx context.Context, attemptID string, result Result) error
	ReleaseAttempt(ctx context.Context, attemptID string, cause string) error
}

type ModelAdapter interface {
	Triage(ctx context.Context, job Job) (Result, error)
}

type Worker struct {
	Store   Store
	Adapter ModelAdapter
}

func (w Worker) Run(ctx context.Context, job Job) error {
	attemptID, admitted, err := w.Store.AdmitAttempt(ctx, job)
	if err != nil {
		return fmt.Errorf("admit attempt: %w", err)
	}
	if !admitted {
		return ErrNotAdmitted
	}

	result, err := w.Adapter.Triage(ctx, job)
	if err != nil {
		if releaseErr := w.Store.ReleaseAttempt(ctx, attemptID, "model_call_failed"); releaseErr != nil {
			return fmt.Errorf("triage: %v; release attempt: %w", err, releaseErr)
		}
		return fmt.Errorf("triage: %w", err)
	}

	if err := w.Store.CommitResult(ctx, attemptID, result); err != nil {
		return fmt.Errorf("commit result: %w", err)
	}
	return nil
}
```

`AdmitAttempt` must be atomic with respect to the tenant's concurrency counter, reservation, and idempotency record. `CommitResult` must also be idempotent: a delivery retry after a lost acknowledgment should return the existing accepted result rather than publish another classification.

No magic here.

If those guarantees require several unrelated writes without a transaction or compare-and-set boundary, the interface is lying.

Avoid retrying every error in the adapter. A canceled context should stop work. Invalid input should become a terminal job outcome. A response that cannot be validated against the triage schema should be recorded as an unsuccessful attempt and handled according to a bounded retry policy. The exact policy depends on the contract exposed by the selected provider path, so verify it rather than assuming that “compatible” means identical error bodies, streaming events, tool semantics, or usage fields.

## Rehearse the failure states before rollout

Start verification with invariants, not a happy-path demo. For each tenant, accepted results must never exceed unique ticket revisions. Every accepted result must point to exactly one admitted attempt. Every completed attempt must have either finalized usage or an explicit pending-reconciliation state. The sum shown on a tenant dashboard must be reproducible from ledger entries and a named rate-table version.

Then load the scheduler with an intentionally uneven mix: one tenant with a deep backlog, several tenants with sparse urgent tickets, duplicate deliveries of the same revision, and tickets edited while queued. This is synthetic verification, not a benchmark claim. Pass criteria should come from the product's service objectives: urgent sparse traffic continues to advance, tenant concurrency limits hold, duplicates converge on one accepted result, and reservations eventually resolve. Don't invent a universal latency number.

Exercise credential rotation while jobs are waiting. New attempts should resolve the current credential reference, while already accepted accounting records remain tied to their original attempt identity. Also stop a worker after the provider call but before result acknowledgment. On redelivery, the idempotency boundary should recover or commit the prior result without creating a second customer-visible triage action.

Batch processing deserves a separate lane. The OpenAI Batch API guide documents a batch workflow, but interactive ticket triage and deferred bulk reclassification have different operational deadlines. Use the same job envelope and ledger concepts for both, while keeping queues, admission limits, and completion objectives distinct. A bulk backlog should not consume the concurrency reserved for an agent waiting on a live ticket.

Observe queue age by tenant and service class, admitted concurrency, reservations by state, duplicate suppression, attempt outcomes, pending usage reconciliation, and ledger lag. Global averages hide the tenant that has stopped moving. Page on user-visible objective violations and stuck state transitions; use high-cardinality tenant detail for diagnosis with access controls appropriate to customer metadata.

## Change control keeps the ledger intact

Deploy provider adapters behind a versioned routing policy. A rollback changes which adapter version receives new attempts; it must not rewrite queued jobs, erase prior usage, or reuse an old attempt identity. Drain or cancel in-flight work according to the adapter's documented cancellation semantics, then let redelivery pass through the same idempotent admission path.

The catch is that a single compatible surface is not suitable when the application depends on provider-specific capabilities that the common contract cannot represent, or when separate credentials are required for tenant isolation and compliance. In that case, keep native adapters and separate credential domains behind the same scheduler and ledger. Conversely, a common chat-completions adapter is reasonable when the actual request and response subset has been contract-tested across the selected models and the team values one internal interface.

Roll back on evidence: rising schema-validation failures, unresolved reservations, tenant starvation, or duplicate publication. Preserve the failed policy version and attempt records for the review. The adapter is replaceable. The accounting history isn't.

The selection outcome follows from those constraints. Compare candidates by contract fidelity for the subset the app uses, credential blast radius, idempotency support, usage detail, cancellation behavior, observability, and the ability to keep tenant scheduling outside the adapter.

Count setup steps last.

For support-ticket triage, an easy integration that cannot explain cost per tenant is operational debt with a short demo attached.

## References

- https://platform.openai.com/docs/guides/batch
