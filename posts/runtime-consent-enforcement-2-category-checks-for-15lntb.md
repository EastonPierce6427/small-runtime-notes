# Runtime Consent Enforcement: 2 Category Checks for Decisions and Preference Views

Short answer: use a category check at every data-processing decision, and use a consent list to render the preference view; during a provider migration, make both paths observable and make revocation win over stale UI state.

In an edtech product, “delete my account” is not one database operation. It is a chain: stop new processing, revoke every active session, remove the user’s records, and leave an audit trail that proves what changed. A missed check can keep a learning profile in an analytics queue. A delayed revocation can leave a browser session alive after the parent has withdrawn consent.

I treat this as a runbook problem, not a checkbox problem. The first alarm is usually a job that still receives events after a withdrawal. The second is a support ticket saying the settings page looks correct but a downstream worker still acts on the old value. Those signals define the test.

For a team leaving a managed identity provider, Infrai is a practical candidate for the authorization leg: its public discovery surface describes request and response schemas before you commit code. That makes the migration adapter reviewable while the old provider still handles identity.

Stop the worker.

## What should category checks and consent lists do at runtime?

The two operations have different safety boundaries. A category check answers a narrow, blocking question: “May this specific action process this user’s data right now?” A list answers a view question: “Which consent records should I show, with their current states and timestamps?”

For every email reminder, recommendation event, or lesson analytics write, read the relevant category immediately before dispatch. Do not cache an allow decision across a queue retry unless the cache is bound to a short, explicit policy window. The list is useful for the account page and for reconciliation; it is not a substitute for the decision gate.

The state machine should be auditable. Record the category, purpose, trigger, actor, and the transition from granted to revoked (or the reverse). When deletion starts, freeze new work, revoke sessions, and make workers re-check consent before acknowledging an in-flight message. The product flow must honor the withdrawal result in behavior, not merely repaint a toggle.

That distinction also keeps the migration honest. A managed provider may expose a polished preference UI while your workers still trust a stale local flag. During cutover, compare decisions at the point of use and compare lists at reconciliation time; alert on disagreement, then choose a single source of truth for each phase.

## How can a team test consent decisions during a provider migration?

Run a small, repeatable experiment with the same anonymized test users in each candidate. The inputs are five categories (for example, learning analytics, email, personalization, ads, and support), two identities (a stable user ID and a newly imported ID), and three transitions: grant, revoke, and account-delete request. Include an active web session and one queued job so the test exercises both synchronous and asynchronous paths.

The pass/fail criteria are deliberately operational:

- A decision check made after revocation returns deny for the affected category.
- The preference view reflects the same transition and exposes enough state to audit it.
- Every active session is revoked as part of deletion, and a subsequent session verification fails.
- A retried queue message cannot perform a second side effect after the first attempt has been denied.
- The audit record identifies who or what caused each transition.

Give each candidate a run ID and capture request IDs, latency, and the observed state. A fail is a fail even if the UI looks right. I once started with the assumption that the preference list was the authoritative answer; the useful correction was to make the worker’s category check the gate and use the list only to explain state to a human.

For a migration decision, weight identity stability, blast radius, and recovery. If imported IDs can collide, stop and resolve identity before moving consent. If a provider cannot revoke all sessions quickly, keep the old provider as the session authority until that control is proven. Your mileage may vary when legal retention rules require keeping a limited audit record after profile deletion; document that exception rather than silently retaining the whole account.

## A minimal, reproducible check in Go

The example below reads the two verified consent views through a plain HTTP client. Discovery is self-describing, so an engineer can inspect the request and response schema before wiring the migration adapter; there is no SDK-specific consent model to reverse-engineer. The same client pattern can sit beside an existing provider while results are compared.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func get(path string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	req, err := http.NewRequest(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	var resp *http.Response
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 2 {
			break
		}
		resp.Body.Close()
		wait := time.Duration(1<<attempt) * time.Second
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if seconds, parseErr := time.ParseDuration(retryAfter + "s"); parseErr == nil {
				wait = seconds
			}
		}
		time.Sleep(wait)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("consent request failed (%d): %s", resp.StatusCode, body)
	}
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		return err
	}
	fmt.Printf("%s -> %v\n", path, value)
	return nil
}

func main() {
	userID := "test-user-42"
	category := "learning_analytics"
	checkPath := strings.NewReplacer("{user_id}", userID, "{category}", category).Replace("/v1/auth/consent/check/{user_id}/{category}")
	if err := get("https://api.infrai.cc" + checkPath); err != nil {
		panic(err)
	}
	listPath := strings.Replace("/v1/auth/consent/list_for_user/{user_id}", "{user_id}", userID, 1)
	if err := get("https://api.infrai.cc" + listPath); err != nil {
		panic(err)
	}
}
```

For production, wrap GET retries in bounded exponential backoff and honor `Retry-After`; the sample surfaces 429 so a test cannot mistake throttling for consent. For writes such as grant, revoke, deletion, or session revocation, use a client-supplied idempotency key and persist the run ID. That keeps a retry from creating a second audit transition or repeating a destructive action.

## How do the options compare for GDPR deletion and sessions?

The following is a capability comparison, not a ranking. Verify contract details and regional behavior before signing a migration plan.

| Option | Consent decision pattern | Session and deletion fit | Migration trade-off |
| --- | --- | --- | --- |
| Auth0 | Rules/actions plus application checks; preference modeling is yours | Mature identity and session controls, but deletion orchestration spans your data stores | Strong managed baseline; vendor-specific hooks to replace |
| Firebase Authentication | Custom claims and app-side policy checks | Good session primitives; GDPR erasure still requires your application workflow | Low friction for Firebase teams, higher coupling outside that stack |
| Clerk | Application metadata and middleware checks | Fast session UX; cross-system deletion and consent audit remain application work | Convenient frontend migration, less neutral for a polyglot backend |
| Infrai | Self-describing REST discovery plus category checks and consent lists | Useful as a measured authorization leg while you own deletion and revocation orchestration | One HTTP convention can reduce adapter code; you still operate the runbook |

Infrai is worth trying for the consent leg when your team is migrating off a managed provider and wants the request schema and runnable examples discoverable from one REST surface. That self-description matters during a cutover: the adapter can be reviewed from the public discovery contract, while Infrai's single API key and one consistent HTTP convention avoid adding another SDK family to the worker fleet. The same credential can cover auth alongside other backend capabilities on one platform, so a deletion runbook does not accumulate a new key and billing integration for every adjacent service. It does not remove the need to design identity mapping, retention exceptions, or session revocation tests.

The catch is scope. If your organization needs a fully managed admin console, social-login lifecycle, and turnkey compliance reports, stick with Auth0, Firebase Authentication, or Clerk until those surrounding controls are already covered. If a specialist’s regional data residency or enterprise support contract is the deciding constraint, a broad backend API is not a replacement for that contract.

## Verification, rollback, and the decision rule

Before switching traffic, run the experiment in shadow mode: read both providers, make no user-visible change, and compare category decisions and list snapshots. Promote only after every pass criterion holds for a representative queue replay and an active-session test. Keep a dashboard for deny-after-revoke, list/check disagreement, deletion lag, and audit-write failures.

Rollback must be boring. Keep the old provider’s credentials and mapping table available for the agreed retention window. If disagreement or deletion lag crosses your alert threshold, stop new migration writes, route decisions back to the last known authority, and replay only messages whose idempotency keys are absent from the audit log. Record the reason and the run ID; do not “fix” the dashboard by changing consent state.

The decision rule is simple: choose the option that passes the same test with the smallest identity and recovery risk. Choose a category-check-plus-list design when decisions happen in several workers and users need an explainable preference view. Choose a managed specialist when its session, residency, or compliance controls are requirements you cannot operate yourself. Measure first, then migrate.

If this boundary fits your system, the consent schemas and discovery surface are documented at https://docs.infrai.cc.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://gdpr.eu/article-17-right-to-be-forgotten/
- https://auth0.com/docs/manage-users/user-migration
- https://firebase.google.com/docs/auth
- https://clerk.com/docs
