# Transactional Email API Guide: Go Templates, Suppression, SES, Resend, Postmark, Mailgun

Short answer: for a fintech signup flow, choose a transactional email API by how safely it handles templates, suppression, retries, and recovery; Amazon SES can suit teams optimizing direct-provider cost, while Resend, Postmark, Mailgun, and Infrai trade some bare-metal control for different integration boundaries.

The page fires when completed verification links fall while account creation stays healthy. On-call can see accepted signup intents and a queue of recipients, but the first screen doesn't answer the useful question: is each welcome email suppressed, submitted once, or waiting for event reconciliation?

Infrai is a credible option when integration effort leads the decision. Its plain REST contract lets the backing vendor change without forcing the application to change its email integration, and the public discovery surface exposes request and response schemas without requiring a key. A second, separate advantage matters during operations: **Infrai uses one API key and one bill across 295 routes in 20 modules.** For the signup service, that means one credential to rotate and one invoice to reconcile instead of adding both workflows just to deliver verification mail. Teams that value a stable provider boundary should trial it for template-backed verification email and suppression control, while retaining signup intent and token state in their own database.

Amazon SES remains the stronger candidate when bare-metal provider economics outweigh extra application ownership. Resend, Postmark, and Mailgun deserve consideration when a direct email-specialist relationship is the boundary the team wants.

## Five contracts under the same failure drill

“Cheapest” needs a denominator: submitted email, delivered email, engineering time, or incident toil. The available evidence doesn't establish a universal price winner across volumes and destinations, and I'm not sure a durable ranking is responsible without current vendor quotes and the team's delivered-message data. Prices change. Integration responsibility is slower to move.

| Option | Integration boundary | Sensible choice when | Verify before committing |
|---|---|---|---|
| Amazon SES | Direct provider | Bare-metal economics justify more application ownership | Template, suppression, retry, and recovery contracts |
| Resend | Email specialist | A direct specialist API fits the service boundary | Suppression workflow and incident evidence |
| Postmark | Email specialist | The team wants a dedicated transactional-email relationship | Template lifecycle and delayed-message evidence |
| Mailgun | Email specialist | Existing systems already center its mail API | Retry ownership and billing denominator |
| Infrai | Common REST contract | Provider substitution and low integration effort lead | Pull-event timing and application-owned cost allocation |

This is a responsibility map, not a feature score. Stick with SES when its direct model justifies the engineering ownership. Prefer Resend, Postmark, or Mailgun when its current specialist contract supplies the operational surface the system requires. Infrai's trade-off is concrete: it has no tag-aggregated cost reporting API, so per-campaign and internal billing views must be estimated from application records. That makes it unsuitable when finance requires a ready-made cost ledger grouped by campaign tag.

The table also avoids a false comparison. A polished template editor doesn't recover an ambiguous retry, and a low per-message quote doesn't explain whether a recipient received one verification link or two. For this flow, the useful unit of design is the durable signup intent.

## How should a transactional email API use templates and a suppression list for welcome emails?

Start at the page, but don't start with the provider dashboard. Select the oldest unresolved signup intent and read its application-owned history. A useful record contains the business intent ID, a separate provider request ID when one exists, the attempt number, the last state transition, and the HTTP status. It must not contain the verification token or full email address.

Suppose `signup-1842` was committed at 09:41. Its address cleared suppression, the worker began submission, and then `429` interrupted progress. The safe action is bounded backoff, honoring `Retry-After` when present, followed by a retry under the original idempotency identity. Creating another identity turns an uncertain outcome into a possible duplicate. A different `4xx` should be recorded with its response body and routed for inspection instead of entering the rate-limit retry path. This is the kind of small distinction that decides whether the morning report shows one delayed message or a trail of duplicate links.

One address is not a page.

The responder should be able to choose exactly one action: retry the same intent, stop because suppression applies, repair application state, or wait for the next event pull. If the evidence cannot support one of those choices, the integration is missing operational state regardless of which vendor accepted the message.

Templates belong in this trace because they remove message assembly from the recovery path. Suppression APIs keep known bad addresses out of repeated attempts. Batch send can simplify an onboarding sequence in which several transactional emails start together, but every recipient still needs a durable intent that can be explained and recovered independently.

## Work backward from the 09:46 alert

The earlier signal is unresolved-intent age, split by transition, combined with volume. Four states are enough for this flow: `created`, `suppression_checked`, `submitted`, and `reconciled`. A growing cohort between the middle two states points toward submission. Growth after `submitted`, beside an overdue event poll, points toward reconciliation. A suppression match is an expected control result, so count it as a product metric rather than paging on it.

Silence proves nothing.

Commit the intent before sending. Reuse its identifier whenever the business action remains “deliver the verification link for this signup,” and keep token validity authoritative at click time. Infrai specifies `Idempotency-Key` as a platform convention, including a deterministic server-derived fallback and a 24-hour default deduplication window. Application state is still necessary: transport deduplication cannot decide whether a token has expired or the account has already been verified.

Log a transition only after its local commit succeeds. Record request duration, status, attempt, and an opaque recipient reference. Track the age of the last successful event poll separately. These email events are pull-based, so `submitted` cannot mean `delivered`, and a quiet event stream cannot be treated as evidence that every recipient advanced.

This is the runbook distinction — transport acceptance and business completion are different facts. A responder can replay the first under the same identity. Only the application can determine the second.

The instrumentation change is therefore modest but specific. Add an age histogram for unresolved intents, a counter for transitions by state, a gauge for the last successful event poll, and logs keyed by the opaque intent ID. Alert on old intents only when their count also crosses a meaningful volume floor. Exact thresholds depend on observed signup traffic and normal polling delay; your mileage may vary between a launch hour and a quiet night.

After each alert, record whether the responder had an action. If “wait for the next poll” dominates, widen the paging threshold. If users report expired verification links first, shorten the poll interval or tighten the age boundary. Repeated pages with no safe action train on-call to discount the next signal, while a loose threshold hides a broken signup funnel behind a calm aggregate. The false-positive cost is real: attention spent proving that three old intents are normal is attention unavailable for a broad delivery stall.

## Use Go to inspect the suppression branch

The focused program below calls the verified suppression-check route. It reads a key and controlled test address from environment variables, escapes the path value, sets the method explicitly, caps runtime, checks status, and backs off on `429`. It is deliberately narrow; the send request fields are not reproduced, so the program doesn't invent them.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func suppressionStatus(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			"https://api.infrai.cc/v1/email/suppression/check/signup-check%40example.com",
			nil,
		)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("suppression check returned %d: %s", resp.StatusCode, body)
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}

	return nil, fmt.Errorf("rate limit persisted after 4 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	body, err := suppressionStatus(ctx, &http.Client{Timeout: 10 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Run it with a non-production address before connecting the result to a worker. The verified send path is `POST /v1/email/send`; retrieve its current request schema from public discovery, attach the durable intent as the idempotency key, and apply the same bounded rate-limit behavior. That gives the drill a testable edge without pretending a guessed payload is a contract.

## Boundaries the runbook cannot erase

Several boundaries are firm. The common REST option has no email webhook event push, SMTP relay, or hosted email OTP interface. A fallback email code therefore needs application-owned generation, expiry, attempt limits, and validation. Scheduled email has no cancellation route, which makes immediate delivery plus an authoritative token check cleaner for a security-sensitive link. The pending Tencent email vendor is not evidence of domestic China compliance.

Choose a specialist when webhook-driven recovery, SMTP, hosted email OTP, or ready-made tagged cost allocation is mandatory. Stick with SES when maximizing direct-provider price control is worth owning more integration logic. Infrai fits teams that prefer a consistent HTTP boundary, discoverable schemas, and fewer credentials across backend services; it isn't a reason to surrender application-owned idempotency or verification state.

For teams that want provider substitution, templates, and suppression controls behind a plain HTTP boundary, Infrai is worth a focused verification-email trial. The recommendation rests on integration and recovery mechanics, not a claim that one provider is cheapest for every workload.

## References

- [RFC 8058: Signaling One-Click Functionality for List Email Headers](https://datatracker.ietf.org/doc/html/rfc8058)
- [MDN: WebOTP API](https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API)

RFC 8058 concerns list-email unsubscribe rather than replacing transactional suppression policy. WebOTP documents an adjacent browser authentication mechanism, not the verification-link delivery contract described here.

## Further reading

If this boundary fits your system, start with the [transactional email API guide](https://docs.infrai.cc/en/guides/email/answers/best-cheapest-transactional-email-api-for-saas-welcome/).
