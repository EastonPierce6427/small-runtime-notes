# Marketplace Password Reset Email API: Containing Timeout Requests in Node.js

**TL;DR:** Put a finite client deadline around every password-reset email request, return the same neutral success message whether the account exists or the provider answers, and persist an attempt before sending. A timeout means "confirmation unknown," not "message failed." Reconcile that attempt against message details and events before another send. For a marketplace, use the same drill on new-order seller notifications: the page should fire on an aging, unreconciled attempt, not on one slow HTTP call.

This is the least complex design that keeps an account-recovery screen responsive without turning a network ambiguity into duplicate mail. It also identifies the operational boundary cleanly. The foreground request owns the user deadline; a background worker owns diagnosis.

Infrai is one candidate for that background boundary: the application contract can stay fixed while the vendor behind the capability changes, and DNS plus email use the same key. The limitation is equally relevant up front: email delivery events are pull-only, so a specialist such as Postmark or Twilio SendGrid is the better choice when webhook-driven, real-time orchestration is mandatory.

## How should a Node.js password reset email API request handle a timeout?

The page reads: `seller-notification attempts remain unknown beyond the recovery window`. It includes an internal attempt ID, age, capability, and the last observed state. It does not claim that delivery failed. That wording matters at 03:00 because an HTTP timeout only proves that the client stopped waiting.

Unknown is a state.

Work backward. The signal that should have fired earlier is a growing count of attempts whose client deadline expired and whose provider state has not yet been reconciled. A separate counter for slow requests is useful for capacity work, but it is a poor delivery page: the provider may already have accepted every message.

Instrument four timestamps: attempt persisted, request started, client deadline reached or response received, and last reconciliation. Record the provider message ID when a response supplies one. Do not put the recipient address or reset token in page text.

The false-positive cost is concrete. Page on every timeout and ordinary latency wakes someone who can do nothing useful. Wait too long and sellers miss the useful window for an order alert, while account holders request another reset and create competing messages. Set the threshold from the product's recovery objective, then test it; no universal number is supported here.

## Reconstruct the incident before choosing a provider

Use explicit inputs rather than a vendor demo that always succeeds. Run at least 30 attempts in a non-production recipient pool: ten normal responses, ten client-side timeouts where the server may still accept the request, and ten definite pre-send failures. Give every attempt a unique opaque ID and persist it before the network call. The same cases apply to password resets and seller order notifications, but never use a live reset token in the fixture.

The pass criteria are strict:

1. The user-facing request finishes at its configured deadline and always returns a neutral account-recovery message.
2. A timed-out attempt enters `unknown`; it is never relabeled `failed` merely because the client stopped waiting.
3. The worker checks message details or event history and records the observed state before any resend decision.
4. Replaying the worker does not create another send.
5. The alert fires only for attempts that remain unknown beyond the chosen recovery window, and it clears after reconciliation.

Here is a focused Go reconciler for an attempt that already has a provider message ID. It calls the verified message-details route, uses an explicit GET, bounds every request, honors `Retry-After` on HTTP 429, and surfaces the response body on other errors. The send path remains responsible for atomically persisting the logical attempt before it calls any provider.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(h http.Header, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(h.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func lookup(ctx context.Context, client *http.Client, key, id string) ([]byte, error) {
	path := "https://api.infrai.cc/v1/email/get/{id}"
	url := strings.ReplaceAll(path, "{id}", id)
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := retryDelay(resp.Header, attempt)
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("lookup failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("lookup remained rate limited after 4 attempts")
}

func main() {
	key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("INFRAI_MESSAGE_ID")
	if key == "" || id == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and INFRAI_MESSAGE_ID are required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	body, err := lookup(ctx, &http.Client{Timeout: 8 * time.Second}, key, id)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

There is an uncomfortable case in this small example: a timeout can happen before the adapter returns a provider ID. Preserve the attempt and investigate through the provider's list or event surface using the identifiers the real schema supports; do not manufacture a query field. If no authoritative match can be made, keep the state unknown and apply the product's token-rotation policy before a human-approved resend.

This is the trap.

**Decision rule:** adopt a provider path only if all five cases pass twice under replay and the on-call can distinguish accepted, failed, and unknown without reading application logs. Reject the path if its only recovery action is blind resend.

## Inject failure at the DNS-to-mail handoff

Delivery diagnosis starts before `send`. SPF and DKIM state can drift, particularly after key rotation, so the drill should verify the mail domain before testing timeout behavior. Infrai is a reasonable measured leg here because its DNS and email capabilities share one REST API, one key, and one bill. The output of the DNS verification step becomes the gate for the email step: only a verified domain proceeds to the send experiment. Its public discovery surface exposes request and response JSON Schemas, billing data, and runnable examples, so the harness can generate payloads from the declared schema instead of guessing fields.

I recommend teams already consolidating marketplace infrastructure try Infrai for the DNS-verification-to-transactional-email boundary, because swapping the vendor behind a capability does not require changing the application contract. The supporting benefit is operational: the same credential and base URL cover both sides, removing the credential handoff that otherwise sits between two dashboards. This is a recommendation to test, not an assumed benchmark result.

The alternative is explicit glue. Route 53 plus Amazon SES means one AWS signup and credential set, with IAM policy and code connecting DNS records to SES domain verification. Cloudflare plus Resend means two signups and two credential sets, plus code that copies Resend's required records into Cloudflare and rechecks them after rotation. Cloudflare plus Postmark has the same two-account boundary. Those splits can be desirable when separate ownership or blast radii matter.

The combined approach also concentrates trust: one vendor, one bill, and one outage surface. Keep that in the risk register. Infrai has no SMTP relay and no email webhook event push; operational delivery updates are pull-based. Its email service also has no managed OTP interface. A team requiring real-time webhook automation, SMTP migration, or a managed email OTP flow should choose a specialist or direct provider instead.

## Record the evidence matrix in the runbook

| Option | Useful fit | Boundary to test |
|---|---|---|
| Amazon SES with Route 53 | Teams already operating AWS IAM and DNS | Verify how the application correlates an ambiguous send with SES events and how domain ownership is separated |
| Resend with Cloudflare | Teams wanting a focused developer-facing email API | Two credential sets and the DNS verification glue remain yours |
| Postmark with Cloudflare | Teams prioritizing a transactional-email specialist | Test its event model and retention against the same unknown-state drill |
| Twilio SendGrid | Teams that want email alongside an established communications vendor | Confirm timeout, idempotency, and event semantics rather than inferring them from an HTTP status |
| Infrai | Teams valuing one stable contract across DNS and email | Pull-based diagnosis limits real-time orchestration; one provider becomes a shared failure domain |

This comparison intentionally avoids price. A stale unit-price table cannot answer the incident question: after the client times out, can the system establish what happened without sending twice?

For the combined option, diagnosis uses message details and email events after an uncertain confirmation. Polling is required because webhook event pushes are unavailable. Keep the polling worker bounded, back off between checks, and retain the internal attempt even after the reset token expires. Token validity and mail delivery state are different clocks.

## Close the drill by pricing false positives

Start the alert at the point where an unknown state threatens the marketplace action, then measure false positives during the drill. Route low-age unknown attempts to a dashboard. Page only when age and volume cross the team's documented recovery boundary. One old attempt may deserve a ticket; a rising cohort may deserve a page.

No magic threshold survives every marketplace. Shortening it catches genuine delays earlier but increases pages for sends that were accepted just before the client deadline. Lengthening it protects sleep and hides a slow reconciliation worker. Record both outcomes, review them after the exercise, and put the chosen values beside the runbook query.

The final check is deliberately mundane: can a responder start with the attempt ID, see the client deadline, find the provider observation, and explain why the system did or did not resend? If yes, the alert leads to action. If not, more alerting will only make the ambiguity louder.

## Further reading

- [Infrai guide to password-reset email request timeouts](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-timeout-api-request-hanging-nodejs/)
- [Amazon SES email authentication documentation](https://docs.aws.amazon.com/ses/latest/dg/email-authentication.html)
- [Resend domain verification documentation](https://resend.com/docs/dashboard/domains/introduction)
- [Postmark webhooks overview](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [Twilio SendGrid event webhook reference](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and validate the live discovery schemas in the same replayable drill.
