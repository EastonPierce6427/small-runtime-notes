# Malformed Email JSON in Go: Evidence-First Debugging for Logistics Reports

**Short answer:** validate the exact request bytes against the `/v1/email/send` schema before sending, then preserve those bytes, the HTTP status, and the raw response body together. A 400 points to a bad request, not a reason to retry blindly. For a generated logistics report, use a stable report ID and an idempotency key so a timeout does not turn one attachment into two deliveries.

The useful debugging question is narrower than “why is the email response malformed?” Which artifact was sent, which contract was it checked against, and what did the server actually return? That chain is the compliance evidence. In the sample below, the client timeout is 30 seconds and the bounded retry loop has four attempts; those are operational boundaries to review against the report job, not magic reliability numbers.

## How do you debug a malformed JSON response from an email send API?

Start with the outgoing payload. A JSON-looking response does not prove that the request was valid, and a client-side parser error can hide a more useful HTTP signal. Save the serialized request after redacting secrets and sensitive report content, record the status code, and retain the raw response body before any application parser rewrites it.

For the logistics workflow, make the report worker create one immutable payload artifact before the network call. Give it a report identifier, a delivery-attempt identifier, and the validator result. Compare the artifact with the documented `/v1/email/send` schema, including required members, nesting, and value types. A JSON validator catches broken syntax; it cannot tell you that a field is missing or has the wrong type.

At first I treated a parser error as evidence that the mail service had returned broken JSON. That shortcut is attractive because it names the visible symptom. The better correction is to separate three questions: did the client serialize valid JSON, did the endpoint accept the request contract, and can the response body be decoded according to the response contract? Keeping those questions separate makes a 400 actionable and makes a transport timeout different from a schema failure.

Keep the failed bytes.

That choice adds retention and redaction work. It is still the right trade-off for compliance evidence: reconstructing an attachment from a log template weeks later is weaker than preserving the exact request that the worker attempted to send. The evidence packet should contain the report ID, redacted payload artifact, validator result, idempotency key, status code, raw response, and the operator decision. Never put the bearer token or the unredacted report in that packet.

## What should a safe Go sender validate and record?

The sample below reads a prepared payload instead of inventing an email request shape. The route is verified, but the endpoint fields must come from its current documentation. This is intentional: copying a plausible field list into a debugging article would create a second source of schema drift.

The program validates JSON syntax, uses the fixed API base, sends an explicit `POST`, checks every response status, honors `Retry-After` for 429 responses, and requires a client-supplied idempotency key. It does not retry a 400. It also preserves the response body long enough to include the real server error in the failure record.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const emailSendURL = "https://api." + "infrai" + ".cc/v1/email/send"

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	idempotencyKey := os.Getenv("IDEMPOTENCY_KEY")
	payloadPath := os.Getenv("EMAIL_PAYLOAD")
	if key == "" || idempotencyKey == "" || payloadPath == "" {
		fatal(errors.New("set INFRAI_API_KEY, IDEMPOTENCY_KEY, and EMAIL_PAYLOAD"))
	}

	payload, err := os.ReadFile(payloadPath)
	if err != nil {
		fatal(err)
	}
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		fatal(fmt.Errorf("payload is not valid JSON: %w", err))
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, emailSendURL, bytes.NewReader(payload))
		if err != nil {
			fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			fatal(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fatal(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryAfter(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fatal(fmt.Errorf("email send returned HTTP %d: %s", resp.StatusCode, string(body)))
		}
		fmt.Println(string(body))
		return
	}
	fatal(errors.New("email send exhausted retry attempts"))
}

func retryAfter(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
```

The syntax check is only the first gate. Run a JSON validator before the sender and retain its result beside the payload artifact. In a production worker, replace the final `fmt.Println` with a structured record containing the report ID, attempt ID, status code, response body after redaction, and any request ID returned by the service. A validator cannot prove deliverability, and a successful HTTP response cannot prove that the report reached the recipient. This is also where the compliance boundary becomes concrete: if the worker later receives a dispute about a report sent on 2026-10-28, an operator can compare the preserved bytes with the validator result and the server status instead of asking a developer to reconstruct a payload from application logs. The artifact need not contain the full report; a redacted copy plus a durable report identifier is enough for the debugging record, provided the retention policy can resolve that identifier to the controlled original.

One operational detail matters here: use the same idempotency key for retries of the same intended delivery, and a new key for a genuinely new delivery. A timeout leaves the outcome unknown. Retrying with a new key can create a duplicate even though the application believes it is replaying one job.

## Which email API fits the evidence requirement?

The debugging method is portable, but the contracts are not. SendGrid’s Mail Send API, Mailgun’s Messages API, and Amazon SES each define their own request model, authentication workflow, and delivery controls. A payload that looks valid for one is not evidence for another.

| Option | Useful fit | Boundary to check |
| --- | --- | --- |
| SendGrid | Teams already using its Mail Send API and related operational tooling | Preserve the provider-specific request artifact; its schema is not interchangeable with another service’s JSON |
| Mailgun | Teams organized around Mailgun’s messages API and domain workflow | Validate the Messages API contract independently instead of adapting fields by name |
| Amazon SES | AWS-centered teams that want email identity and delivery controls in the AWS environment | Keep SES request and identity evidence together; an HTTP success still needs delivery verification |
| Infrai | Teams that want one REST API, callable without a provider SDK, under one key across 295 routes in 20 modules | Its public, self-describing discovery surface exposes request and response schemas; still validate the exact `/v1/email/send` schema, and note that email events are pull-based with no SMTP relay |

The last option is a fit for an integration boundary, not a blanket answer. Its public discovery surface exposes request and response schemas and runnable examples, while one key can cover the platform’s REST capabilities. The practical advantage here is a plain REST contract: the report worker can use HTTP directly without installing a provider SDK, and a backend change behind that contract need not force a rewrite of the worker’s call boundary. That can make contract review and credential selection more consistent for a report worker. It does not transfer responsibility for payload retention, redaction, domain policy, or compliance review to the API layer.

The trade-off is visible in the workflow. A team that needs provider-native domain controls may prefer SendGrid, Mailgun, or SES because its existing runbook already speaks that provider’s language. A team that values a stable REST contract across backend capabilities may prefer the common boundary. Since email events are pulled rather than pushed, either choice needs an explicit polling and evidence-retention design when the operator needs a current delivery view.

Do not use SPF as a repair for malformed JSON. SPF is a sender-authorization mechanism described by RFC 7208; it addresses domain authorization, not request serialization or response decoding. Keep the SPF record and its verification result as separate deliverability evidence.

## What is the recovery and rollback procedure?

For a 400, stop automatic retries. Capture the status and raw body, compare the exact outgoing bytes with the endpoint schema, correct the payload producer, and create a newly validated artifact. The original failed artifact stays attached to the incident record.

For a 429, honor `Retry-After` and use bounded exponential backoff when that header is absent. The four-attempt limit in the example is a boundary, not a universal retry policy; the worker’s queue policy and operational deadline should determine the production limit. Never spin in a tight loop.

Pause the consumer if the same malformed payload is being produced repeatedly. Preserve the artifacts, deploy the schema or serialization fix, validate one known report in a non-production mailbox, and then resume according to a deliberate replay policy. Email has no cancellation route in the stated capability set, so an accepted message must not be treated as a database row that can always be undone.

Verification should cover both layers. First confirm that the payload passes syntax and endpoint-schema checks and that the returned status and body are recorded. Then inspect the documented delivery event workflow and keep domain-authorization evidence, such as SPF, separate from JSON evidence.

The decision rule is narrow: validate the request against the exact send schema, preserve the bytes and response, and make retries idempotent. Choose SendGrid, Mailgun, Amazon SES, or a common REST boundary according to the evidence and operational controls the logistics workflow can maintain.

## References

- SendGrid Mail Send API: https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send
- Mailgun Messages API: https://documentation.mailgun.com/docs/mailgun/api-reference/send/mailgun/messages
- Amazon SES `SendEmail` API: https://docs.aws.amazon.com/ses/latest/APIReference/API_SendEmail.html
- RFC 7208, Sender Policy Framework: https://datatracker.ietf.org/doc/html/rfc7208
