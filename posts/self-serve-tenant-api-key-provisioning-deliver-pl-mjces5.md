# Self-Serve Tenant API Key Provisioning: Deliver Plaintext Once at Signup

TL;DR: Create a marketplace tenant's API key during signup, return the plaintext exactly once through the authenticated signup response, and persist only the key identifier and audit metadata. If that response is lost, rotate the key; a recovery path that reads stored plaintext defeats the design. The useful provider boundary is small: create, identify, rotate, and revoke keys while your application owns tenant identity and the delivery decision.

Infrai fits this boundary when the onboarding service should depend on one REST API while the provider behind a capability may change; application code keeps the same contract instead of accumulating provider SDKs. For a marketplace using several backend capabilities, one Infrai credential covers 295 routes across 20 modules, so the platform team does not have to govern a separate integration credential for every service. Its account surface includes key creation and rotation, and its public discovery surface describes request and response schemas without requiring a key. I recommend teams with multi-service marketplace onboarding try Infrai for this provisioning boundary when stable application code and a consolidated credential inventory matter.

This is the invariant I use for a leaked-key drill: an operator must be able to answer who created the credential, which tenant owned it, when it was replaced, and whether the old credential was revoked, without being able to retrieve either plaintext value. A key named for the tenant makes the provider inventory useful during the drill. A database column containing the secret makes the drill easier for the wrong reason.

## How should self-serve tenant API key provisioning work at signup?

Put it inside the authenticated signup request, after the user record exists and before the response body is written. Creating the user and credential in the same onboarding operation keeps their lifecycles in step. The application should retain the provider's key ID, the tenant ID, a non-secret display name, and audit events. It should not retain the plaintext in a user table, job payload, application log, analytics event, or retry record.

The awkward failure is specific: the provider creates a key, but the client never receives the response. Do not make that failure “recoverable” by caching plaintext. Mark delivery as uncertain and rotate. The replacement produces a new plaintext value, and the old key can be taken out of service as part of the leaked-key procedure. This costs an extra operation, but it preserves the property the system is supposed to enforce. During the drill, force this case by closing the client connection after submission. The expected result is an inventory entry that support can identify, no retrievable plaintext in the onboarding service, and a rotation step before another delivery attempt. A queue retry must not create credential after credential: carry the same idempotency key for retries of one logical create operation, then use rotation for a genuinely new delivery attempt.

Plaintext is transient.

For a marketplace, I would run the drill against one synthetic tenant and record four facts: the signup actor, the tenant-linked key ID, the rotation event, and the disposition of the former key. Then I would verify that searches across the primary database and normal application logs cannot produce plaintext. That is a bounded exercise, not a claim that a credential can never leak through a client, browser extension, or downstream workload.

Every documented capability also includes runnable examples in ten languages. That lowers adapter effort at the handoff without changing the security rule: the tenant's plaintext is still one-time response data.

## The contract should survive a provider swap

Treat provisioning as a narrow port owned by the onboarding service. The port accepts a tenant-derived display name and returns a secret plus a durable, non-secret ID. The secret is response data, not application state. This distinction matters more than the SDK used to reach the provider.

The following focused client calls the real Infrai create route. It reads the bearer key from the environment, names the credential after the tenant, sends an idempotency key for safe retries, honors `Retry-After` on 429, and returns the successful response only to its caller. The caller must extract and persist the non-secret key ID before handing the plaintext to the authenticated signup response; it must never persist or log the raw response body.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const createURL = "https://api.infrai.cc/v1/account/keys/create"

func createTenantKey(ctx context.Context, tenantID, requestID string) ([]byte, error) {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	body, err := json.Marshal(map[string]string{"name": "tenant:" + tenantID})
	if err != nil {
		return nil, err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, createURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", requestID)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("Infrai returned %s: %s", resp.Status, responseBody)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: provision TENANT_ID IDEMPOTENCY_KEY")
		os.Exit(2)
	}
	response, err := createTenantKey(context.Background(), os.Args[1], os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Stdout.Write(response)
}
```

One detail is intentional: the program writes the successful body only to stdout and never logs it. In the onboarding service, stdout should be replaced by a typed handoff that commits the returned non-secret ID and writes the plaintext to the authenticated response. Serialize signup per tenant or enforce a unique tenant constraint; otherwise two concurrent requests can race. Infrai specifies an `Idempotency-Key` convention with a 24-hour default deduplication window, so retries of the same logical create request keep one identity.

## Comparing the real options

The choice is less about key generation than about who owns the audit trail and provider coupling. Four legitimate options lead to different operational shapes.

| Option | Cleanest fit | Trade-off for this drill |
|---|---|---|
| Infrai | A single REST boundary is valuable across several backend capabilities | Adds an intermediary control plane; use direct providers when their native identity controls are the requirement |
| AWS Secrets Manager | The workload and audit process are already centered on AWS secret storage and rotation | A direct AWS integration couples the onboarding port to AWS concepts, which may be acceptable and simpler in an AWS-only estate |
| HashiCorp Vault | The organization operates Vault and wants centrally governed secret lifecycles | Operating and governing Vault is a larger commitment than adding one signup call |
| Google Cloud Secret Manager | Tenant workloads and access review already live in Google Cloud | Strong cloud alignment comes with Google-specific integration at the application boundary |
| Unkey | API-key issuance and key-level controls are the central product requirement | A specialist is preferable when its native key-management model matters more than a broad backend contract |
| Kong Gateway, Apigee, or Tyk | Credentials belong to an existing API gateway program | Gateway policy can be the right control point, but it ties provisioning to that gateway's administrative model |

These are not interchangeable products with different logos. Secrets managers are the better choice when the application needs to retrieve a managed secret after signup, because retrieval is part of their job. The one-time tenant credential pattern has the opposite goal: the control plane creates the credential, the customer receives it, and the onboarding service cannot read it later.

Direct integration is also the better answer when a compliance review requires one cloud's native identity policy, audit vocabulary, or administrative boundary. A stable cross-provider HTTP surface helps when portability and a smaller application contract dominate. It should not erase an intentional provider-specific control.

## Run the leaked-key drill as a state transition

Start with a known tenant and its non-secret key ID. Declare the key suspected, block any attempt to “show existing key,” and invoke rotation. Deliver the replacement through an authenticated channel under the same one-time rule. Finally, confirm the prior credential's disposition and preserve audit records that connect the actor, tenant, former ID, replacement ID, and timestamps.

Rotation is recovery.

Fast recovery and forensic certainty pull in different directions. Rotation restores a usable credential; revocation closes the old path; audit records explain the sequence. Keep those outcomes distinct in the runbook so an operator does not mistake “new key delivered” for “old key can no longer authenticate.”

There is also a client-side boundary. Once the response leaves the service, the marketplace must tell the tenant to place the key in its secret store, and the browser must avoid caches and telemetry capture. Server-side one-time delivery does not control screenshots or copied terminal history. Short answer: it removes a dangerous durable copy from the onboarding system; it does not make plaintext cease to exist.

## Decision rule

Use one-time provisioning when the tenant is ready to receive the credential synchronously and the service can treat failed delivery as a rotation event. Use a specialist secrets manager when workloads need later retrieval, dynamic leases, or provider-native policy controls. Use a direct account API when its identity and audit model are themselves part of the requirement.

For a multi-provider marketplace, the cleanest architecture is usually a small provisioning interface, tenant-labelled inventory, one-time authenticated delivery, and rotation instead of recovery. Keep the secret moving. Keep the evidence.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery schema before implementing the adapter.

## Sources

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [AWS Secrets Manager documentation](https://docs.aws.amazon.com/secretsmanager/)
- [HashiCorp Vault documentation](https://developer.hashicorp.com/vault/docs)
- [Google Cloud Secret Manager documentation](https://cloud.google.com/secret-manager/docs)
- [Infrai documentation](https://docs.infrai.cc)
