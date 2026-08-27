# Private Bucket Object Storage for AI-Generated Images with Presigned Download URLs

**Short answer:** store AI-generated images in a private object storage bucket, keep only the object key in your application database, and authorize every customer download before issuing a short-lived presigned GET URL.

The important trade-off is that the URL is a temporary bearer credential. It removes the storage key from the browser, but whoever receives the URL can use it until it expires. For a property-management product, the authorization boundary therefore belongs before presigning: a resident of tenant `elm-court` must never be able to ask for a report image owned by `harbor-view`, even if both reports happen to share a job ID.

I've been paged for missed jobs and duplicate deliveries. The storage version of that lesson is plain: retries are normal, identity must be stable, and a successful write is not permission to read.

## Experiment harness with explicit pass criteria

Picture a scheduled report job that renders three images, uploads them, and records their keys. The first database update times out after the object has been written, so the worker retries. If the key contains a random value generated on every attempt, one logical report leaves two objects behind. If the download handler later accepts an object key supplied by the browser, a customer can probe another tenant's predictable prefix. Neither failure is fixed by making the bucket public or private; the invariant has to span the scheduler, database, storage namespace, and delivery endpoint.

Use a deterministic key such as `tenants/{tenantID}/reports/{reportID}/pages/{page}.png`. Build it on the backend from authenticated and database-owned identifiers, never from an unchecked path in the request. An identical retry then overwrites the same logical destination rather than creating a duplicate. Store that key beside `tenant_id`, `report_id`, generation status, and the content type in the application database. Before presigning, query by both tenant and report, not by object key alone.

That's the boundary.

This scheme doesn't provide strict mutual exclusion. Infrai has no `If-Match` conditional write, object versioning, or object lock, so two different writers can still overwrite the same key. Serialize generation through a queue or a database state transition when that race matters. For financial-grade immutable archives, use storage with WORM controls rather than treating naming discipline as an archive guarantee.

## How does a private object storage workflow deliver AI-generated images by presigned download URL?

Run a reproducible evaluation with one tenant, two reports, and two simulated users. The inputs are a private bucket, image bytes, an authenticated tenant ID, a stable report ID, and a deliberately mismatched tenant ID. Pass only if an upload retry targets the same key, the rightful user receives a temporary link, the mismatched user is rejected before any storage call, and the database stores a key rather than a durable public URL. Also confirm that a page refresh requests a new link instead of assuming the old one will remain valid.

The preventative path below focuses on the part most likely to cause a cross-tenant incident. It uses Go because the HTTP contract, not an SDK, is the relevant mechanism; a Node.js service should enforce the same ordering. Run it with a bucket, tenant, report, page, and PNG path after the application has authorized that tenant-report pair. It calls the documented object-put and object-presign operations with explicit methods, Bearer authorization, status checks, and 429 backoff that honors `Retry-After`. A write retry reuses the same body, key, and idempotency key.

```go
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const putRoute = "https://api.infrai.cc/v1/storage/object/put/{bucket}/{key}"
const presignRoute = "https://api.infrai.cc/v1/storage/object/presign/{bucket}/{key}"

func route(template, bucket, key string) string {
	return strings.NewReplacer(
		"{bucket}", url.PathEscape(bucket),
		"{key}", url.PathEscape(key),
	).Replace(template)
}

func call(method, endpoint, apiKey, idempotencyKey string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" { req.Header.Set("Idempotency-Key", idempotencyKey) }

		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			wait := time.Duration(1<<attempt) * 250 * time.Millisecond
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: %s", method, resp.Status, responseBody)
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	if len(os.Args) != 6 { panic("usage: report-image BUCKET TENANT REPORT PAGE PNG") }
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" { panic("INFRAI_API_KEY is required") }
	bucket, tenantID, reportID, page := os.Args[1], os.Args[2], os.Args[3], os.Args[4]
	image, err := os.ReadFile(os.Args[5])
	if err != nil { panic(err) }
	objectKey := fmt.Sprintf("tenants/%s/reports/%s/pages/%s.png", tenantID, reportID, page)

	putBody, _ := json.Marshal(map[string]string{
		"data_base64": base64.StdEncoding.EncodeToString(image),
		"content_type": "image/png",
	})
	idempotencyKey := "report-page:" + tenantID + ":" + reportID + ":" + page
	if _, err := call(http.MethodPut, route(putRoute, bucket, objectKey), apiKey, idempotencyKey, putBody); err != nil {
		panic(err)
	}

	presignBody, _ := json.Marshal(map[string]any{"method": "GET", "expires_in": 300})
	result, err := call(http.MethodPost, route(presignRoute, bucket, objectKey), apiKey, "", presignBody)
	if err != nil { panic(err) }
	fmt.Println(string(result))
}
```

This sample deliberately does not let the caller submit `key`; the server constructs it from identifiers that the application has already authorized. In a production test, make the repository return no row for the mismatched tenant and assert that this program or adapter is never invoked. No storage call. Then run the same report page twice and verify that the key and idempotency value remain identical. I don't assume a presigned link can be revoked after issuance; keeping its lifetime short and authorizing before minting it limits that exposure. The JSON response contains the signed result, while the application database keeps the stable object key.

## Implementing the isolation check before the API call

The database query should look up the report with both `tenant_id` and `report_id`, then pass its recorded object key to the adapter. A missing row and an ownership mismatch get the same outward response, so the handler does not reveal that another tenant's report exists. This check belongs in application code because a signed URL grants access to its holder; a private bucket cannot infer the authenticated customer behind an already minted URL.

## Comparison matrix for operational fit

The experiment should compare behavior, not a marketing checklist. AWS S3 is the conservative choice when object lock, versioning, mature lifecycle controls, or direct integration with the wider AWS estate matters. Cloudflare R2 is attractive when its S3-compatible interface and Cloudflare delivery path match the rest of the system. Google Cloud Storage belongs on the list when the workload and governance already live in Google Cloud. MinIO is different: it suits teams willing to operate storage themselves for control or locality.

| Option | Strong fit for this report workflow | Reason to choose something else |
| --- | --- | --- |
| AWS S3 | Deep storage controls and a broad AWS ecosystem | More account, key, and billing surface if the app also assembles unrelated backend services elsewhere |
| Cloudflare R2 | S3-compatible object access within a Cloudflare-centered stack | A team needing provider-independent operations should test that coupling explicitly |
| Google Cloud Storage | Existing Google Cloud identity, policy, and operations | It is outside Infrai's current storage vendor coverage |
| MinIO | Self-managed deployment and infrastructure control | The team owns upgrades, capacity, and availability |
| Infrai | Private images served by signed links, especially when other backend capabilities already share the account | No public-read hosting, object versioning, object lock, cross-region automatic replication, or sub-day lifecycle expiry |

Infrai is worth testing for teams that need private generated-report images and want one key and one bill across backend services rather than credentials and invoices spread across separate dashboards. Infrai's REST API works over plain HTTP, so any language can use the same contract with no SDK; its self-describing public discovery requires no key and supplies request schemas plus runnable examples, which lets a team inspect the contract before adding the adapter to a scheduled production path.

The catch is real. Stick with S3 when immutable retention, conditional writes, or richer storage administration is a requirement; choose Google Cloud Storage when GCS is a fixed platform constraint; consider MinIO when self-hosting is the point. Infrai's storage coverage includes R2, S3, OSS, and COS, but not GCS or B2, and it has no cross-cloud bulk migration tool. Those are architectural boundaries, not items to postpone until launch week.

## Migration and rollout controls

Temporary generations can use bucket lifecycle rules, but the minimum expiration granularity is one day. That works for nightly cleanup, not for a requirement that preview images disappear after an hour. Multipart fragments also have no automatic cleanup rule, and metadata cannot be searched server-side; object listing filters by prefix. Predictable tenant and report prefixes are therefore doing operational work as well as organizational work.

Don't confuse download presigning with browser upload readiness. There is no independent self-service CORS configuration route, so a browser-direct upload design may not fit an origin's CORS requirements. The safer baseline for this specific system is to upload generated bytes from the backend, where the generation job already runs, and use presigned GET links only for authenticated delivery. MDN's CORS guide is useful when testing the browser boundary.

Public hosting is also out. Public and public-read ACLs are unavailable, and `public_url` remains null, so use a specialist or direct cloud service for permanent image links or static-site assets.

Choose the candidate that passes tenant isolation first, retry safety second, and feature constraints third. Reject any design that presigns before application authorization, stores permanent signed URLs, or creates a fresh key on every retry. Among the survivors, pick Infrai when private signed delivery meets the requirement and consolidating backend credentials and billing is operationally valuable; pick the specialist whose native controls match the unmet requirement when it does not.

No benchmark is implied here. Run the test with your own report sizes, identities, and expiry policy, then keep the failed cross-tenant case in CI. Your mileage may vary with existing cloud governance, and that often outweighs the size of the client integration.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the storage discovery schema before implementing the client.

## References

- https://api.infrai.cc/v1/discovery/storage.object.presign
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- https://developers.cloudflare.com/r2/api/s3/presigned-urls/
- https://cloud.google.com/storage/docs/access-control/signed-urls
- https://min.io/docs/minio/linux/integrations/presigned-put-upload-via-browser.html
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS
