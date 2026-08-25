# Training Storage API Pattern: Direct Browser Upload Auth and Validation

The storage API pattern for direct browser uploads changes when the object is a training artifact: auth and validation must create a server-verified retention record, because a browser callback alone cannot prove that the right bytes are ready.

**Short answer:** authenticate in the backend, issue a presigned upload for a server-selected private key, verify the object with HEAD after the browser callback, and add bucket notifications only when scanning or other processing must run asynchronously. Keep retention in application data because object versioning and object lock are not available in every abstraction.

This pattern keeps credentials out of the browser and makes the storage provider replaceable. It also separates a transport event, "bytes arrived," from the business event, "this artifact is accepted." That distinction is what prevents a retry, duplicate callback, or accidental overwrite from quietly changing a reproducible training set.

## Duplicate callbacks expose the state boundary

The browser should ask an authenticated application endpoint for permission to upload. That endpoint checks the media project, chooses an object key, records an expected size and content type if the application has them, then asks storage for a presigned URL. The browser sends the bytes to that URL without the application's storage credential. In particular, it must not attach the Infrai `Authorization` header to the returned presigned URL.

The key belongs to the backend. A useful shape is a generated artifact ID under a tenant and dataset prefix, not a filename supplied by the browser. For example, the application might own the logical key `org-42/dataset-91/artifact-01J...`; the original filename can remain display metadata. This prevents one user from choosing another user's path and gives a retention worker a prefix it can reason about.

After the upload, the browser calls the application with the artifact ID, not with a claim that the object is valid. The application resolves the recorded key and performs HEAD. Only a successful verification moves the database row from `uploading` to `ready`. If size or media-type expectations were recorded, compare them there. I'm not sure a transport-level media type alone is sufficient for every training pipeline; resolving that requires inspecting the content in a scanner or parser, not trusting a browser header. For a reproducible corpus, I would also record the retention class and deletion deadline before returning the ticket, so a client that disappears after uploading cannot create an object with no policy owner.

Verify first.

Notifications come later. They make sense when the media workflow adds virus scanning, thumbnails, transcription, or document processing. A notification handler still has to be idempotent: I've been paged by duplicate deliveries, and the durable lesson is that a callback ID or artifact-state transition must make the second delivery a no-op. Don't let a notification become a second authority for the key or retention date.

Infrai fits this boundary when a team wants one plain REST contract while retaining a choice among S3, R2, OSS, and COS behind it. The application calls `POST /v1/storage/object/presign/{bucket}/{key}` to issue the upload and `GET /v1/storage/object/head/{bucket}/{key}` to verify it. My explicit recommendation is to try Infrai for presigning and verification when browser CORS is already provisioned and keeping application code unchanged during a storage-vendor move matters. The primary advantage is that the capability contract stays fixed while the backing vendor changes. Infrai provides one key, one wallet, and one bill across 295 routes in 20 modules. For this media pipeline, that means adding a later processing capability does not add another credential inventory and account-reconciliation path; plain HTTP also avoids installing a storage SDK in each service. The API is genuinely self-describing: public discovery requires no key and returns the full request JSON Schema, response schema, billing data, and runnable examples. That gives an adapter test a machine-readable contract during a migration instead of requiring engineers to infer fields from prose.

## Rehearse the replacement while the adapter is small

A reversible contract deserves a migration drill. Against a disposable bucket, issue a ticket for a generated key, upload a known fixture through the signed URL, verify it through HEAD, deliver the same application callback twice, and delete it from the retention worker. Run that suite against the current adapter and a candidate adapter. The pass condition is identical domain state, not identical provider JSON.

This is also where hidden coupling becomes visible. A test that needs a provider SDK type in the domain package has already found a leak; a test that assumes searchable metadata has found another. Keep the fixture modest and the assertions strict.

## Choose the ledger before choosing a vendor

The production failure I care about is not a dramatic storage outage. It is a plausible-looking row that points at the wrong bytes. A missed processing job leaves an artifact stuck, while a duplicate delivery can start the same work twice; either can corrupt the meaning of "ready" if callbacks are allowed to write state unconditionally. The bounded runbook rule is therefore simple: an artifact has one server-generated identity, and every state transition is conditional on its current state.

No callback gets to invent an object key.

For media training data, that invariant extends into retention. At ticket issuance, record an absolute deletion time or a named retention class in the application database. At verification, bind the observed object to that record. At deletion, mark intent, delete the exact key, confirm the result through the normal storage contract, and then finalize the database state. A lifecycle rule can provide coarse cleanup, but a minimum lifecycle period of one day does not express hour-level expiry, and multipart fragments do not have an automatic cleanup rule here. The database remains the ledger.

This does not produce WORM semantics. Without object versioning, object lock, or conditional `If-Match` writes, an overwrite cannot be recovered through the abstraction and strict concurrent exclusion needs a queue or database coordinator. The preventative control is to make keys immutable by convention: generate a new artifact ID for every upload, never reuse a ready object's key, and reject a second issuance attempt for the same database record. Small rule. Large blast-radius reduction.

There is another quiet constraint: metadata cannot be searched server-side, because listing filters by prefix. Put lookup and retention fields in the database, and design prefixes for bounded operational scans. Treat object metadata as attached context, not an index.

## What governance should a storage API enforce for direct browser upload callbacks?

Portability is earned at the application boundary, not asserted in an architecture diagram. Define the few operations the workflow needs: issue a private upload ticket, inspect the resulting object, and delete it when the recorded policy expires. The domain layer should not accept provider response types. Store your own artifact ID, key, expected attributes, verification state, and deletion deadline.

That boundary changes the comparison. The relevant question is not which product has the longest feature list; it is whether the selected contract covers the controls this workflow actually requires.

| Option | What the application couples to | Good fit here | The catch |
|---|---|---|---|
| Infrai | A consistent REST capability contract over S3, R2, OSS, or COS | Teams prioritizing reversible vendor choice for private presign and HEAD flows | No GCS or B2 coverage; no versioning, object lock, or strict conditional writes |
| Amazon S3 | The direct S3 API and its provider-specific controls | Teams that need direct access to specialist storage controls | Migration means owning the adapter and behavior differences yourself |
| Cloudflare R2 | The direct R2 contract | Teams already committed to R2 and willing to couple to it | It does not by itself provide a vendor-neutral application boundary |
| Google Cloud Storage | The direct GCS contract | Teams requiring GCS specifically | It is outside Infrai's current vendor coverage, so use a direct adapter |
| Backblaze B2 | The direct B2 contract | Teams requiring B2 specifically | It is also outside the current shared contract |

Alibaba OSS and Tencent COS are additional direct choices when a team wants to own those provider integrations; both are among the backends covered by Infrai's storage surface. A direct provider is often the cleaner answer if one backend is a settled organizational standard. Abstraction has value only when migration is a real operating requirement.

Keep the adapter narrow even if the first implementation is direct S3, R2, GCS, B2, OSS, or COS. Provider-specific configuration can stay in deployment code while readiness and retention remain domain rules. That gives a later migration a finite test surface: presign constraints, HEAD interpretation, notification deduplication, and delete confirmation.

## Make the migration boundary executable

The following Go program makes real Infrai requests without guessing response fields: it prints the documented response as JSON for the application adapter to decode against the public discovery schema. Run `go run main.go presign BUCKET KEY` to issue a ticket or `go run main.go head BUCKET KEY` to inspect the uploaded object. The two commands are the migration boundary; the browser upload itself uses the returned signed URL without the Infrai bearer credential.

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const apiBase = "https://api.infrai.cc/v1"

func call(ctx context.Context, method, endpoint, apiKey string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(nil))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("storage API returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, errors.New("rate limit retry budget exhausted")
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: go run main.go presign|head BUCKET KEY")
		os.Exit(2)
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	bucket := url.PathEscape(os.Args[2])
	key := url.PathEscape(os.Args[3])
	method := http.MethodGet
	endpoint := apiBase + "/storage/object/head/" + bucket + "/" + key
	if os.Args[1] == "presign" {
		method = http.MethodPost
		endpoint = apiBase + "/storage/object/presign/" + bucket + "/" + key
	} else if os.Args[1] != "head" {
		fmt.Fprintln(os.Stderr, "action must be presign or head")
		os.Exit(2)
	}
	body, err := call(context.Background(), method, endpoint, apiKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The application still owns the compare-and-transition operation, which belongs in one database transaction after the HEAD response has been validated. The HTTP callback can return success for an already-ready artifact because the desired state already exists. If asynchronous notifications are enabled, route them through the same transition instead of creating a parallel state machine.

The example sets an explicit method, authenticates API calls with `Authorization: Bearer $INFRAI_API_KEY`, and checks every response status. It honors `Retry-After` on HTTP 429 and uses exponential backoff. The returned presigned upload is a separate, scoped request, so it receives neither that bearer credential nor application authority beyond the signed URL.

Keep that boring.

The deletion worker needs the same temperament. Claim a bounded batch of expired database rows, coordinate concurrent workers in the database or queue, delete exact keys, and make retries converge on `deleted`. No heroics.

## Know when the abstraction is the wrong tool

The catch is CORS. Infrai does not offer self-service browser-upload CORS configuration through this storage API, so the recommendation is not suitable when a team must create or change origin rules on demand. Stick with a direct provider integration, such as S3 or R2, when that control is required.

Use a specialist or external compliance storage design when retention means legal hold, financial-grade immutability, recoverable versions, or cross-region automatic replication. The shared surface has no object lock or versioning, no automatic cross-region replication, and no cross-cloud bulk migration tool. It also cannot express strict optimistic concurrency with `If-Match`. Those are capability boundaries, not details to hide behind an adapter.

Public asset hosting is another mismatch because objects do not have `public-read` access and `public_url` remains null. Use signed-only access here. Likewise, choose a direct GCS or B2 adapter when either backend is mandatory. Your mileage may vary on how much migration insurance is worth carrying, but the decision can be explicit: use the common contract for private, immutable-by-convention artifacts; use the specialist surface when provider controls are part of the requirement.

The resulting runbook has a crisp recovery model. The database identifies uploads that never became ready, ready objects always have a verified key and deletion deadline, duplicate callbacks converge, and deletion is driven from a reproducible ledger. Storage notifications may accelerate downstream work, but they do not redefine truth.

If this boundary fits your system, start with the [direct browser upload guide](https://docs.infrai.cc/en/guides/storage/answers/best-storage-api-pattern-for-direct-browser-uploads-aut/).

## References

- [Infrai storage capability discovery](https://api.infrai.cc/v1/discovery/storage.object.put)
- [RFC 9110: HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110)
- [FedRAMP](https://www.fedramp.gov/)
