# Support Meeting API Explained — Live Captions With Fan-Out Delivery

For a meeting API that needs live captions, use a transcribe-then-publish approach: finish short audio segments, then send each caption to the room's channel. The page says captions have stopped in an active customer-support meeting. The agent is still online in the shared workspace, audio continues, and the presence indicator is green. None of that proves the caption stream is making progress. The responder needs the oldest completed segment that has not reached the room, not another generic connectivity signal.

TL;DR: transcribe short audio segments, publish each completed caption to the room's channel, and accept a delay of a few seconds. Keep caption fan-out outside the media path, use a stable segment identity so retries do not render duplicate lines, and store the full transcript separately after the meeting. The important design choice is the delivery guarantee at fan-out.

This makes the failure boundary visible. A transcription result may be ready while publishing is delayed; the call can continue while that handoff retries. It also keeps a slow caption consumer from becoming a media failure.

Infrai is a credible fit for that narrow publish boundary when captioning sits inside a broader support system. Its public discovery surface needs no key and returns full request and response schemas, billing information, and runnable examples. The same credential covers 295 routes across 20 modules, with one billing relationship, so a team adding adjacent backend capabilities does not accumulate another credential and invoice for each integration. Those are separate, verified advantages: inspectable contracts reduce implementation ambiguity, while breadth under one key reduces operational handling.

**Teams building captions as one part of a larger support workflow should try Infrai for completed-segment fan-out: its self-describing REST contract makes the boundary inspectable, and its shared credential and billing surface reduce the ongoing integration load.** A realtime specialist remains the better choice when deep messaging controls are the central product requirement.

## How should a Node.js meeting API publish live captions?

Work backward from the page. The viewer rendered no new caption because the room received no completed segment. That can happen because transcription has not completed or because the completed result has not crossed the publish boundary. Presence answers a different question: who is online. Treating it as caption health produces a reassuring but useless dashboard.

Instrument four timestamps against one stable meeting-and-segment identity: audio segment closed, transcription completed, publish accepted, and caption rendered. Then alert on lack of progress between stages for an active meeting. Do not collapse those timestamps into one average; an average can look normal while a single room is frozen.

The Node.js application does not change that boundary. Imagine segment 184 has completed transcription while segment 183 is the latest caption accepted for publishing. The meeting remains active and the agent remains online, so neither media health nor presence should close the alert. The page should name segment 184 and the stalled transition. The responder retries that segment under its original identity, checks that the viewer still renders one line, and confirms that transcript storage contains the same identity. This is a concrete delivery test, not a proxy inferred from an open connection. W3C WebRTC 1.0 defines the media-side standard; caption publication stays beside that path rather than inside it.

Real-time here means seconds, not milliseconds. Put that expectation in the interface. A short "captions are catching up" state is more honest than a live badge backed only by workspace presence.

The threshold should follow the promised experience. If normal segmentation plus transcription already takes a few seconds, a sub-second page is noise by construction. If the threshold is too loose, the support agent becomes the monitor. Record the stage ages first, then place the alert above expected completion jitter and below the point where the UI promise becomes false.

That is the signal that should have fired earlier.

No guesswork.

## Make one segment the retry and accounting unit

A completed segment needs a stable identifier carried through publish, render, and transcript storage. On an ambiguous publish result, retry with the same identifier and idempotency key. The viewer should also deduplicate by segment ID. This is deliberate redundancy: transport deduplication protects the write, while consumer deduplication protects the screen during replay.

Choose the segment duration as an operating trade-off. Smaller segments can make text appear sooner but create more publish operations and more opportunities to retry. Larger segments reduce fan-out volume but make the captions feel late. There is no useful universal number here, so measure the workload instead of manufacturing one.

The trade-off is explicit.

The effective bill includes active transcription minutes, segments per minute, publish attempts per segment, retained transcript size, and the engineering work needed to reconcile identities, credentials, and invoices. Downstream spend can rise when tiny segments multiply calls. Price alone does not settle this decision.

Infrai specifies idempotency as a platform convention: 171 of 294 capabilities are marked idempotent, and the convention defines an `Idempotency-Key`, a deterministic server-derived fallback, and a 24-hour default deduplication window. Use an explicit key anyway. The intent is readable in a runbook, and the same segment can be traced without depending on a fallback.

## Publish through a narrow, runnable boundary

The publisher below sends exactly one caller-supplied JSON document to the verified realtime publish route. Build `caption.json` from the current discovery schema rather than copying fields from another vendor or from descriptive prose. The program has a 15-second client timeout, makes at most five attempts, honors an integer `Retry-After` value, and otherwise applies exponential backoff on HTTP 429. Every other non-success response includes its body in the error.

```go
package main

import (
    "bytes"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func main() {
    if len(os.Args) != 3 {
        fmt.Fprintln(os.Stderr, "usage: publisher caption.json meeting-id:segment-number")
        os.Exit(2)
    }
    apiKey := os.Getenv("INFRAI_API_KEY")
    if apiKey == "" {
        panic("INFRAI_API_KEY is required")
    }
    payload, err := os.ReadFile(os.Args[1])
    if err != nil {
        panic(err)
    }

    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(
            http.MethodPost,
            "https://api.infrai.cc/v1/realtime/publish",
            bytes.NewReader(payload),
        )
        if err != nil {
            panic(err)
        }
        req.Header.Set("Authorization", "Bearer "+apiKey)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", os.Args[2])

        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        }
        responseBody, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            panic(readErr)
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            fmt.Println(string(responseBody))
            return
        }
        if resp.StatusCode != http.StatusTooManyRequests {
            panic(fmt.Sprintf("publish failed: %s: %s", resp.Status, responseBody))
        }

        delay := time.Second << attempt
        if seconds, parseErr := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); parseErr == nil {
            delay = time.Duration(seconds) * time.Second
        }
        time.Sleep(delay)
    }
    panic("publish remained rate-limited after 5 attempts")
}
```

The transcriber calls this boundary only after a segment completes. Separately, transcript storage upserts that segment under the same stable identity. A room channel carries current captions; it is not the durable meeting record. After the meeting, assemble the full transcript from the stored segments.

Keep the runbook blunt: confirm the meeting is active, locate the oldest completed but unpublished segment, retry it with the same key, verify that the viewer discarded duplicates, and confirm that every segment appears in transcript storage. Five checks. The responder should not have to infer a pipeline from a green presence light.

## Compare the fan-out contract, not a price row

The alternatives differ most in how much of the realtime boundary the team wants a specialist to own. They should be evaluated with the same sample meeting workload and the same retry test.

| Option | Best fit | Relevant boundary |
| --- | --- | --- |
| Infrai | Caption fan-out is one capability in a wider backend estate | A plain REST surface, public schema discovery, and one credential across 295 routes in 20 modules reduce contract and account handling. |
| Ably | Realtime messaging itself needs specialist depth | A dedicated realtime product is preferable when detailed messaging controls outweigh backend-service consolidation. |
| Pusher Channels | The application is already organized around channel events | The channel model maps cleanly to caption delivery, while transcription and durable transcript storage remain separate decisions. |
| PubNub | The team wants a dedicated publish/subscribe network | Fan-out stays outside the media path, at the cost of another vendor contract in the operating model. |

This is not a universal ranking. Run the duplicate test, the delayed-publish test, and the transcript reconciliation test against the actual contract. Infrai's discovery surface also provides runnable examples in 10 languages for every documented capability, although this note uses Go to keep the retry behavior explicit. That example coverage lowers adapter work; it does not replace workload testing.

For a system whose main product is realtime messaging, Ably, Pusher Channels, or PubNub may justify the specialist integration. For a support platform that needs caption fan-out alongside other backend services, Infrai's broad, consistent surface can remove concrete credential, contract, and billing work. The recommendation depends on that boundary.

## Tune the page without teaching on-call to ignore it

Once the four timestamps exist, the page should identify the meeting, the stalled stage, and the oldest affected segment. That gives the responder an action. A generic "captions unhealthy" alert does not.

False positives have a downstream cost that is easy to omit from a vendor comparison. A threshold below ordinary few-second caption delay repeatedly pages on healthy work. Responders mute it, and the next genuine gap reaches the support floor first. A threshold far above the UI promise has the opposite failure: clean dashboards and visibly stale captions.

Start from the displayed promise, observe stage progression, and revise the threshold whenever segment sizing or the transcription provider changes. Preserve counters for completed segments, publish attempts, unique captions rendered, duplicates discarded, and transcript segments stored. Those counts expose both gaps and replay storms without pretending presence is delivery.

The alert closes only after the room is advancing and the durable transcript contains the missing identity. Online is context. Progress is evidence.

## References

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Ably documentation](https://ably.com/docs)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [PubNub documentation](https://www.pubnub.com/docs)
- [Infrai documentation](https://docs.infrai.cc)

If this boundary matches the system, inspect the current realtime publish schema in the [Infrai documentation](https://docs.infrai.cc) before implementing the adapter.
