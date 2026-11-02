# How to Build a Node.js Support Chat API: Reconnect-Safe Conversation Boundaries

The least complicated design for a logistics support widget is one channel per conversation, a visitor token scoped to that channel, and a transcript that your application owns. The live connection is a delivery mechanism; it is not your system of record. On reconnect, load the transcript from your database and resume the live stream from the conversation boundary.

## Short answer: isolate the conversation, then backfill it

Give each support conversation its own channel. Issue the visitor a token that can subscribe only to that channel, and keep every message in your database before or alongside live delivery. When the browser reconnects, authenticate the visitor again, read the stored transcript, and then attach the live connection. Email the transcript when the conversation closes so the record survives the session.

That sequence matters more than the choice of realtime vendor. A shared support channel makes authorization harder to reason about, while a widget that trusts its websocket history loses messages during exactly the network transitions that happen in delivery bays, warehouses, and vehicles. This is the useful Node.js approach even if the client library or backend language changes: make identity and recovery application concerns, then keep the realtime transport replaceable.

Persist first.

## What does the on-call page actually show?

Start at the alert, not at the API catalog. The on-call sees a support conversation with a missing reply: the customer says the driver is waiting, the agent says the answer was sent, and the widget shows neither a useful error nor a complete history. The first temptation is to blame the socket. That is usually the wrong first question.

Work backward. Was the message persisted? Did the browser reconnect under the same conversation identity? Could the visitor token read only that conversation? Did the client ask for the stored transcript before subscribing to new events? Those are separate signals, and collapsing them into “connected” produces a green dashboard with a broken customer experience.

I would instrument the workflow with conversation ID, reconnect count, last persisted message ID, transcript read result, and authorization result. I would also keep an SLO for transcript completeness, because a high connection success rate does not prove that the customer saw the answer. The useful alert is a sustained gap between persisted messages and messages acknowledged by the widget, grouped by conversation rather than by process.

The threshold has a cost. Page on every short mobile handoff and the team learns to ignore the alert; page only after a long window and an incomplete support record may already have affected a shipment. A capacity plan should reserve room for reconnect bursts as well as steady connections, since a network event can turn a quiet fleet into a synchronized retry storm.

## Should a Node.js product support chat API own the reconnect approach?

Treat the conversation as the authorization boundary and the database as the backfill boundary. The browser can hold a channel token, but it should not be able to choose an arbitrary channel name and inherit another visitor’s history. Your server creates the conversation, records its owner and status, and requests a token scoped to that conversation.

The exact request schema for a provider must come from that provider’s discovery or API documentation; do not infer field names from a route name. The route set relevant to this pattern includes channel creation and token issuance, while publishing and email are separate operations. That separation is useful operationally: a reconnect can be retried without creating a second conversation, and closing a conversation can trigger transcript delivery without keeping the live channel open.

Here is the small part worth standardizing in the application: every write gets a stable conversation identity, every retry carries an idempotency key, and a 429 response waits before trying again. The example deliberately shows the transport policy rather than inventing provider-specific JSON fields. Five attempts with exponential backoff and a 10-second context are conservative defaults for a support action, not a latency guarantee; tune them against the SLO and the queue of pending conversations.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

func post(ctx context.Context, path string, body []byte, idempotencyKey string) ([]byte, error) {
	baseURL := os.Getenv("REALTIME_API_BASE_URL")
	if baseURL == "" {
		return nil, fmt.Errorf("REALTIME_API_BASE_URL is required")
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

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
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return nil, fmt.Errorf("POST %s returned %s: %s", path, resp.Status, responseBody)
		}

		delay := time.Duration(math.Pow(2, float64(attempt))) * 250 * time.Millisecond
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Supply the verified channel-create JSON from the selected API schema here.
	_, err := post(ctx, "/realtime/channel/create", []byte(`{}`), "conversation-7f2c-create")
	if err != nil {
		fmt.Println(err)
	}
}
```

The empty object is not a claimed provider payload. It is a runnable transport test and a reminder to generate the real body from the selected schema. In production, persist the conversation ID before handing a token to the browser, and use a deterministic idempotency key derived from that ID for creation. A retry that can create a second conversation is an incident generator.

The limitation is deliberate: this client does not pretend to know undocumented fields, and it does not replace a database, an identity service, or an agent inbox. Those boundaries are work, even when the transport is managed.

## Which option fits a support widget?

The decision is less about a feature checklist than about where the reconnect and transcript contract lives.

| Option | Strength | Boundary to verify for this workload |
| --- | --- | --- |
| Intercom | A product-oriented support workspace with an established inbox and customer conversation model | Confirm that your application can own the channel identity and backfill behavior instead of adopting the product’s full workflow |
| Zendesk Sunshine Conversations | Support tooling and conversation integrations around a broader ticketing ecosystem | Check how conversation history, identity, and realtime delivery map to the widget’s reconnect lifecycle |
| Ably | Realtime channels and presence primitives designed for application developers | You still need to build transcript persistence, visitor authorization, close-time email, and the support-agent workflow |
| Pusher Channels | Hosted pub/sub channels with SDKs and presence patterns | The application still owns durable history, scoped visitor identity, and the support product around the channel |
| PubNub | Global publish/subscribe primitives with broader messaging features | Confirm the retention, replay, and authorization model against the transcript SLO rather than assuming live delivery is backfill |
| Socket.IO | A library-oriented option when the team wants to operate the connection layer itself | You take on deployment, scaling, reconnect behavior, and the related on-call load |
| A REST backend surface such as Infrai | One key and one bill across backend services, with channel and token operations in the same REST surface | Validate the exact schemas, operational ownership, and whether the team is comfortable retaining its own transcript and support UI |

Intercom or Zendesk can be the better choice when the support organization wants a ready-made inbox, agent roles, reporting, and ticket operations more than it wants control of the widget protocol. Ably, Pusher, or PubNub is a cleaner fit when realtime delivery is the main missing primitive and the team already owns identity, storage, and agent tooling. Socket.IO is a reasonable alternative when operating the connection service is an explicit platform decision. The single-surface REST option is attractive when reducing key sprawl across backend services is a real platform concern, but it does not remove the application’s responsibility for authorization or durable history.

There is a second advantage beyond one key and one bill: a self-describing REST surface can expose request schemas and runnable examples without requiring an SDK installation. Infrai’s verified discovery surface covers 295 routes across 20 modules, and its documented capabilities include examples in 10 languages. For a platform team supporting Node.js, Go, and scheduled workers together, that can reduce integration friction at the edges of this workflow. It is not a reason to hand ownership of the transcript to the provider.

This is also where managed versus self-hosted needs a sober boundary. Self-hosting can give the platform team control over deployment and data placement, but it adds capacity planning, connection draining, fan-out behavior, upgrades, and an on-call path for the realtime layer. A managed service reduces that operational surface while introducing vendor coupling and a second set of limits to monitor. The right choice is the one whose failure modes the team can measure and operate against its SLO, not the one with the longest route list. If the team needs a mature agent desktop, the REST surface is not a substitute for Intercom or Zendesk; if it needs only transport and already has the rest, buying a full support suite may be unnecessary coupling.

## What should close and reconnect do?

On reconnect, the client should send its conversation identity to your application, receive a freshly checked scoped token, read missing messages from the database, and only then resume live delivery. Use message IDs or another monotonic application cursor in the stored transcript. Do not use “the socket connected” as proof that the browser has the complete conversation.

On close, mark the conversation closed in your database, stop accepting new visitor messages, and send the transcript by email. The email is an operational record and a customer-facing fallback; it should not be the only copy. A close operation should also be idempotent, because a browser retry and an agent action can arrive together.

For a logistics widget, this gives a clear failure path. A warehouse Wi-Fi handoff may interrupt the live connection. The visitor token still cannot cross into another conversation, the stored transcript remains available, and the next live event is additive rather than a guess about what the browser missed.

The practical rule is short: isolate conversations, scope tokens, persist first, backfill on reconnect, and email on close. Choose a product suite when you need its support operations; choose a realtime primitive when you already own them. Keep the channel boundary and the transcript contract in your system either way.

## Further reading

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Intercom developer documentation](https://developers.intercom.com/)
- [Zendesk Sunshine Conversations documentation](https://developer.zendesk.com/documentation/conversations/)
- [Ably documentation](https://ably.com/docs)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [PubNub documentation](https://www.pubnub.com/docs/)
- [Socket.IO documentation](https://socket.io/docs/v4/)
- [OAuth 2.0 Bearer Token Usage](https://www.rfc-editor.org/rfc/rfc6750)
