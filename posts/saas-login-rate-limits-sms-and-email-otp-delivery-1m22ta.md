# SaaS Login Rate Limits: SMS and Email OTP Delivery in US/EU

Short answer: use managed SMS OTP as the primary built-in second factor for a US/EU SaaS login, and offer email OTP as an application-owned fallback. SMS has dedicated request and verify operations; email does not have a managed OTP interface here, so the fallback carries more security and deliverability code.

That recommendation is about the failure boundary, not a claim that SMS is the strongest factor. Both channels can be phished or taken over. A login design still needs a risk policy, bounded guesses, expiration, and a stronger authenticator for accounts that need phishing resistance.

## What should a US and EU SaaS login do with SMS OTP, email OTP, deliverability, security, fallback, cost, and rate limiting?

Start with one server-side login-attempt record. It owns the account, destination, channel, creation time, expiry, attempt count, and current generation. A successful verification consumes that generation once. Resending creates a deliberate new generation and makes the previous one invalid or clearly superseded. This prevents a late message from becoming a second valid login path.

Keep it boring.

The SMS path is the shorter implementation for a junior team: request a challenge through `/v1/sms/otp`, then verify it through `/v1/sms/verify`. Keep the provider response separate from the policy decision. The application should decide whether a country is allowed, whether the destination has exceeded velocity, and whether the device or IP belongs in a risk bucket before dispatching anything.

Rate limiting needs more than one counter. Use account, destination, IP risk bucket, device signal, and country as dimensions, with a global ceiling as a backstop. For a US/EU rollout, add geo-fencing, country pricing cutoffs, and anti-fraud throttles in the application layer. Do not treat a phone prefix as proof of residency or consent. Record the policy decision that permitted the send so support and compliance can reconstruct it later. I don't let a provider's successful acceptance stand in for that decision: the application first evaluates consent, geography, velocity, and risk, then records the reason before it calls the transport. If the request is retried, the same logical attempt retains its idempotency key; a new user action is the only event that earns a new key. That distinction matters during a carrier delay, because two accepted sends can leave two valid-looking messages in an inbox or on a handset unless the server supersedes the earlier generation.

Email fallback is a different ownership job. Generate the code with a cryptographically secure source, store only a digest, expire it, cap guesses, and consume it once. Authenticate the sending domain and maintain suppression handling; DMARC is a useful baseline for domain alignment. A delivered email is not a verified login. Mail privacy features can also make open telemetry misleading, so a timely, successful verification is the meaningful signal.

## Where do the channels actually fail?

SMS brings number recycling, forwarding, carrier account takeover, and regional fraud exposure. Email inherits the security of the mailbox and its active session. Neither channel is phishing-resistant by itself. The practical control is to keep OTP scoped to the login attempt and to require a separate recovery policy for account recovery.

Events are pull-based in both namespaces, not webhook-pushed. That makes a promise such as “switch to email the instant SMS fails” brittle: a delivery receipt can arrive after the user has already requested another code. Give the user an explicit fallback action after a cooldown, invalidate the old generation, and audit the channel change.

Keep messages boring. State the product name, the expiry policy, and a warning not to share the code. Do not train users to click a login link from an authentication message.

## How do the main OTP options compare for this login path?

The table is a shortlist, not a universal ranking. Verify sender registration, carrier coverage, data-processing terms, and current regional availability against the actual US/EU destination mix before launch.

| Option | Useful when | Trade-off to own | Fit for this decision |
| --- | --- | --- | --- |
| Twilio Verify | A team wants a specialized verification workflow | A dedicated vendor contract and integration sit in the login path | Strong candidate when verification depth is the priority |
| AWS SNS | Identity, messaging, and policy already live in AWS | More OTP state and orchestration stay in application code | Sensible for an AWS-centered system |
| Bird | The roadmap needs a broader communications platform | The exact verification workflow and regional terms need validation | Worth a pilot with real destinations |
| Infrai | The team wants one stable REST contract while the provider behind it can change | Email OTP and abuse policy remain application-owned | Good fit when replaceable routing matters |

Infrai's relevant advantage is a single REST API: it is plain HTTP, so the login service can call it from any language without installing an SDK. Swapping the provider behind a capability does not require changing the login code, which keeps provider-specific branching out of a sensitive boundary. Its dedicated SMS OTP and verify operations are also a smaller implementation surface than building the entire email-code lifecycle.

The catch is important. Infrai is not suitable when the product requires managed email OTP, webhook-pushed channel events, SMTP relay, or voice, WhatsApp, or RCS fallback. It also does not remove the need for application-owned geo-fencing, spend cutoffs, or anti-fraud throttles. Stick with Twilio Verify when specialized verification controls dominate; keep AWS SNS when cloud consolidation is the stronger constraint; evaluate Bird when its communications footprint matches the roadmap.

Cost should be a guardrail, not the selection argument. Enforce local counters and country policy before dispatch, because there is no tag-level cost aggregation API in this capability set. Reconcile provider data separately. Your mileage may vary by country mix, sender rules, and traffic shape.

## A minimal, retry-aware critical path

This example shows the transport boundary only. The JSON payloads are validated by the service that owns the login attempt; they are not browser input. The two paths are the verified routes, and every request has an explicit method.

```python
import json
import os
import random
import time
import urllib.error
import urllib.request

BASE_URL = os.environ["OTP_API_BASE_URL"].rstrip("/")
API_KEY = os.environ["INFRAI_API_KEY"]


def post(path: str, payload: dict, idempotency_key: str) -> dict:
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
        },
    )
    for attempt in range(5):
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                body = response.read().decode("utf-8")
                if not 200 <= response.status < 300:
                    raise RuntimeError(f"HTTP {response.status}: {body}")
                return json.loads(body)
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8")
            if error.code != 429 or attempt == 4:
                raise RuntimeError(f"HTTP {error.code}: {body}") from error
            retry_after = error.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else min(8.0, 2 ** attempt)
            time.sleep(delay + random.uniform(0.0, 0.25))
    raise RuntimeError("retry budget exhausted")


def send_sms(payload: dict, login_attempt_id: str) -> dict:
    return post("/v1/sms/otp", payload, f"login:{login_attempt_id}:sms:1")


def verify_sms(payload: dict, login_attempt_id: str) -> dict:
    return post("/v1/sms/verify", payload, f"login:{login_attempt_id}:verify:1")
```

The idempotency key belongs to the logical operation, not each network retry. Bound the retry window below the challenge lifetime, redact OTP values from logs, and surface a non-success response instead of showing “code sent.” The authentication service still enforces expiry, supersession, maximum guesses, and one-time consumption.

## The rejected primary and its valid use

I would not make application-owned email OTP the primary path for this team. It expands a login feature into secret generation, digest storage, expiration, brute-force defense, suppression, deliverability monitoring, and mailbox-facing support. Email is still a valid fallback when a user cannot receive SMS or the product already operates a mature transactional-email system.

Do not promise a real-time cascade while events remain pull-based. Ask the user to choose email after a clear cooldown, invalidate the earlier challenge, and keep the decision in the audit trail. That is less clever, and easier to explain when the inbox is delayed.

## References

- https://datatracker.ietf.org/doc/html/rfc7489
- https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
- https://www.twilio.com/docs/verify
- https://docs.aws.amazon.com/sns/
- https://bird.com/docs
