# Security Policy

## Supported Versions

Security fixes are applied to the latest released minor version.

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |
| < 0.1   | ❌        |

## Reporting a Vulnerability

Please report suspected vulnerabilities privately via
[GitHub Security Advisories](https://github.com/gourdian25/grnoti/security/advisories/new)
rather than opening a public issue.

Include:

- A description of the issue and its impact
- Steps or a proof-of-concept to reproduce
- Affected version(s)

You can expect an acknowledgment within a week. Once a fix is available, the
advisory will be published together with a patched release.

## Scope Notes

grnoti dispatches push notifications via Firebase Cloud Messaging and reads
events from Kafka; it stores device tokens, user preferences, idempotency
markers, and dead-letter records in whichever Mongo/Postgres/Redis backends
the caller configures. The most relevant security considerations for users:

- **Device tokens are bearer credentials for delivery, not identity.**
  grnoti does not verify that a `DeviceToken` actually belongs to the
  `UserID`/`AnonymousID` it's associated with — that binding is the caller's
  responsibility at token-registration time (`TokenStore.SaveToken`).
- **No encryption at rest or in transit is provided by grnoti itself.** TLS
  to Mongo/Postgres/Redis/Kafka and encryption of any sensitive
  `Event.Payload`/`Message.Data` values before they reach grnoti are the
  caller's responsibility.
- **`Event.Payload` and template data are not sanitized against injection
  into the rendered `Message`.** Templates are Go `text/template` (not
  `html/template`); a caller feeding untrusted input directly into a payload
  value that a template renders is responsible for sanitizing it first if
  that value could contain control characters or unexpected structure.
- **Idempotency and DLQ keys are trusted, not authenticated.** Any caller
  with a `IdempotencyStore`/`DLQHandler` handle can mark arbitrary event IDs
  processed or replay dead-lettered events; grnoti does not implement
  per-caller access control over these operations.
- **FCM credentials (service account keys) are never handled by grnoti
  directly** — the `PushDispatcher`'s FCM client is constructed and
  authenticated by the caller via the official Firebase Admin SDK, keeping
  key management outside this library's scope.
