# Security Policy

## Supported versions

orcastork is pre-1.0. Security fixes land on `main` and in the next release; there are no
maintained backport branches yet.

| Version | Supported |
|---|---|
| 0.1.x | ✅ |
| < 0.1 | ❌ |

## Reporting a vulnerability

**Please do not open a public issue for a security vulnerability.**

Report it privately through either channel:

- [GitHub private vulnerability reporting](https://github.com/Imper-ai/orcastork/security/advisories/new)
  (preferred — it keeps the discussion attached to the repository)
- Email **security@imper.ai**

Please include, as far as you can establish it:

- what an attacker can do, and what they need in order to do it
- the affected version or commit
- a reproduction — a failing test against the in-memory runtime is ideal
- any mitigation you have found

You can expect an acknowledgement within 3 working days and an assessment within 10. We will
keep you informed while a fix is prepared, and credit you in the advisory unless you would
rather not be named.

## Scope

Reports about the library's own behaviour are in scope. In particular we treat these as
security-relevant, not merely as bugs:

- a way to bypass the fencing epoch and write to a session owned by a higher epoch
- a way to make `ctx.once` run a guarded side effect twice, or lose a committed claim
- PII persisted in plaintext, or under an unkeyed digest, where the archive is supposed to
  fail closed
- a `CompletionCondition` or `FlowDefinition` fingerprint that can be made to admit a graph it
  should reject
- sensitive values reaching logs, spans or audit records

Out of scope:

- vulnerabilities in Redis, MongoDB, or any dependency — report those upstream, though do tell
  us if orcastork's use of them is what makes the issue exploitable
- a deployment that wires `NullCipher` and then archives PII. The archive adapter raises
  `UnprotectedPiiError` rather than storing it, so this is the documented behaviour rather than
  a flaw; see [docs/deployment.md](docs/deployment.md#encryption)
- misconfiguration covered in the deployment guide, such as sharing a Redis database with
  another workload (the keys are deliberately unprefixed) or setting a lock TTL below
  `operation_timeout`
