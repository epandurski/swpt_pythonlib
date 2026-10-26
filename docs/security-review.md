# Security and bug review: swpt_pythonlib

| | |
|---|---|
| Reviewed commit | `2b5796c` (2026-03-25) |
| Review date | 2026-10-26 |
| Scope | `swpt_pythonlib/` (SWPT URIs, protocol schemas, RabbitMQ consumer and publisher, signal bus, utils) |
| Method | Manual code review; findings reproduced by calling the library directly |

This document covers the issue **fixed in this pull request**, plus
review notes.

## Summary

| ID | Severity | Finding | Status |
|---|---|---|---|
| BUG-1 | Low | SWPT URI parsing accepts non-canonical URIs (Unicode digits, trailing newline) | Fixed |

## BUG-1: Non-canonical SWPT URIs are accepted (Low, fixed)

**Where:** `swpt_pythonlib/swpt_uris.py`.

**Problem.** The URI regular expressions used `\d` and `$`:

- In Python's `re` module (with `str` patterns), `\d` matches any
  Unicode decimal digit, and `int()` converts them. So
  `swpt:١٢٣` (Arabic-Indic digits) and `swpt:１２３` (full-width
  digits) were parsed as debtor `123`.
- `$` also matches just before a trailing newline, so `swpt:123\n` and
  `swpt:1/abc\n` were accepted too.
- `make_account_uri("abc\n")` passed the `URLSAFE_B64` check (also using
  `$`), so the newline was copied raw into the generated URI instead of
  being base64-encoded.

```
parse_debtor_uri('swpt:١٢٣')  → 123
parse_debtor_uri('swpt:123\n')                → 123
parse_account_uri('swpt:1/abc\n')             → (1, 'abc')
```

The parsed values are correct numbers, so this did not lead to a wrong
account. However, many different strings mapped to the same account,
which breaks the assumption that a URI has one canonical form. That
assumption matters where URIs are stored and compared as strings, for
example `recipient_uri` in `swpt_debtors`, which is compared on
idempotent retries.

**Fix.** The patterns now use `[0-9]` and `\Z`.

**Tests:** new cases in `test_parse_debtor_uri`,
`test_parse_account_uri` and `test_make_account_uri`. Three of the four
URI tests fail without the fix.

**Note for dependent services.** The services pin `swpt_pythonlib` to
a git tag (currently `v0.12.1`), so they pick up this fix only after a
new tag is released and their pins are bumped.

## Review notes

- **Consumer error handling.** In `rabbitmq.Consumer`, an exception
  raised by `process_message` stops the worker and terminates
  consumption, and the message is redelivered. A handler bug that
  raises on a particular message therefore blocks the whole queue. The
  `swpt_accounts`, `swpt_creditors` and `swpt_trade` handlers have been
  changed (in their own pull requests) to log and drop invalid messages
  instead of raising. Moving such messages to a dead-letter queue after
  a few failed deliveries would make this robust in the library itself.
- **Protocol schemas.** `PrepareTransferMessageSchema` validates
  `coordinator_id` for `direct` and `issuing` transfers but not for
  `agent` transfers, and `AccountTransferMessageSchema` accepts
  `acquired_amount == 0`. `swpt_stomp` rejects both at the network edge,
  but the consumers relied on stricter assumptions.
- **`u64_to_i64` / `Int64Converter`.** Out-of-range values raise
  `ValueError` (converted to a 404 in URL routing).

## Tests

Full suite: 78 passed, 17 skipped (the skipped tests need external
services).
