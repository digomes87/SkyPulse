# Architecture Decision Records

Each significant design choice is recorded as a short ADR: context, decision, consequences.
Records are immutable once accepted; a change of mind is a new ADR that supersedes the old one.

| # | Title | Status |
|---|-------|--------|
| 0001 | Streaming abstraction over Kafka and Kinesis | Planned (M2) |
| 0002 | Delta Lake and medallion architecture | Planned (M3) |
| 0003 | Idempotency and deduplication strategy | Planned (M3) |
| [0004](0004-local-first-development.md) | Local-first development with LocalStack and replay fixtures | Accepted |
| 0005 | EU261 rules engine as a pure library | Planned (M4) |
| 0006 | Cost control on AWS | Planned (M8) |

## Template

```markdown
# NNNN. Title

- Status: Proposed | Accepted | Superseded by NNNN
- Date: YYYY-MM-DD

## Context
## Decision
## Consequences
```
