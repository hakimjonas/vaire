# Documentation index

Status: current
Audience: contributor.

Every document in `docs/` is classified by audience and status. The vocabulary follows Fungal
ADR 004 §4.1: `authoritative`, `current`, or `historical`. Superseded documents are deleted
rather than archived (ADR 004 §4.3); nothing is kept only because it once mattered. The directory
currently contains no historical document: the deletion rule was applied for the first time when
this index was revised.

## Documents

| Document | Audience | Status | Purpose |
|---|---|---|---|
| [`backend-claims.md`](backend-claims.md) | user; contributor for internal sections | current | Which constructs each backend rejects, and the test that pins each rejection |
| [`ci.md`](ci.md) | contributor | current | Test tiers, gates, memory model, local runs |
| [`error-policy-design.md`](error-policy-design.md) | contributor | current | Per-row error policy design and the shipped E1–E3 decisions |
| [`xpath-divergence.md`](xpath-divergence.md) | user; contributor for internal sections | authoritative | The XPath divergence, which backend to trust, and the upstream record |
| `README.md` (this file) | contributor | current | The index and the convention below |

## Status vocabulary

- **authoritative** — the normative statement of its subject. Other documents point here instead
  of restating it.
- **current** — maintained and accurate, but not the normative source for another document.
- **historical** — served its purpose. This is a deletion marker, not a retained state: a
  document classified historical is deleted in the change that classifies it, and is recovered
  from git rather than kept here. No document in this directory carries it.

## Audience and placement

- **user** — behaviour a caller can hit: divergences, unsupported constructs, performance
  boundaries. A new user-facing divergence belongs in the document that owns that subject, not
  in a plan or a ledger.
- **contributor** — CI, design rationale, conventions, and how to change the code.
- **internal** — coverage ledgers and filing records. These are bookkeeping, not product
  documentation.

A document with more than one audience states both on its `Audience:` line and marks the
internal parts with `Internal:` in the section heading. A new document must name its audience
and status here before it is added; a document with no audience is not a document.

## Plans

Work plans are not repository documentation. A plan belongs in the session or the issue tracker;
its durable outcomes belong in the CHANGELOG, the code and the tests. Git holds the plan itself.
Do not add a plan to `docs/`, and do not replace a retired plan with a summary document.

## Required header

Every file in `docs/` starts with its title, then:

```
Status: authoritative | current | historical
Audience: user | contributor | internal, and any mixed audience.
```

A missing or invented status word is a documentation defect.
