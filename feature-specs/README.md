# Feature specifications

This directory contains normative, language-agnostic specifications for client features. A feature
specification explains the observable behavior and invariants that every implementation of the
feature must preserve, independently of programming language, SDK, or internal architecture.

These documents are the source of truth for feature behavior. Implementations and tests are
expected to conform to them. If code, tests, and a specification disagree, treat the disagreement
as a defect to resolve explicitly; do not silently weaken a specification to match an accidental
implementation detail.

## Change discipline

Change a feature specification with great care. A specification change can alter compatibility,
routing, failure handling, concurrency, or operational safety across every client that implements
the feature.

Every change should:

- state the intended behavior precisely;
- preserve language-independent terminology;
- consider compatibility and failure-mode consequences;
- update interaction and edge-case sections;
- update the implementation mapping when code entities move or change;
- add or update conformance tests in every affected implementation; and
- call out known implementation gaps rather than describing them as completed behavior.

Each feature specification must include:

- vocabulary and a mapping from portable terms to local code entities;
- core behavior and configuration;
- interactions with other features;
- edge cases in both standalone and interacting behavior; and
- references to conformance tests.

## Specifications

- [Compression](compression.md)
- [Header optimization](header-optimization.md)
- [Key-route affinity](key-route-affinity.md)
- [Node health](node-health.md)
- [Query plans](query-plan.md)
