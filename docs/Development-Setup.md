# Development setup and repository policy

**Current state:** This repository contains planning documents only. There is no server, client, benchmark harness, or build command yet.

## Requirements now

- Git for reviewing changes.
- A Markdown editor or viewer for the SOW, technical design, ADR, and README.
- A GitHub account only if contributing through pull requests.

The documentation check in `.github/workflows/docs-ci.yml` runs on GitHub-hosted Ubuntu for pushes, pull requests, and manual dispatch. It checks that the required documents are present and nonempty and that tracked changes have no whitespace errors. A green result means **documentation checks passed**; it does not imply a database build or test passed.

## Proposed implementation environment

[ADR-001](ADR-001-Language-and-Filesystem.md) proposes stable Rust for the first implementation and 64-bit Linux on a local ext4 filesystem for durability validation. This is an added design decision, not a language or filesystem mandate from the SOW. The exact toolchain version and any dependencies should be pinned when Phase 1 code is added. Phase 1's local map can be developed without filesystem-specific features; Phase 2 must test the selected sync and rename behavior through both the real and simulated file layers.

The design's 10–12 week effort estimate assumes about 30 focused hours per week from one engineer already familiar with the language. Confirm the available calendar before treating it as a schedule. If time is constrained, retain the core failure tests and defer optional replica reads, advanced indexing, and speculative performance work.

## Repository layout

```text
README.md
LICENSE
.gitignore
.github/workflows/docs-ci.yml
docs/SOW.md
docs/Technical-Design.md
docs/ADR-001-Language-and-Filesystem.md
docs/Development-Setup.md
```

The SOW proposes future `src/`, `client/`, `tests/`, `benchmarks/results/`, `scripts/`, and `docker/` areas. These will be added when they contain real work. Keep published benchmark results tracked; the `.gitignore` excludes local runtime data, not `benchmarks/results/`.

## CI/CD policy

**CI now:** documentation presence and whitespace checks. Build, unit, integration, crash, and benchmark jobs will be added alongside their implementations, so a passing workflow cannot be mistaken for passing database tests.

**CD now:** no deployment or release workflow. The SOW defines a local three-node demonstration and Docker-based setup as future deliverables, but no deployable database artifact exists. A release workflow should be introduced only after an executable, versioning policy, and release checks exist. Do not publish packages or attach benchmark claims automatically from documentation-only commits.

## Contributions

Keep requirements traceable to the SOW and label new technical choices in the design or an ADR. Commit messages should describe the change and its verification.
