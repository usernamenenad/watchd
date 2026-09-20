# Contributing to watchd

Thank you for helping build `watchd`. The project is pre-alpha: its APIs and storage contracts may change, and correctness work takes priority over compatibility until the first stable release.

## Before starting

1. Read the [architecture tour](docs/architecture.md) and [v0 semantics](docs/semantics.md).
2. Choose an open issue with a clear definition of done. Comment before starting substantial work so efforts do not overlap.
3. For behavior or scope not covered by an issue, open one and agree on the contract before implementing it.

No existing issue is currently labelled `good first issue`: the open work still touches correctness-sensitive foundations. We will use that label only for bounded tasks with enough context for a new contributor to complete independently.

## Local setup

Requirements:

- Go version declared in `go.mod`
- Docker with Compose
- Git

Start PostgreSQL and run all current checks:

```bash
make fmt-check
make vet
make postgres-up
make test
make integration
make vulncheck   # requires golang.org/x/vuln/cmd/govulncheck
```

Reset the disposable local database with `make postgres-down`. This removes the development volume.

## Continuous integration

CI (`.github/workflows/ci.yml`) runs the same `make` targets used locally, so a
clean checkout behaves the same in both places:

- `fmt-check` and `vet` gate every push and pull request.
- Unit tests run with the race detector and publish a coverage report; the
  percentage is informational, not a pass/fail gate.
- Integration tests run against `postgres:15-alpine`, `postgres:16-alpine`,
  and `postgres:17-alpine` on Linux amd64, matching this project's
  currently-supported PostgreSQL range. Add a version to the matrix in
  `ci.yml` when the project commits to supporting it, and drop one once it is
  end-of-life upstream. Additional platforms (architectures, OSes) are out of
  scope until a concrete deployment target needs them.
- `govulncheck` scans for known vulnerabilities on every push and pull
  request.

Required checks are enabled on `main` and `develop` once these workflows have
proven stable.

## Branches

- `develop` is the default integration branch. Open normal pull requests against it.
- `main` is the release branch. Promote tested changes from `develop` through a pull request.
- Both branches are protected and use squash merges. Do not build work directly on either branch.

Create a topic branch from the latest `develop`:

```bash
git switch develop
git pull --ff-only
git switch -c issue-123-short-description
```

## Commit policy

Every non-merge commit must:

- be signed off under the [Developer Certificate of Origin](DCO);
- reference at least one GitHub issue using `Refs #123`, `Relates-to #123`, or `Closes #123`;
- be focused enough to review and revert independently.

Create a compliant commit with:

```bash
git commit -s -m "feat(cdc): handle keepalive messages" -m "Refs #1"
```

Use `Closes #123` only when the commit completes the issue. CI validates every commit in a pull request and every commit pushed to `main`.

## Pull requests

- Keep changes scoped to the linked issue.
- Add tests proportional to correctness and failure risk.
- Update user-facing and architecture documentation when contracts change.
- Describe operational, security, compatibility, and resource-limit effects.
- Complete the pull-request checklist.

Maintainers may ask for a design discussion before accepting changes that modify the source cursor, snapshot boundary, progress, resync, transaction atomicity, security boundary, or public API.

## Review and merge

The project currently uses a maintainer-led model described in [GOVERNANCE.md](GOVERNANCE.md). Approval does not guarantee immediate merge: correctness-sensitive changes may require fault tests or a documented invariant first.

Pull requests are squash-merged. The final commit must retain the issue reference and sign-off. A promotion from `develop` to `main` must reference its release or promotion issue.

By contributing, you agree that your contribution is licensed under [Apache-2.0](LICENSE) and certify it under the [DCO](DCO).
