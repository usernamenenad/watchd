#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <git-revision-range>" >&2
  exit 2
fi

revision_range=$1
failed=0

while IFS= read -r commit_sha; do
  [[ -z "$commit_sha" ]] && continue

  commit_message=$(git show --no-patch --format=%B "$commit_sha")
  short_sha=$(git rev-parse --short "$commit_sha")

  if ! grep -Eq '^Signed-off-by: .+ <[^>]+>$' <<<"$commit_message"; then
    echo "$short_sha: missing a valid Signed-off-by trailer" >&2
    failed=1
  fi

  if ! grep -Eiq '(^|[[:space:]])((close[sd]?|fix(e[sd])?|resolve[sd]?|refs?|relates-to)[[:space:]:]+)?#[0-9]+([[:space:][:punct:]]|$)|https://github\.com/[^/]+/[^/]+/issues/[0-9]+' <<<"$commit_message"; then
    echo "$short_sha: missing a GitHub issue reference such as Refs #123" >&2
    failed=1
  fi
done < <(git rev-list --reverse "$revision_range")

if [[ $failed -ne 0 ]]; then
  echo "commit policy failed; see CONTRIBUTING.md" >&2
  exit 1
fi
