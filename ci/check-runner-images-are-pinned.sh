#!/bin/sh
# No workflow job runs on `ubuntu-latest`; every hosted Linux runner names its release.
#
# GitHub moves `ubuntu-latest` on its own schedule. On 2026-11-15 it moved to 26.04 and every
# browser job across the downstream repositories went red at once with no change of ours. A named
# release (`ubuntu-24.04`) moves only when this repository moves it. The downstream repositories
# carry the same rule; this is its copy for the workflows here.
#
#   sh ci/check-runner-images-are-pinned.sh            judges .github/workflows
#   sh ci/check-runner-images-are-pinned.sh <dir>      judges another directory (the self-test)
set -eu
dir="${1:-.github/workflows}"
[ -d "$dir" ] || { echo "error: $dir does not exist; this check measured nothing" >&2; exit 1; }
files=0
bad=0
for f in "$dir"/*.yml "$dir"/*.yaml; do
  [ -f "$f" ] || continue
  files=$((files + 1))
  # Comments are not judged; a line that names the floating image is.
  if grep -n 'ubuntu-latest' "$f" | grep -v '^[0-9]*:[[:space:]]*#' ; then
    bad=$((bad + 1))
  fi
done
[ "$files" -gt 0 ] || { echo "error: no workflow files in $dir; this check measured nothing" >&2; exit 1; }
if [ "$bad" -gt 0 ]; then
  echo "error: $bad workflow(s) above run on ubuntu-latest; name the release (ubuntu-24.04)" >&2
  exit 1
fi
echo "ok: $files workflow(s) name their Linux runner release"
