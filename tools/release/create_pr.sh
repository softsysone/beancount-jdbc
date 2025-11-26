#!/usr/bin/env bash
set -euo pipefail

# Create and push a release branch based on Version.java and the first matching
# entry in CHANGELOG.md. Prints a compare URL for opening a PR.

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

# Extract version from Version.java (MAJOR.MINOR.PATCH[-QUALIFIER])
version=$(
  python - <<'PY'
import re, pathlib
txt = pathlib.Path("jdbc/src/main/java/com/beancount/jdbc/Version.java").read_text()
def val(name):
    m = re.search(rf'\b{name}\s*=\s*"?(.*?)"?;', txt)
    if not m:
        raise SystemExit(f"missing {name}")
    return m.group(1)
ver = f"{val('MAJOR')}.{val('MINOR')}.{val('PATCH')}"
qual = val('QUALIFIER')
print(f"{ver}-{qual}" if qual else ver)
PY
)

# Grab the matching CHANGELOG heading (first line that starts with ## and contains the version)
changelog_line=$(grep -m1 -E "^## .*${version}" CHANGELOG.md || true)
changelog_title=${changelog_line### }
commit_msg="Release ${version}"
[ -n "$changelog_title" ] && commit_msg="${commit_msg}: ${changelog_title#${version} }"

branch="release/${version}"

echo "Version:        ${version}"
echo "Changelog line: ${changelog_title:-<not found>}"
echo "Branch:         ${branch}"
echo "Commit message: ${commit_msg}"
echo

# Create/switch branch
if git rev-parse --verify "$branch" >/dev/null 2>&1; then
  git switch "$branch"
else
  git switch -c "$branch"
fi

# Stage, commit, and push
git add -A
git commit -m "$commit_msg"
git push -u origin "$branch"

echo
origin_url=$(git remote get-url origin)
echo "Pushed ${branch}."
echo "Open PR: ${origin_url%.*}/compare/${branch}?expand=1"
