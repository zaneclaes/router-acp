#!/usr/bin/env bash
set -euo pipefail

RELEASE_REPO="${DEV_WORKSTATION_RELEASE_REPO:-zaneclaes/router-acp}"
TARGET="x86_64-unknown-linux-gnu"
BINARY="router-acp"

root="$(git rev-parse --show-toplevel)"
cd "$root"

if [ -n "$(git status --porcelain)" ]; then
  echo "release-dev-workstation: the working tree must be clean" >&2
  exit 1
fi

rev="$(git rev-parse HEAD)"
if ! git branch -r --contains "$rev" | grep -q .; then
  echo "release-dev-workstation: ${rev} is not present on a fetched remote branch" >&2
  echo "Push the commit before publishing its binary." >&2
  exit 1
fi

if [ "$(uname -s)" != Linux ] || [ "$(uname -m)" != x86_64 ]; then
  echo "release-dev-workstation: Linux x86_64 is the only supported build host" >&2
  exit 1
fi

cargo build --locked --release --bin "$BINARY"
"target/release/${BINARY}" --version

tmp="$(mktemp -d "${TMPDIR:-/tmp}/router-acp-release.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT
package_dir="${tmp}/package"
asset="${BINARY}-${TARGET}.tar.gz"
checksum="${asset}.sha256"
mkdir -p "$package_dir"
install -m 0755 "target/release/${BINARY}" "${package_dir}/${BINARY}"
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
  -cf - -C "$package_dir" "$BINARY" | gzip -n >"${tmp}/${asset}"
(
  cd "$tmp"
  sha256sum "$asset" >"$checksum"
)

tag="dev-workstation-${rev}"
if gh release view "$tag" --repo "$RELEASE_REPO" >/dev/null 2>&1; then
  existing="${tmp}/existing"
  mkdir -p "$existing"
  gh release download "$tag" --repo "$RELEASE_REPO" \
    --pattern "$asset" --pattern "$checksum" --dir "$existing"
  if ! cmp -s "${tmp}/${checksum}" "${existing}/${checksum}"; then
    echo "release-dev-workstation: ${tag} already exists with a different checksum" >&2
    exit 1
  fi
  (cd "$existing" && sha256sum --check "$checksum")
  echo "${tag} already contains the expected ${asset}."
else
  gh release create "$tag" \
    "${tmp}/${asset}" \
    "${tmp}/${checksum}" \
    --repo "$RELEASE_REPO" \
    --target "$rev" \
    --prerelease \
    --title "Dev workstation ${rev}" \
    --notes "Pinned Linux x86-64 binary for Hickory dev workstations."
fi

printf 'revision=%s\nasset=%s\nsha256=%s\n' \
  "$rev" "$asset" "$(awk '{print $1}' "${tmp}/${checksum}")"
