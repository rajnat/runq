#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-dev}"
OUT_DIR="dist/runq/${VERSION}"
mkdir -p "${OUT_DIR}"

platforms=(
  "darwin amd64"
  "darwin arm64"
  "linux amd64"
  "linux arm64"
)

for platform in "${platforms[@]}"; do
  GOOS="${platform%% *}"
  GOARCH="${platform##* }"
  BIN_NAME="runq-${VERSION}-${GOOS}-${GOARCH}"
  TARGET_DIR="${OUT_DIR}/${GOOS}-${GOARCH}"
  mkdir -p "${TARGET_DIR}"
  echo "building ${BIN_NAME}"
  GOOS="${GOOS}" GOARCH="${GOARCH}" CGO_ENABLED=0 go build -o "${TARGET_DIR}/runq" ./cmd/runq
  tar -czf "${OUT_DIR}/${BIN_NAME}.tar.gz" -C "${TARGET_DIR}" runq
  shasum -a 256 "${OUT_DIR}/${BIN_NAME}.tar.gz" >> "${OUT_DIR}/SHA256SUMS"
done

echo "artifacts written to ${OUT_DIR}"
