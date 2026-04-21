# CLI release flow

runq now includes a simple packaging flow for the standalone CLI.

## Build release artifacts

```bash
make package-cli VERSION=v0.1.0
```

This runs:
- `scripts/package_runq_cli.sh`

Artifacts are written under:
- `dist/runq/<version>/`

Current packaged targets:
- darwin/amd64
- darwin/arm64
- linux/amd64
- linux/arm64

For each target, the flow produces:
- a `runq` binary
- a `.tar.gz` archive
- a `SHA256SUMS` manifest

## Notes

- builds use `CGO_ENABLED=0` for portable binaries
- the current flow is intentionally simple and local-first
- this is a packaging baseline and can later be replaced by Goreleaser or CI-driven release automation
