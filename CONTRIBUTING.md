# Contributing to `pg_auto_dw`

## Releases

`pg_auto_dw` follows [semantic versioning](semver.org) and is released to [pgt.dev](https://pgt.dev/extensions/pg_auto_dw).

To release, follow these steps:

1. Create a PR updating the version in `Cargo.toml` and `Trunk.toml`. These two values must agree.
2. Merge the PR into the `main` branch.
3. [Create the release](https://github.com/tembo-io/pg_auto_dw/releases/new)
   1. Use the tag format `vX.Y.Z` where `X.Y.Z` is the version number. e.g. `v0.1.0`. This version should be the same value as in `Cargo.toml` and `Trunk.toml`.
   2. Click "Generated release notes" to auto-populate the release notes or fill in with your own content and notes.
   3. Click "Publish release"
