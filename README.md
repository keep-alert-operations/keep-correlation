# keep-correlation

Tenant-ordered Kafka correlation processing and automatic incident orchestration.

See [system design](https://github.com/keep-alert-operations/keep-design), [workspace setup](https://github.com/keep-alert-operations/keep-delivery) and [source provenance](SOURCE.md).

## Development

Use Python 3.13, Poetry 2.3.1 and the committed `poetry.lock`. Configure Git authentication for the
private organization repositories before resolving internal dependencies.

```powershell
poetry check --lock
poetry sync --all-extras
poetry run ruff check src tests
poetry run pytest
poetry build --format wheel
```

Shared `keep` modules use namespace packaging; each module has exactly one owning
distribution. Dependencies resolve immutable Git revisions in Python dependency metadata and `poetry.lock`.
For coordinated local edits use the delivery workspace setup, which installs local
packages without changing release dependency pins.

## Execution

`poetry run keep-correlation` is the entrypoint. Supply the documented environment
bindings; package installation does not start services or migrate data.
