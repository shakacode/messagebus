# MessageBus - Claude Code Instructions

## Project Overview
MessageBus is an async message bus library for Rust, inspired by Actix. It provides various handler types (sync/async, batched/non-batched, synchronized/unsynchronized) for intercommunication between modules using message passing.

## Technology Stack
- **Language**: Rust (edition 2018)
- **Async Runtime**: Tokio 1.x with parking_lot, rt-multi-thread, sync, and time features
- **Key Dependencies**: async-trait, futures, parking_lot, thiserror, serde
- **Workspace Structure**: Main crate + workspace members in `crates/derive` and `crates/remote`

## Development Workflow

### Before Making Changes
- Always `git pull --rebase` before starting work on a branch
- Create descriptive branch names with `justin808/` prefix (e.g., `justin808/fix-deadlock`, `justin808/add-timeout-handler`)

### Testing Requirements
- **CRITICAL**: Always run `cargo test --workspace` before pushing commits
- Test all workspace members together - changes to `crates/derive` or `crates/remote` must be tested with main crate
- When modifying handlers or core bus logic, run relevant integration tests from `tests/` directory
- For performance-sensitive changes, consider running benchmarks from `examples/benchmark.rs`

### Code Quality Standards
- **CRITICAL**: Always run `cargo fmt --all` before committing to ensure consistent formatting
- **CRITICAL**: Run `cargo clippy --all -- -D warnings` and fix all warnings before pushing
- **CRITICAL**: Ensure `cargo build --verbose --all` passes without warnings
- CI runs `cargo fmt --all -- --check` and `cargo build --verbose --all` via Drone CI
- Never push code that fails local fmt-check or clippy

### Pre-Push Checklist
1. Run `cargo fmt --all` to auto-fix formatting
2. Run `cargo clippy --all -- -D warnings` and address all issues
3. Run `cargo test --workspace` to ensure all tests pass
4. Run `cargo build --verbose --all` to verify build succeeds
5. Review git diff to avoid extraneous changes

### Async/Concurrency Guidelines
- Follow Tokio best practices: use `spawn_blocking` for CPU-intensive or blocking operations
- Never block in async functions - use `tokio::task::spawn_blocking` for sync work
- Be mindful of deadlock potential in synchronized handlers
- Use parking_lot primitives for synchronization when needed
- Pay attention to idle detection and backpressure mechanisms

### API Stability & Breaking Changes
- **CRITICAL**: Never modify public trait signatures (Handler, AsyncHandler, etc.) without discussion
- Avoid removing or changing public APIs - this is a library used by others
- Use `#[deprecated]` warnings when phasing out functionality
- Document breaking changes clearly in commit messages
- Consider backwards compatibility for message types and handler traits

### File Organization
- Core logic in `src/`: builder.rs, handler.rs, receiver.rs, error.rs, etc.
- Receiver implementations in `src/receivers/`
- Integration tests in `tests/` (e.g., test_batch.rs, test_concurrency.rs, test_idle.rs)
- Examples in `examples/` directory for demonstrating features
- Workspace members: `crates/derive` (proc macros), `crates/remote` (remote bus support)

### Version Management
- Do NOT bump versions in Cargo.toml for regular PRs
- Version updates are maintainer responsibility for releases only
- Document changes in commit messages for future CHANGELOG updates

### Commit and PR Guidelines
- Prefer small, focused PRs that can be easily reviewed
- Write clear commit messages explaining why (not just what)
- Always end files with a newline character
- Run all quality checks before creating PR
- Use `gh pr create` to open PRs after pushing changes

### Common Commands
```bash
# Format code
cargo fmt --all

# Check formatting (CI check)
cargo fmt --all -- --check

# Run clippy
cargo clippy --all -- -D warnings

# Run tests (all workspace members)
cargo test --workspace

# Build everything
cargo build --verbose --all

# Run specific example
cargo run --example demo_async

# Run specific test
cargo test test_idle --workspace
```

### Code Patterns & Conventions
- Use `thiserror` for error types
- Handler traits define `Error` and `Response` associated types
- Messages must implement `Message` trait (Send + Sync + 'static)
- Use `TypeTag` for message type identification
- Prefer `Arc` for shared state in handlers
- Use `parking_lot` mutexes over std when appropriate

### Important Traits to Understand
- `Handler<M>` / `AsyncHandler<M>`: Stateless handlers (Send + Sync)
- `SynchronizedHandler<M>` / `AsyncSynchronizedHandler<M>`: Stateful handlers (Send only)
- `BatchHandler<M>` / `AsyncBatchHandler<M>`: Batch processing with InBatch/OutBatch types
- `Message`: Core trait for all messages (Send + Sync + 'static)

### CI Configuration
- Drone CI pipeline defined in `.drone.yml`
- Runs: `cargo build --verbose --all` and `cargo fmt --all -- --check`
- All CI checks must pass before merge

### When Stuck
- Review examples in `examples/` directory for usage patterns
- Check tests in `tests/` for edge cases and integration patterns
- Consult README.md for handler types and API overview
- Look at recent commits in CHANGELOG.md for context on changes
