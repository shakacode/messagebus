# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MessageBus is an async message bus library for Rust, inspired by Actix. It enables actor-style communication between components using typed messages routed through receivers (queue implementations).

## Build Commands

```bash
# Build the project
cargo build

# Run tests
cargo test

# Run a single test
cargo test <test_name>

# Run examples
cargo run --example demo_async
cargo run --example demo_sync_batch
cargo run --example benchmark

# Check for lints
cargo clippy

# Format code
cargo fmt
```

## Workspace Structure

This is a Cargo workspace with three crates:

- **messagebus** (root) - Core message bus implementation
- **crates/derive** (`messagebus_derive`) - Proc-macro derive implementations for `Message` and `Error` traits
- **crates/remote** (`messagebus_remote`) - Remote communication support using QUIC/Redis

## Architecture

### Core Concepts

1. **Bus** - Central message dispatcher that routes messages to receivers based on TypeId
2. **Message** - Trait for types that can be sent through the bus (derive with `#[derive(Message)]`)
3. **Handlers** - Traits implemented by receivers to process messages
4. **Receivers** - Queue implementations that manage message delivery

### Handler Types

The library provides 12 handler traits for different use cases:

#### Thread-Safe Handlers (Send+Sync)

| Handler Type | Batched | Async |
|--------------|---------|-------|
| `Handler` | No | No |
| `AsyncHandler` | No | Yes |
| `BatchHandler` | Yes | No |
| `AsyncBatchHandler` | Yes | Yes |

#### Synchronized Handlers (Send only)

| Handler Type | Batched | Async |
|--------------|---------|-------|
| `SynchronizedHandler` | No | No |
| `AsyncSynchronizedHandler` | No | Yes |
| `BatchSynchronizedHandler` | Yes | No |
| `AsyncBatchSynchronizedHandler` | Yes | Yes |

#### Local Handlers (no Send/Sync)

| Handler Type | Batched | Async |
|--------------|---------|-------|
| `LocalHandler` | No | No |
| `LocalAsyncHandler` | No | Yes |
| `LocalBatchHandler` | Yes | No |
| `LocalAsyncBatchHandler` | Yes | Yes |

### Receiver Types (in `src/receivers/`)

- **BufferUnorderedAsync/Sync** - Concurrent message processing
- **BufferUnorderedBatchedAsync/Sync** - Batched concurrent processing
- **SynchronizedAsync/Sync** - Sequential message processing with mutable handler
- **SynchronizedBatchedAsync/Sync** - Batched sequential processing

### Message Derive Macro

Use `#[derive(Message)]` with attributes:
- `#[message(clone)]` - Enable message cloning for broadcast
- `#[message(shared)]` - Enable serialization for remote transport
- `#[type_tag("custom::name")]` - Custom type tag
- `#[namespace("my_namespace")]` - Type tag namespace prefix

### SendOptions

Messages can be sent with different routing strategies:
- `Broadcast` - Send to all matching receivers (default)
- `Direct(id)` - Send to specific receiver
- `Except(id)` - Send to all except specified receiver
- `Random` / `Balanced` - Load distribution strategies

### Key Source Files

- `src/lib.rs` - Bus implementation and public API
- `src/handler.rs` - Handler trait definitions
- `src/receiver.rs` - Receiver trait and implementation
- `src/builder.rs` - BusBuilder for registration
- `src/envelop.rs` - Message trait and TypeTagged
