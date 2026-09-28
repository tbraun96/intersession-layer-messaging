# 📨 Intersession Layer Messaging (ILM)

[![Crates.io](https://img.shields.io/crates/v/intersession-layer-messaging.svg)](https://crates.io/crates/intersession-layer-messaging)
[![Documentation](https://docs.rs/intersession-layer-messaging/badge.svg)](https://docs.rs/intersession-layer-messaging)
[![Validate PR](https://github.com/tbraun96/intersession-layer-messaging/actions/workflows/validate.yml/badge.svg)](https://github.com/tbraun96/intersession-layer-messaging/actions/workflows/validate.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 📋 Table of Contents

- [📨 Intersession Layer Messaging (ILM)](#-intersession-layer-messaging-ilm)
  - [📋 Table of Contents](#-table-of-contents)
  - [🎯 Overview](#-overview)
  - [✨ Features](#-features)
  - [🚀 Usage](#-usage)
    - [Basic Example](#basic-example)
  - [🧪 Testing](#-testing)
    - [Standard Testing](#standard-testing)
    - [🌐 WebAssembly (WASM) Testing](#-webassembly-wasm-testing)
      - [Prerequisites](#prerequisites)
      - [Setup Instructions](#setup-instructions)
      - [Building and Running Tests](#building-and-running-tests)
      - [Troubleshooting](#troubleshooting)

## 🎯 Overview

A reliable messaging system that provides guaranteed message delivery and ordering across network sessions, with automatic recovery of pending messages when peers reconnect. ILM can be considered a "metasession" layer that sits on top of your existing network transport, ensuring that messages are reliably delivered and processed in the correct order even
if the underlying transport is unreliable or unordered.

Why do you need ILM if transports like TCP/QUIC are already reliable/ordered? While these transports are reliable and ordered within a single session, they do not provide guarantees across sessions. If a peer disconnects and reconnects, messages sent before the disconnection may be lost or delivered out of order. ILM solves this problem by persisting messages until they are successfully delivered and acknowledged, and by enforcing strict ordering of messages between peers.

## ✨ Features

- **Guaranteed Message Delivery**: Messages are persisted until successfully delivered and acknowledged
- **Message Ordering**: Strict ordering of messages between peers
- **Concurrent Processing**: Efficiently handles multiple peer connections simultaneously
- **Graceful Shutdown**: Clean shutdown with delivery of pending messages
- **Flexible Backend**: Pluggable backend storage system
- **Custom Network Layer**: Adaptable to different network implementations
- **Intersession Recovery**: Automatically recovers pending inbound/outbound messages when peers reconnect
- **Head of Line Blocking**: Ensures stability for applications that require strict message ordering

## 🚀 Usage
ILM is designed to be flexible and easy to integrate into your existing network system. It provides a simple API for sending and receiving messages, and can be customized to work with your own network and storage systems.
Below is an example of using the `TestMessage` type, which already implements `MessageMetadata`
### Basic Example

```rust
use intersession_layer_messaging::{ILM, IlmOptions, MessageMetadata, Network, Backend, LocalDelivery, testing::*};

#[tokio::main]
async fn main() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let network1 = network.add_peer(1).await;
    let network2 = network.add_peer(2).await;

    let backend1 = InMemoryBackend::<TestMessage>::default();
    let backend2 = InMemoryBackend::<TestMessage>::default();

    let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
    let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

    // Every construction site says which wire extensions it wants; LEGACY is
    // the original protocol, byte for byte.
    let messenger1 = ILM::new(backend1, tx1, network1, IlmOptions::LEGACY).await.unwrap();
    let messenger2 = ILM::new(backend2, tx2, network2, IlmOptions::LEGACY).await.unwrap();

    // Peer 1 sends a message to Peer 2
    messenger1.send_to(2, vec![1, 2, 3]).await.unwrap();

    // Peer 2 sends a message to Peer 1
    messenger2.send_to(1, vec![4, 5, 6]).await.unwrap();

    // Peer 1 receives the message from Peer 2
    let received_message1 = rx1.recv().await.unwrap();
    assert_eq!(received_message1.source_id(), 2);
    assert_eq!(received_message1.destination_id(), 1);
    assert_eq!(received_message1.contents(), &[4, 5, 6]);

    // Peer 2 receives the message from Peer 1
    let received_message2 = rx2.recv().await.unwrap();
    assert_eq!(received_message2.source_id(), 1);
    assert_eq!(received_message2.destination_id(), 2);
    assert_eq!(received_message2.contents(), &[1, 2, 3]);
}
```

### Wire extensions: piggybacked ACKs and compression

`IlmOptions` has no default; each construction site chooses:

```rust
let options = IlmOptions {
    // Fold the cumulative ACK into the next data frame to the same peer; a
    // standalone ACK still goes within PIGGYBACK_ACK_WINDOW (40 ms), and at
    // once after PIGGYBACK_ACK_EVERY (4) deliveries.
    piggyback_acks: true,
    // Disabled, or (with the matching cargo features) Brotli, Deflate, Zstd, All.
    dynamic_compression: DynamicCompression::All,
};
```

Both are used toward a peer only after that peer has advertised them. Every
Ack and Poll carries this node's `PeerCapabilities`; a peer whose control
frames carry none is a legacy peer, and every frame to it is the legacy frame.
Compression also needs a per-message `CompressionHint`
(`send_to_with_hint`); no hint means no compression.

Every codec sits behind one pure trait (`FrameCodec`: `compress(&[u8],
&Params)` / `decompress(&[u8], cap)`), registered under a permanent wire id:

| id | codec    | feature                                   |
|----|----------|-------------------------------------------|
| 0  | identity | always                                    |
| 1  | brotli   | `compression-brotli`                      |
| 2  | deflate  | `compression-deflate` (raw deflate)       |
| 3  | rill     | reserved (small-frame dictionary codec)   |
| 4  | zstd     | `compression-zstd` (zstd-rs, magicless)   |

Features are off by default; every implemented codec is pure Rust and builds
for `wasm32-unknown-unknown`. The browser build uses zstd + deflate: brotli
adds ~1 MB to a wasm module, zstd-rs ~64 KB gzipped. Peers advertise the whole SET of codec ids they
decode, so a peer with some codecs and not others is served what it has.

Which codec a frame gets is one table, `compression::policy(hint, len,
available)`: small (< 1 KiB) structured frames prefer rill, then brotli q4,
then deflate l1, then zstd (which, without a dictionary, loses to both below
1 KiB); large JSON, text and Yjs prefer zstd level 3, then brotli q4, then
deflate; CBOR commands wait for rill; `Opaque` and no hint are identity. A
codec is skipped if it is not available (compiled, enabled and advertised by
the peer) or the frame is below its floor (128 B for brotli/deflate/zstd), and a
frame the chosen codec would not shrink is sent raw. A receiver refuses
anything that expands past 16 MiB. Adding rill is a registry entry and a
feature, not a wire change.

The transport encodes what ILM decides: `send_message` takes an
`OutboundFrame` (payload plus `FrameExtensions`) and `next_message` yields an
`InboundFrame` (payload, `CapabilityEvidence`, any piggybacked ACK).

## 🧪 Testing

### Standard Testing

To run the standard test suite:

```bash
cargo test --features=testing
```

### 🌐 WebAssembly (WASM) Testing

This crate supports WebAssembly targets including `wasm32-wasip2`. The testing infrastructure has been adapted to work in WASM environments where filesystem operations are not available.

#### Prerequisites

Before running WASM tests, you need to install the following tools:

1. **🦀 Rust with WASM target**
   ```bash
   rustup target add wasm32-wasip2
   ```

2. **⚡ Wasmtime** - WebAssembly runtime
   ```bash
   # macOS
   brew install wasmtime
   
   # Linux/Windows
   curl https://wasmtime.dev/install.sh -sSf | bash
   ```

3. **🛠️ WASI SDK** - Required for building WASM modules
   
   Download the appropriate SDK for your platform from [WASI SDK Releases](https://github.com/WebAssembly/wasi-sdk/releases):
   
   ```bash
   # Example for macOS ARM64
   curl -L https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-25/wasi-sdk-25.0-arm64-macos.tar.gz | tar xz
   ```

#### Setup Instructions

1. **Set the WASI SDK path** (required for building):
   ```bash
   export WASI_SDK_PATH=/path/to/wasi-sdk-25.0-arm64-macos
   ```

2. **Verify your setup**:
   ```bash
   # Check Rust WASM target
   rustup target list | grep wasm32-wasip2
   
   # Check Wasmtime
   wasmtime --version
   
   # Check WASI SDK
   ls $WASI_SDK_PATH/bin/clang
   ```

#### Building and Running Tests

1. **Build the tests** for WASM target:
   ```bash
   WASI_SDK_PATH=/path/to/wasi-sdk cargo test \
     --package intersession-layer-messaging \
     --target=wasm32-wasip2 \
     --features=testing \
     --no-run
   ```

2. **Find the test binary**:
   ```bash
   # The test binary will be in the target directory
   ls target/wasm32-wasip2/debug/deps/intersession_layer_messaging-*.wasm
   ```

3. **Run the tests** with Wasmtime:
   ```bash
   wasmtime target/wasm32-wasip2/debug/deps/intersession_layer_messaging-*.wasm
   ```

#### Troubleshooting

- **❌ Permission Denied Error**: If you see "Permission denied (os error 13)" when cargo tries to run the test, this is expected. WASM files cannot be executed directly. Use `wasmtime` to run them instead.

- **❌ Filesystem Errors**: The test implementation uses conditional compilation to provide in-memory storage for WASM targets instead of filesystem operations.

- **❌ Missing WASI SDK**: If you get errors about missing `clang` or linking failures, ensure your `WASI_SDK_PATH` is correctly set and points to a valid WASI SDK installation.

### 📝 Implementation Notes

The test infrastructure (`src/testing.rs`) includes:

- **`InMemoryBackend`**: A backend implementation that uses in-memory storage for WASM targets and filesystem storage for other targets
- **`InMemoryNetwork`**: A simulated network transport using channels
- **Comprehensive test suite**: Tests covering message ordering, persistence, recovery, and error handling

All tests are designed to work seamlessly across both standard and WASM environments.

