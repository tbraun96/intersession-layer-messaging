# Intersession Layer Messaging (ISM)

[![Crates.io](https://img.shields.io/crates/v/intersession-layer-messaging.svg)](https://crates.io/crates/intersession-layer-messaging)
[![Documentation](https://docs.rs/intersession-layer-messaging/badge.svg)](https://docs.rs/intersession-layer-messaging)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A reliable messaging system that provides guaranteed message delivery and ordering across network sessions, with automatic recovery of pending messages when peers reconnect. ISM can be considered a "metasession" layer that sits on top of your existing network transport, ensuring that messages are reliably delivered and processed in the correct order even
if the underlying transport is unreliable or unordered.

Why do you need ISM if transports like TCP/QUIC are already reliable/ordered? While these transports are reliable and ordered within a single session, they do not provide guarantees across sessions. If a peer disconnects and reconnects, messages sent before the disconnection may be lost or delivered out of order. ISM solves this problem by persisting messages until they are successfully delivered and acknowledged, and by enforcing strict ordering of messages between peers.

## Features

- **Guaranteed Message Delivery**: Messages are persisted until successfully delivered and acknowledged
- **Message Ordering**: Strict ordering of messages between peers
- **Concurrent Processing**: Efficiently handles multiple peer connections simultaneously
- **Graceful Shutdown**: Clean shutdown with delivery of pending messages
- **Flexible Backend**: Pluggable backend storage system
- **Custom Network Layer**: Adaptable to different network implementations
- **Intersession Recovery**: Automatically recovers pending inbound/outbound messages when peers reconnect
- **Head of Line Blocking**: Ensures stability for applications that require strict message ordering

## Usage
ISM is designed to be flexible and easy to integrate into your existing network system. It provides a simple API for sending and receiving messages, and can be customized to work with your own network and storage systems.
### Basic Example

```rust
use intersession_layer_messaging::{MessageSystem, MessageMetadata, Network, Backend, LocalDelivery};
use async_trait::async_trait;

// Implement MessageMetadata for your message type
#[derive(Clone, Debug)]
struct MyMessage {
    source_id: u32,
    destination_id: u32,
    message_id: u32,
    data: Vec<u8>,
}

impl MessageMetadata for MyMessage {
    type PeerId = u32;
    type MessageId = u32;

    fn source_id(&self) -> Self::PeerId { self.source_id }
    fn destination_id(&self) -> Self::PeerId { self.destination_id }
    fn message_id(&self) -> Self::MessageId { self.message_id }
    fn contents(&self) -> &[u8] { &self.data }
    fn construct_from_parts(
        source_id: Self::PeerId,
        destination_id: Self::PeerId,
        message_id: Self::MessageId,
        contents: impl Into<Vec<u8>>,
    ) -> Self {
        MyMessage {
            source_id,
            destination_id,
            message_id,
            data: contents.into(),
        }
    }
}

#[tokio::main]
async fn main() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let network1 = network.add_peer(1).await;
    let network2 = network.add_peer(2).await;

    let backend1 = InMemoryBackend::<TestMessage>::default();
    let backend2 = InMemoryBackend::<TestMessage>::default();

    let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
    let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

    let message_system1 = MessageSystem::new(backend1, tx1, network1).await.unwrap();
    let message_system2 = MessageSystem::new(backend2, tx2, network2).await.unwrap();

    // Peer 1 sends a message to Peer 2
    let message1 = TestMessage {
        source_id: 1,
        destination_id: 2,
        message_id: 1,
        contents: vec![1, 2, 3],
    };
    
    message_system1.send_raw_message(message1).await.unwrap();

    // Peer 2 sends a message to Peer 1
    let message2 = TestMessage {
        source_id: 2,
        destination_id: 1,
        message_id: 2,
        contents: vec![4, 5, 6],
    };
    
    message_system2.send_raw_message(message2).await.unwrap();

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

