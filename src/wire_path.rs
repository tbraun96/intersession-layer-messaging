//! Every frame ILM sends goes through here, so every frame gets the extensions
//! negotiation allows for its peer and none that it does not.

use crate::frame::{InboundFrame, OutboundFrame};
use crate::local_delivery::LocalDelivery;
use crate::negotiation::{AckPlan, PIGGYBACK_ACK_WINDOW};
use crate::{
    platform_sleep, platform_spawn, Backend, MessageMetadata, NetworkError, Payload,
    UnderlyingSessionTransport, ILM,
};
use serde::{Deserialize, Serialize};

impl<M, B, L, N> ILM<M, B, L, N>
where
    M: MessageMetadata + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    B: Backend<M> + Send + Sync + 'static,
    L: LocalDelivery<M> + Send + Sync + 'static,
    N: UnderlyingSessionTransport<Message = M> + Send + Sync + 'static,
{
    /// A data frame, carrying whatever ACK is waiting for its peer.
    pub(crate) async fn send_data(&self, message: M) -> Result<(), NetworkError<OutboundFrame<M>>> {
        let peer = message.destination_id();
        let extensions = self
            .negotiation
            .message_extensions(peer, message.message_id());
        let carried_ack = match extensions {
            crate::frame::FrameExtensions::Negotiated {
                piggybacked_ack, ..
            } => piggybacked_ack,
            _ => None,
        };
        let result = self
            .send_message_internal(OutboundFrame {
                payload: Payload::Message(message),
                extensions,
            })
            .await;
        if let (Err(_), Some(ack)) = (&result, carried_ack) {
            // The frame never left, so neither did the ACK riding on it.
            if self.negotiation.restore_ack(peer, ack) {
                self.schedule_ack_flush(peer);
            }
        }
        result
    }

    /// An Ack or Poll, advertising this node's capabilities if it has any.
    pub(crate) async fn send_control(
        &self,
        payload: Payload<M>,
    ) -> Result<(), NetworkError<OutboundFrame<M>>> {
        self.send_message_internal(OutboundFrame {
            payload,
            extensions: self.negotiation.control_extensions(),
        })
        .await
    }

    /// Acknowledge `original` -- now, or on the next frame to its sender.
    pub(crate) async fn acknowledge(&self, original: &M) {
        let peer = original.source_id();
        let message_id = original.message_id();
        let ack_id = match self.negotiation.defer_ack(peer, message_id) {
            AckPlan::SendNow => message_id,
            AckPlan::Release(highest) => highest,
            AckPlan::Deferred => {
                self.schedule_ack_flush(peer);
                return;
            }
            AckPlan::Merged => return,
        };
        if let Err(e) = self.send_ack(peer, ack_id).await {
            log::error!(target: "ism", "[ILM-ACK] FAILED to send ACK for msg_id={ack_id} to peer {peer}: {e:?}");
        }
    }

    async fn send_ack(
        &self,
        peer: M::PeerId,
        message_id: M::MessageId,
    ) -> Result<(), NetworkError<OutboundFrame<M>>> {
        self.send_control(Payload::Ack {
            from_id: self.network.local_id(),
            to_id: peer,
            message_id,
        })
        .await
    }

    /// Send the peer's waiting ACK alone once the window passes, unless a data
    /// frame took it first.
    pub(crate) fn schedule_ack_flush(&self, peer: M::PeerId) {
        let this = self.clone_internal();
        platform_spawn(async move {
            platform_sleep(PIGGYBACK_ACK_WINDOW).await;
            if !this.can_run() {
                return;
            }
            if let Some(ack_id) = this.negotiation.take_ack(&peer) {
                log::debug!(target: "ism", "[ILM-ACK] nothing outbound to peer {peer} within {PIGGYBACK_ACK_WINDOW:?}; sending ACK {ack_id} alone");
                if let Err(e) = this.send_ack(peer, ack_id).await {
                    log::error!(target: "ism", "[ILM-ACK] FAILED to flush ACK {ack_id} to peer {peer}: {e:?}");
                }
            }
        });
    }

    /// Learn from the framing, and apply any ACK folded into it, before the
    /// payload itself is handled.
    pub(crate) async fn absorb_extensions(&self, frame: &InboundFrame<M>) {
        let sender = frame.payload.source_id();
        self.negotiation.observe(sender, frame.evidence);
        if let Some(acked) = frame.piggybacked_ack {
            self.handle_ack(sender, frame.payload.destination_id(), acked)
                .await;
        }
    }
}
