use crate::{DeliveryError, MessageMetadata};
use async_trait::async_trait;

#[async_trait]
pub trait LocalDelivery<M: MessageMetadata> {
    async fn deliver(&self, message: M) -> Result<(), DeliveryError>;
}

#[async_trait]
impl<M: MessageMetadata> LocalDelivery<M> for tokio::sync::mpsc::UnboundedSender<M> {
    async fn deliver(&self, message: M) -> Result<(), DeliveryError> {
        self.send(message).map_err(|_| DeliveryError::ChannelClosed)
    }
}
