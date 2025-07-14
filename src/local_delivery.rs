use crate::{DeliveryError, MessageMetadata};
use async_trait::async_trait;
use auto_impl::auto_impl;

#[async_trait]
#[auto_impl(Box, Arc)]
pub trait LocalDelivery<M: MessageMetadata> {
    async fn deliver(&self, message: M) -> Result<(), DeliveryError>;
}

#[async_trait]
impl<M: MessageMetadata> LocalDelivery<M> for citadel_io::tokio::sync::mpsc::UnboundedSender<M> {
    async fn deliver(&self, message: M) -> Result<(), DeliveryError> {
        self.send(message).map_err(|_| DeliveryError::ChannelClosed)
    }
}
