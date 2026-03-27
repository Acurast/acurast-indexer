//! Phase work item types for the indexing pipeline.

use async_channel::{Receiver, Sender};

use crate::entities::{EpochRow, EventRow, ExtrinsicRow};

/// Work item for phase-based indexing pipeline.
#[derive(Clone)]
pub enum PhaseWorkItem {
    Extrinsic(ExtrinsicRow),
    Event(EventRow),
    Epoch(EpochRow),
}

/// Senders for phase work items, split by priority.
/// Priority order: Epoch > Event > Extrinsic
#[derive(Clone)]
pub struct PhaseWorkSenders {
    pub epoch: Sender<EpochRow>,
    pub event: Sender<EventRow>,
    pub extrinsic: Sender<ExtrinsicRow>,
}

impl PhaseWorkSenders {
    /// Send an epoch work item (highest priority).
    pub async fn send_epoch(
        &self,
        epoch: EpochRow,
    ) -> Result<(), async_channel::SendError<EpochRow>> {
        self.epoch.send(epoch).await
    }

    /// Send an event work item (medium priority).
    pub async fn send_event(
        &self,
        event: EventRow,
    ) -> Result<(), async_channel::SendError<EventRow>> {
        self.event.send(event).await
    }

    /// Send an extrinsic work item (lowest priority).
    pub async fn send_extrinsic(
        &self,
        extrinsic: ExtrinsicRow,
    ) -> Result<(), async_channel::SendError<ExtrinsicRow>> {
        self.extrinsic.send(extrinsic).await
    }
}

/// Receivers for phase work items, with priority-aware receiving.
/// Priority order: Epoch > Event > Extrinsic
#[derive(Clone)]
pub struct PhaseWorkReceivers {
    pub epoch: Receiver<EpochRow>,
    pub event: Receiver<EventRow>,
    pub extrinsic: Receiver<ExtrinsicRow>,
}

impl PhaseWorkReceivers {
    /// Receive the next work item, prioritizing epochs over events over extrinsics.
    /// Returns None if all channels are closed.
    pub async fn recv(&self) -> Option<PhaseWorkItem> {
        // First, try to get from higher priority queues without blocking
        if let Ok(epoch) = self.epoch.try_recv() {
            return Some(PhaseWorkItem::Epoch(epoch));
        }
        if let Ok(event) = self.event.try_recv() {
            return Some(PhaseWorkItem::Event(event));
        }
        if let Ok(extrinsic) = self.extrinsic.try_recv() {
            return Some(PhaseWorkItem::Extrinsic(extrinsic));
        }

        // All queues empty, block until something arrives (with priority)
        tokio::select! {
            biased;
            result = self.epoch.recv() => result.ok().map(PhaseWorkItem::Epoch),
            result = self.event.recv() => result.ok().map(PhaseWorkItem::Event),
            result = self.extrinsic.recv() => result.ok().map(PhaseWorkItem::Extrinsic),
        }
    }

    /// Check if all channels are closed and empty.
    pub fn is_closed(&self) -> bool {
        self.epoch.is_closed() && self.event.is_closed() && self.extrinsic.is_closed()
    }
}

/// Create a new set of phase work queues with unbounded channels.
pub fn phase_work_queues() -> (PhaseWorkSenders, PhaseWorkReceivers) {
    let (tx_epoch, rx_epoch) = async_channel::unbounded();
    let (tx_event, rx_event) = async_channel::unbounded();
    let (tx_extrinsic, rx_extrinsic) = async_channel::unbounded();

    (
        PhaseWorkSenders {
            epoch: tx_epoch,
            event: tx_event,
            extrinsic: tx_extrinsic,
        },
        PhaseWorkReceivers {
            epoch: rx_epoch,
            event: rx_event,
            extrinsic: rx_extrinsic,
        },
    )
}
