use metrics::Counter;
use reth_metrics_derive::Metrics;
use reth_primitives::{NodeRecord, PeerId};
use std::{
    collections::HashMap,
    task::{Context, Poll},
};
use tokio::time::Interval;

/// Metrics for the discv4 service
#[derive(Default)]
pub struct Discv4Metrics {
    /// Metrics for bootnodes, indexed by their node record
    pub(crate) bootnode_metrics: HashMap<NodeRecord, BootnodeMetrics>,
    /// A map of bootstrap node IDs to their node records
    pub(crate) bootnodes: HashMap<PeerId, NodeRecord>,
    /// Whether or not the discv4 service should check that bootnodes are online on an interval
    pub(crate) check_online: CheckOnline,
}

/// Whether or not the discv4 service should check that bootnodes are online on an interval,
/// even if the bootstrap process was successful.
#[derive(Debug, Default)]
pub enum CheckOnline {
    /// Do not check that bootnodes are online.
    #[default]
    Disabled,
    /// Check that bootnodes are online.
    Enabled(Interval),
}

/// Metrics for a bootnode.
///
/// Note that the `Discv4Service` will evict bootnodes that have missed a pong, and will not add
/// them back unless there are no closest nodes, or pending FindNode requests.
///
/// This means additional pings will not be sent to bootnodes that have missed a pong.
///
/// A non-zero number of missed pongs, but low number of sent pings sent may indicate that
/// bootstrapping the network has succeeded through some other method that did not involve the
/// bootnodes.
#[derive(Metrics)]
#[metrics(scope = "discv4")]
pub struct BootnodeMetrics {
    /// The number of missed pongs
    pub missed_pongs: Counter,
    /// The number of pings sent
    pub pings_sent: Counter,
}

impl Discv4Metrics {
    /// Create new metrics, with enabled bootnode checking.
    pub fn from_interval(interval: Interval) -> Self {
        Self { check_online: CheckOnline::Enabled(interval), ..Default::default() }
    }

    /// Report that a bootnode missed a pong, using the peer ID.
    /// If the peer ID is not associated with a bootnode, this is a no-op.
    pub(crate) fn bootnode_missed_pong_peer_id(&mut self, peer_id: PeerId) {
        if let Some(bootnode) = self.bootnodes.get(&peer_id) {
            self.bootnode_missed_pong(*bootnode);
        }
    }

    /// Report that a bootnode has been sent a ping.
    ///
    /// If the bootnode does not exist, this is a no-op.
    pub(crate) fn bootnode_ping_sent(&mut self, bootnode: NodeRecord) {
        self.bootnode_metrics.entry(bootnode).and_modify(|m| {
            m.pings_sent.increment(1);
        });
    }

    /// Report that a bootnode responded to a ping, and did not miss a pong. If the bootnode does
    /// not exist, new metrics will be created for it.
    pub(crate) fn bootnode_responded_to_ping(&mut self, bootnode: NodeRecord) {
        self.bootnode_metrics
            .entry(bootnode)
            .or_insert_with(|| {
                BootnodeMetrics::new_with_labels(&[("bootnode", bootnode.to_string())])
            })
            .missed_pongs
            .absolute(0);

        self.bootnodes.entry(bootnode.id).or_insert(bootnode);
    }

    /// Register a bootnode. If it does not exist, it will be created and initialized with a
    /// default value. If the bootnode already exists, it will be left unchanged.
    pub(crate) fn register_bootnode(&mut self, bootnode: NodeRecord) {
        self.bootnode_metrics.entry(bootnode).or_insert_with(|| {
            BootnodeMetrics::new_with_labels(&[("bootnode", bootnode.to_string())])
        });

        self.bootnodes.entry(bootnode.id).or_insert(bootnode);
    }

    /// Report that a boootnode missed a pong, or create metrics for a bootnode if it does not
    /// exist.
    fn bootnode_missed_pong(&mut self, bootnode: NodeRecord) {
        self.bootnode_metrics
            .entry(bootnode)
            .or_insert_with(|| {
                BootnodeMetrics::new_with_labels(&[("bootnode", bootnode.to_string())])
            })
            .missed_pongs
            .increment(1);

        self.bootnodes.entry(bootnode.id).or_insert(bootnode);
    }

    /// Polls the `CheckOnline` enum, returning true if the discv4 service should check that
    /// bootnodes are online.
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        match &mut self.check_online {
            CheckOnline::Disabled => Poll::Ready(false),
            CheckOnline::Enabled(interval) => match interval.poll_tick(cx) {
                Poll::Ready(_) => Poll::Ready(true),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}
