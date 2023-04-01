use metrics::Counter;
use reth_metrics_derive::Metrics;
use reth_primitives::{NodeRecord, PeerId};
use std::collections::HashMap;

/// Metrics for the discv4 service
#[derive(Default)]
pub struct Discv4Metrics {
    /// Metrics for bootnodes, indexed by their node record
    pub(crate) bootnode_metrics: HashMap<NodeRecord, BootnodeMetrics>,
    /// A map of bootstrap node IDs to their node records
    pub(crate) bootnodes: HashMap<PeerId, NodeRecord>,
}

/// Metrics for a bootnode
#[derive(Metrics)]
#[metrics(scope = "discv4")]
pub struct BootnodeMetrics {
    /// The number of missed pongs
    pub missed_pongs: Counter,
}

impl Discv4Metrics {
    /// Report that a bootnode missed a pong, using the peer ID.
    /// If the peer ID is not associated with a bootnode, this is a no-op.
    pub(crate) fn bootnode_missed_pong_peer_id(&mut self, peer_id: PeerId) {
        if let Some(bootnode) = self.bootnodes.get(&peer_id) {
            self.bootnode_missed_pong(*bootnode);
        }
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
}
