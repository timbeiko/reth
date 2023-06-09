use std::ops::{Deref, RangeInclusive};

use crate::{ExecInput, ExecOutput, Stage, StageError, UnwindInput, UnwindOutput};
use reth_db::{cursor::DbCursorRO, database::Database, tables, transaction::DbTx, DatabaseError};
use reth_primitives::{
    stage::{
        CheckpointBlockRange, EntitiesCheckpoint, IndexHistoryCheckpoint, StageCheckpoint, StageId,
    },
    BlockNumber,
};
use reth_provider::Transaction;
use tracing::info;

/// The log indexing stage.
///
/// This stage walks over all available receipts and inserts the index of block numbers where
/// particular log addresses and topics occurred.
///
/// For more information see [reth_db::tables::LogAddressHistory] and
/// [reth_db::tables::LogTopicHistory].
#[derive(Debug, Clone)]
pub struct IndexLogHistoryStage {
    commit_threshold: u64,
}

#[async_trait::async_trait]
impl<DB: Database> Stage<DB> for IndexLogHistoryStage {
    /// Return the id of the stage
    fn id(&self) -> StageId {
        StageId::IndexLogHistory
    }

    /// Execute the stage.
    async fn execute(
        &mut self,
        tx: &mut Transaction<'_, DB>,
        input: ExecInput,
    ) -> Result<ExecOutput, StageError> {
        let (range, is_final_range) = input.next_block_range_with_threshold(self.commit_threshold);

        if range.is_empty() {
            return Ok(ExecOutput::done(*range.start()))
        }

        let mut stage_checkpoint = stage_checkpoint(tx, input.checkpoint(), &range)?;

        // Retrieve updated indices for a given range.
        let (log_address_indices, log_topic_indices, num_of_receipts) =
            tx.get_log_addresses_and_topics(range.clone())?;

        // Update the indices in the database.
        tx.insert_log_address_history_index(log_address_indices)?;
        tx.insert_log_topic_history_index(log_topic_indices)?;

        // Increment the number of processed entities.
        stage_checkpoint.progress.processed += num_of_receipts;

        info!(target: "sync::stages::index_log_history", checkpoint = *range.end(), is_final_range, "Stage iteration finished");
        Ok(ExecOutput {
            checkpoint: StageCheckpoint::new(*range.end())
                .with_index_history_stage_checkpoint(stage_checkpoint),
            done: is_final_range,
        })
    }

    /// Unwind the stage.
    async fn unwind(
        &mut self,
        tx: &mut Transaction<'_, DB>,
        input: UnwindInput,
    ) -> Result<UnwindOutput, StageError> {
        let (range, unwind_progress, is_final_range) =
            input.unwind_block_range_with_threshold(self.commit_threshold);

        tx.unwind_log_history_indices(range)?;

        info!(target: "sync::stages::index_account_history", to_block = input.unwind_to, unwind_progress, is_final_range, "Unwind iteration finished");
        Ok(UnwindOutput { checkpoint: StageCheckpoint::new(unwind_progress) })
    }
}

/// The function proceeds as follows:
/// 1. It first checks if the checkpoint has an [IndexHistoryCheckpoint] that matches the given
/// block range. If it does, the function returns that checkpoint.
/// 2. If the checkpoint's block range end matches the current checkpoint's block number, it creates
/// a new [IndexHistoryCheckpoint] with the given block range and updates the progress with the
/// current progress.
/// 3. If none of the above conditions are met, it creates a new [IndexHistoryCheckpoint] with the
/// given block range and calculates the progress by counting the number of processed entries in the
/// [tables::Receipts] table within the given block range.
fn stage_checkpoint<DB: Database>(
    tx: &Transaction<'_, DB>,
    checkpoint: StageCheckpoint,
    range: &RangeInclusive<BlockNumber>,
) -> Result<IndexHistoryCheckpoint, DatabaseError> {
    Ok(match checkpoint.index_history_stage_checkpoint() {
        Some(stage_checkpoint @ IndexHistoryCheckpoint { block_range, .. })
            if block_range == CheckpointBlockRange::from(range) =>
        {
            stage_checkpoint
        }
        Some(IndexHistoryCheckpoint { block_range, progress })
            if block_range.to == checkpoint.block_number =>
        {
            IndexHistoryCheckpoint {
                block_range: CheckpointBlockRange::from(range),
                progress: EntitiesCheckpoint {
                    processed: progress.processed,
                    total: tx.deref().entries::<tables::Receipts>()? as u64,
                },
            }
        }
        _ => {
            let last_processed_tx_num = tx
                .get::<tables::BlockBodyIndices>(*range.end())?
                .map_or(0, |body| body.last_tx_num());
            IndexHistoryCheckpoint {
                block_range: CheckpointBlockRange::from(range),
                progress: EntitiesCheckpoint {
                    processed: tx
                        .cursor_read::<tables::Receipts>()?
                        .walk_range(0..=last_processed_tx_num)?
                        .count() as u64,
                    total: tx.deref().entries::<tables::Receipts>()? as u64,
                },
            }
        }
    })
}
