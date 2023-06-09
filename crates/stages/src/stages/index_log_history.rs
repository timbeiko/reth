use crate::{ExecInput, ExecOutput, Stage, StageError, UnwindInput, UnwindOutput};
use reth_db::database::Database;
use reth_primitives::stage::{StageCheckpoint, StageId};
use reth_provider::Transaction;
use tracing::info;

/// The log indexing stage.
///
/// TODO:
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

        // TODO: stage checkpoint
        let (log_address_indices, log_topic_indices) =
            tx.get_log_addresses_and_topics(range.clone())?;
        tx.insert_log_address_history_index(log_address_indices)?;
        tx.insert_log_topic_history_index(log_topic_indices)?;

        info!(target: "sync::stages::index_log_history", checkpoint = *range.end(), is_final_range, "Stage iteration finished");
        Ok(ExecOutput {
            checkpoint: StageCheckpoint::new(*range.end()), // TODO:
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

// TODO:
// fn stage_checkpoint<DB: Database>(
//     tx: &Transaction<'_, DB>,
// ) -> Result<EntitiesCheckpoint, DatabaseError> {
//     Ok(EntitiesCheckpoint {
//         processed: tx.deref().entries::<tables::TxSenders>()? as u64,
//         total: tx.deref().entries::<tables::Transactions>()? as u64,
//     })
// }
