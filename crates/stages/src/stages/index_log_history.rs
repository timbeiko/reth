use crate::{ExecInput, ExecOutput, Stage, StageError, UnwindInput, UnwindOutput};
use reth_db::{cursor::DbCursorRO, database::Database, tables, transaction::DbTx, DatabaseError};
use reth_primitives::{
    stage::{
        CheckpointBlockRange, EntitiesCheckpoint, IndexHistoryCheckpoint, StageCheckpoint, StageId,
    },
    BlockNumber,
};
use reth_provider::Transaction;
use std::ops::{Deref, RangeInclusive};
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
            return Ok(ExecOutput::done(input.checkpoint()))
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
                .get::<tables::BlockBodyIndices>(checkpoint.block_number)?
                .map_or(0, |body| body.last_tx_num() + 1);
            IndexHistoryCheckpoint {
                block_range: CheckpointBlockRange::from(range),
                progress: EntitiesCheckpoint {
                    processed: tx
                        .cursor_read::<tables::Receipts>()?
                        .walk_range(..last_processed_tx_num)?
                        .count() as u64,
                    total: tx.deref().entries::<tables::Receipts>()? as u64,
                },
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        stage_test_suite_ext, ExecuteStageTestRunner, StageTestRunner, TestRunnerError,
        TestTransaction, UnwindStageTestRunner, PREV_STAGE_ID,
    };
    use assert_matches::assert_matches;
    use reth_db::models::ShardedKey;
    use reth_interfaces::test_utils::generators::{random_block_range, random_receipt};
    use reth_primitives::{Receipt, SealedBlock, H256};

    stage_test_suite_ext!(IndexLogHistoryTestRunner, index_log_history);

    /// Execute the stage twice with input range that exceeds the commit threshold
    #[tokio::test]
    async fn execute_intermediate_commit() {
        let threshold = 50;
        let mut runner = IndexLogHistoryTestRunner::default();
        runner.set_threshold(threshold);
        let (stage_progress, previous_stage) = (1000, 1100); // input exceeds threshold

        let first_input = ExecInput {
            previous_stage: Some((PREV_STAGE_ID, previous_stage)),
            checkpoint: Some(StageCheckpoint::new(stage_progress)),
        };
        let (seed_blocks, seed_receipts) =
            runner.seed_execution(first_input).expect("failed to seed execution");

        let total_receipts = runner.tx.table::<tables::Receipts>().unwrap().len() as u64;
        assert_eq!(seed_receipts.len() as u64, total_receipts);

        // Execute first time
        let result = runner.execute(first_input).await.unwrap();
        let expected_progress = stage_progress + threshold;
        let processed = seed_blocks
            .iter()
            .filter(|block| block.number <= expected_progress)
            .fold(0, |acc, block| acc + block.body.len()) as u64;
        assert_matches!(result, Ok(_));
        assert_eq!(
            result.unwrap(),
            ExecOutput {
                checkpoint: StageCheckpoint::new(expected_progress)
                    .with_index_history_stage_checkpoint(IndexHistoryCheckpoint {
                        block_range: CheckpointBlockRange::from(
                            stage_progress + 1..=expected_progress
                        ),
                        progress: EntitiesCheckpoint { processed, total: total_receipts }
                    }),
                done: false
            }
        );

        // Execute second time to completion
        let second_input = ExecInput {
            previous_stage: Some((PREV_STAGE_ID, previous_stage)),
            checkpoint: Some(StageCheckpoint::new(expected_progress)),
        };
        let result = runner.execute(second_input).await.unwrap();
        assert_matches!(result, Ok(_));
        assert_eq!(
            result.as_ref().unwrap(),
            &ExecOutput {
                checkpoint: StageCheckpoint::new(previous_stage)
                    .with_index_history_stage_checkpoint(IndexHistoryCheckpoint {
                        block_range: CheckpointBlockRange::from(
                            expected_progress + 1..=previous_stage
                        ),
                        progress: EntitiesCheckpoint {
                            processed: total_receipts,
                            total: total_receipts
                        }
                    }),
                done: true
            }
        );

        assert!(runner.validate_execution(first_input, result.ok()).is_ok(), "validation failed");
    }

    struct IndexLogHistoryTestRunner {
        tx: TestTransaction,
        threshold: u64,
    }

    impl Default for IndexLogHistoryTestRunner {
        fn default() -> Self {
            Self { threshold: 1000, tx: TestTransaction::default() }
        }
    }

    impl IndexLogHistoryTestRunner {
        fn set_threshold(&mut self, threshold: u64) {
            self.threshold = threshold;
        }

        fn ensure_no_log_indices_by_block(
            &self,
            block: BlockNumber,
        ) -> Result<(), TestRunnerError> {
            let tx = self.tx.inner();
            for entry in tx.cursor_read::<tables::LogAddressHistory>()?.walk_range(..)? {
                let (_, block_indices) = entry?;
                for block_number in block_indices.iter(0) {
                    assert!(block_number as u64 <= block);
                }
            }
            for entry in tx.cursor_read::<tables::LogTopicHistory>()?.walk_range(..)? {
                let (_, block_indices) = entry?;
                for block_number in block_indices.iter(0) {
                    assert!(block_number as u64 <= block);
                }
            }
            Ok(())
        }
    }

    impl StageTestRunner for IndexLogHistoryTestRunner {
        type S = IndexLogHistoryStage;

        fn tx(&self) -> &TestTransaction {
            &self.tx
        }

        fn stage(&self) -> Self::S {
            IndexLogHistoryStage { commit_threshold: self.threshold }
        }
    }

    impl ExecuteStageTestRunner for IndexLogHistoryTestRunner {
        type Seed = (Vec<SealedBlock>, Vec<Receipt>);

        fn seed_execution(&mut self, input: ExecInput) -> Result<Self::Seed, TestRunnerError> {
            let stage_progress = input.checkpoint().block_number;
            let end = input.previous_stage_checkpoint_block_number();

            let tx_offset = None;

            let blocks = random_block_range(stage_progress + 1..=end, H256::zero(), 0..2);
            self.tx.insert_blocks(blocks.iter(), tx_offset)?;

            let logs_per_receipt = 3;
            let receipts = blocks
                .iter()
                .flat_map(|block| {
                    block.body.iter().map(|tx| random_receipt(&tx, Some(logs_per_receipt)))
                })
                .collect::<Vec<_>>();
            self.tx.insert_receipts(receipts.iter(), tx_offset)?;

            Ok((blocks, receipts))
        }

        fn validate_execution(
            &self,
            input: ExecInput,
            output: Option<ExecOutput>,
        ) -> Result<(), TestRunnerError> {
            match output {
                Some(output) => self.tx.query(|tx| {
                    let start_block = input.next_block();
                    let end_block = output.checkpoint.block_number;

                    if start_block > end_block {
                        return Ok(())
                    }

                    let mut body_cursor = tx.cursor_read::<tables::BlockBodyIndices>()?;
                    body_cursor.seek_exact(start_block)?;

                    while let Some((block_number, body)) = body_cursor.next()? {
                        for tx_id in body.tx_num_range() {
                            let receipt =
                                tx.get::<tables::Receipts>(tx_id)?.expect("no receipt entry");
                            for log in &receipt.logs {
                                // Validate address index is present for this log address
                                let address_index_entry = tx
                                    .cursor_read::<tables::LogAddressHistory>()?
                                    .seek(ShardedKey::new(log.address, block_number))?;
                                assert_matches!(address_index_entry, Some(_));
                                assert_matches!(
                                    address_index_entry.unwrap().1.find(block_number as usize),
                                    Some(_)
                                );

                                for topic in &log.topics {
                                    // Validate topic index is present for this log topic
                                    let topic_index_entry = tx
                                        .cursor_read::<tables::LogTopicHistory>()?
                                        .seek(ShardedKey::new(*topic, block_number))?;
                                    assert_matches!(topic_index_entry, Some(_));
                                    assert_matches!(
                                        topic_index_entry.unwrap().1.find(block_number as usize),
                                        Some(_)
                                    );
                                }
                            }
                        }
                    }

                    Ok(())
                })?,
                None => self.ensure_no_log_indices_by_block(input.checkpoint().block_number)?,
            };

            Ok(())
        }
    }

    impl UnwindStageTestRunner for IndexLogHistoryTestRunner {
        fn validate_unwind(&self, input: UnwindInput) -> Result<(), TestRunnerError> {
            self.ensure_no_log_indices_by_block(input.unwind_to)
        }
    }
}
