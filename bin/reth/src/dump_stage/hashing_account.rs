use crate::{
    db::DbTool,
    dirs::{DbPath, PlatformPath},
    dump_stage::setup,
};
use eyre::Result;
use reth_db::{database::Database, table::TableImporter, tables, transaction::DbTx};
use reth_provider::Transaction;
use reth_stages::{stages::AccountHashingStage, Stage, StageId, UnwindInput};
use std::ops::DerefMut;
use tracing::info;

pub(crate) async fn dump_hashing_account_stage<DB: Database>(
    db_tool: &mut DbTool<'_, DB>,
    from: u64,
    to: u64,
    output_db: &PlatformPath<DbPath>,
    should_run: bool,
) -> Result<()> {
    let (output_db, tip_block_number) = setup::<DB>(from, to, output_db, db_tool)?;

    // Import relevant AccountChangeSets
    let tx = db_tool.db.tx()?;
    let from_transition_rev =
        tx.get::<tables::BlockTransitionIndex>(from)?.expect("there should be at least one.");
    let to_transition_rev =
        tx.get::<tables::BlockTransitionIndex>(to)?.expect("there should be at least one.");
    output_db.update(|tx| {
        tx.import_table_with_range::<tables::AccountChangeSet, _>(
            &db_tool.db.tx()?,
            Some(from_transition_rev),
            to_transition_rev,
        )
    })??;

    unwind_and_copy::<DB>(db_tool, from, tip_block_number, &output_db).await?;

    if should_run {
        dry_run(output_db, to, from).await?;
    }

    Ok(())
}

/// Dry-run an unwind to FROM block and copy the necessary table data to the new database.
async fn unwind_and_copy<DB: Database>(
    db_tool: &mut DbTool<'_, DB>,
    from: u64,
    tip_block_number: u64,
    output_db: &reth_db::mdbx::Env<reth_db::mdbx::WriteMap>,
) -> eyre::Result<()> {
    let mut unwind_tx = Transaction::new(db_tool.db)?;
    let mut exec_stage = AccountHashingStage::default();

    exec_stage
        .unwind(
            &mut unwind_tx,
            UnwindInput { unwind_to: from, stage_progress: tip_block_number, bad_block: None },
        )
        .await?;
    let unwind_inner_tx = unwind_tx.deref_mut();

    output_db.update(|tx| tx.import_table::<tables::PlainAccountState, _>(unwind_inner_tx))??;

    unwind_tx.drop()?;

    Ok(())
}

/// Try to re-execute the stage straightaway
async fn dry_run(
    output_db: reth_db::mdbx::Env<reth_db::mdbx::WriteMap>,
    to: u64,
    from: u64,
) -> eyre::Result<()> {
    info!(target: "reth::cli", "Executing stage.");

    let mut tx = Transaction::new(&output_db)?;
    let mut exec_stage = AccountHashingStage {
        clean_threshold: 1, // Forces hashing from scratch
        ..Default::default()
    };

    let mut exec_output = false;
    while !exec_output {
        exec_output = exec_stage
            .execute(
                &mut tx,
                reth_stages::ExecInput {
                    previous_stage: Some((StageId("Another"), to)),
                    stage_progress: Some(from),
                },
            )
            .await?
            .done;
    }

    tx.drop()?;

    info!(target: "reth::cli", "Success.");

    Ok(())
}
