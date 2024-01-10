use std::path::PathBuf;

use deno_runtime::deno_core;
use deno_runtime::deno_core::op2;
use deno_runtime::deno_core::ModuleSpecifier;
use deno_runtime::deno_core::OpState;
use deno_runtime::permissions::PermissionsContainer;
use deno_runtime::worker::MainWorker as DenoWorker;
use deno_runtime::worker::WorkerOptions;
use gasket::framework::*;
use pallas::interop::utxorpc::map_block;
use pallas::interop::utxorpc::map_tx_output;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::ledger::traverse::OutputRef;
use serde::Deserialize;
use serde_json::json;
use tracing::info;
use utxorpc::proto::cardano::v1 as u5c;

use crate::framework::model::CRDTCommand;
use crate::framework::*;

const SYNC_CALL_SNIPPET: &str = r#"Deno[Deno.internal].core.ops.op_put_record(METHOD(Deno[Deno.internal].core.ops.op_pop_record()));"#;
const ASYNC_CALL_SNIPPET: &str = r#"METHOD(Deno[Deno.internal].core.ops.op_pop_record()).then(x => Deno[Deno.internal].core.ops.op_put_record(x));"#;

deno_core::extension!(deno_reducer, ops = [op_pop_record, op_put_record]);

#[op2]
#[serde]
pub fn op_pop_record(state: &mut OpState) -> Result<serde_json::Value, deno_core::error::AnyError> {
    let block: u5c::Block = state.take();
    Ok(json!(block))
}

#[op2]
pub fn op_put_record(
    state: &mut OpState,
    #[serde] value: serde_json::Value,
) -> Result<(), deno_core::error::AnyError> {
    match value {
        serde_json::Value::Null => (),
        _ => state.put(value),
    };

    Ok(())
}

#[derive(Deserialize)]
pub struct Config {
    main_module: String,
    use_async: bool,
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Stage, Error> {
        let stage = Stage {
            main_module: PathBuf::from(self.main_module),
            storage_type: ctx.storage_type.clone(),
            call_snippet: if self.use_async {
                ASYNC_CALL_SNIPPET
            } else {
                SYNC_CALL_SNIPPET
            },
            ..Default::default()
        };

        Ok(stage)
    }
}

async fn setup_deno(main_module: &PathBuf) -> Result<DenoWorker, WorkerError> {
    let empty_module = deno_core::ModuleSpecifier::parse("data:text/javascript;base64,").unwrap();

    let mut deno = DenoWorker::bootstrap_from_options(
        empty_module,
        PermissionsContainer::allow_all(),
        WorkerOptions {
            extensions: vec![deno_reducer::init_ops()],
            ..Default::default()
        },
    );

    let code = deno_core::FastString::from(std::fs::read_to_string(main_module).unwrap());

    deno.js_runtime
        .load_side_module(&ModuleSpecifier::parse("lyra:reducer").unwrap(), Some(code))
        .await
        .unwrap();

    let runtime_code = deno_core::FastString::from_static(include_str!("./runtime.js"));

    deno.execute_script("[lyra:runtime.js]", runtime_code)
        .or_panic()?;
    deno.run_event_loop(false).await.unwrap();

    Ok(deno)
}

#[derive(Default, Stage)]
#[stage(name = "reducer-deno", unit = "ChainEvent", worker = "Worker")]
pub struct Stage {
    main_module: PathBuf,
    storage_type: String,
    call_snippet: &'static str,

    pub input: ReducerInputPort,
    pub output: ReducerOutputPort,

    #[metric]
    ops_count: gasket::metrics::Counter,
}

pub struct Worker {
    runtime: DenoWorker,
}

impl Worker {
    async fn reduce(
        &mut self,
        call_snippet: String,
        block: u5c::Block,
    ) -> Result<Option<serde_json::Value>, WorkerError> {
        let deno = &mut self.runtime;

        deno.js_runtime.op_state().borrow_mut().put(block);

        let script = deno_core::FastString::from(call_snippet);
        deno.execute_script("<anon>", script).or_panic()?;
        deno.run_event_loop(false).await.unwrap();

        let output: Option<serde_json::Value> = deno.js_runtime.op_state().borrow_mut().try_take();

        Ok(output)
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let runtime = setup_deno(&stage.main_module).await?;
        Ok(Self { runtime })
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<ChainEvent>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &ChainEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        let record = unit.record();
        if record.is_none() {
            return Ok(());
        }

        let record = record.unwrap();

        let call_snippet = match unit {
            ChainEvent::Apply(_, _) => stage.call_snippet.replace("METHOD", "apply"),
            ChainEvent::Undo(_, _) => stage.call_snippet.replace("METHOD", "undo"),
            ChainEvent::Reset(_) => return Ok(()),
        };

        let output = match record {
            Record::EnrichedBlockPayload(block, ctx) => {
                let block = MultiEraBlock::decode(block)
                    .map_err(Error::cbor)
                    .or_panic()?;
                let mut block = map_block(&block);

                for tx in block.body.as_mut().unwrap().tx.iter_mut() {
                    for input in tx.inputs.iter_mut() {
                        if input.tx_hash.len() == 32 {
                            let mut hash_bytes = [0u8; 32];
                            hash_bytes.copy_from_slice(&input.tx_hash);

                            let tx_hash = pallas::crypto::hash::Hash::from(hash_bytes);
                            let output_index = input.output_index as u64;
                            let output_ref = OutputRef::new(tx_hash, output_index);

                            if let Ok(output) = ctx.find_utxo(&output_ref) {
                                input.as_output = Some(map_tx_output(&output));
                            }
                        }
                    }
                }

                self.reduce(call_snippet, block).await.unwrap()
            }
            Record::UtxoRpcBlockPayload(block) => {
                self.reduce(call_snippet, block.clone()).await.unwrap()
            }
            _ => todo!(),
        };

        if let Some(json) = output {
            let event = match stage.storage_type.as_str() {
                "Redis" => {
                    let commands: Vec<CRDTCommand> =
                        CRDTCommand::from_json_array(&json).or_panic()?;
                    ChainEvent::apply(unit.point().clone(), Record::CRDTCommand(commands))
                }
                "Postgres" => {
                    let commands: Vec<String> = serde_json::from_value(json).or_panic()?;
                    ChainEvent::apply(unit.point().clone(), Record::SQLCommand(commands))
                }
                _ => return Err(WorkerError::Panic),
            };
            stage.output.send(event).await.or_retry()?;
        }

        info!("Reduced block {:?}", unit.point());

        Ok(())
    }
}
