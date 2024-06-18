use gasket::framework::*;
use model::CRDTCommand;
use serde::Deserialize;

use crate::framework::*;

#[derive(Stage)]
#[stage(name = "reducer-wasm", unit = "ChainEvent", worker = "Worker")]
pub struct Stage {
    pub input: ReducerInputPort,
    pub output: ReducerOutputPort,

    plugin: extism::Plugin,
    storage_type: String,

    #[metric]
    ops_count: gasket::metrics::Counter,
}

#[derive(Default)]
pub struct Worker;

impl From<&Stage> for Worker {
    fn from(_: &Stage) -> Self {
        Self
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_: &Stage) -> Result<Self, WorkerError> {
        Ok(Default::default())
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<ChainEvent>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;

        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &ChainEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        let output = match unit {
            ChainEvent::Apply(_, r) => {
                let extism::convert::Json::<serde_json::Value>(output) = match r {
                    Record::UtxoRpcBlockPayload(x) => stage
                        .plugin
                        .call("apply", extism::convert::Json(x))
                        .unwrap(),
                    _ => todo!(),
                };

                Some(output)
            }
            ChainEvent::Undo(_, r) => {
                let extism::convert::Json::<serde_json::Value>(output) = match r {
                    Record::UtxoRpcBlockPayload(x) => {
                        stage.plugin.call("undo", extism::convert::Json(x)).unwrap()
                    }
                    _ => todo!(),
                };

                Some(output)
            }
            ChainEvent::Reset(_) => return Ok(()),
        };

        if let Some(json) = output {
            let event = match stage.storage_type.as_str() {
                "None" => ChainEvent::apply(unit.point().clone(), Record::None),
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
            stage.ops_count.inc(1);
        }

        Ok(())
    }
}

#[derive(Default, Deserialize)]
pub struct Config {
    path: String,
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Stage, Error> {
        let wasm = extism::Wasm::file(self.path);
        let manifest = extism::Manifest::new([wasm]);
        let plugin = extism::Plugin::new(&manifest, [], true).unwrap();

        Ok(Stage {
            input: Default::default(),
            output: Default::default(),
            ops_count: Default::default(),
            plugin,
            storage_type: ctx.storage_type.clone(),
        })
    }
}
