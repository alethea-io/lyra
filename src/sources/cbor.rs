use std::path::PathBuf;

use gasket::framework::*;
use pallas::network::miniprotocols::Point;
use serde::Deserialize;
use tracing::info;

use crate::framework::*;

pub enum Action {
    Apply(Vec<u8>),
    Undo(Vec<u8>),
}

#[derive(Clone)]
struct LedgerStore;

impl LedgerStore {
    fn new() -> Self {
        Self
    }
}

impl pallas::interop::utxorpc::LedgerContext for LedgerStore {
    fn get_utxos<'a>(
        &self,
        _refs: &[pallas::interop::utxorpc::TxoRef],
    ) -> Option<pallas::interop::utxorpc::UtxoMap> {
        None
    }
}

pub struct Worker {
    index: usize,
    files: Vec<PathBuf>,
    mapper: pallas::interop::utxorpc::Mapper<LedgerStore>,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let mut files = Vec::new();
        let mut dir = tokio::fs::read_dir(&stage.config.dir).await.unwrap();

        while let Some(entry) = dir.next_entry().await.map_err(|_| WorkerError::Panic)? {
            let path = entry.path();
            if path.is_file() {
                files.push(path);
            }
        }

        files.sort();

        let ledger = LedgerStore::new();
        let mapper = pallas::interop::utxorpc::Mapper::new(ledger);

        Ok(Self {
            index: 0,
            files,
            mapper,
        })
    }

    async fn schedule(&mut self, _: &mut Stage) -> Result<WorkSchedule<Vec<Action>>, WorkerError> {
        if self.index < self.files.len() {
            let path = &self.files[self.index];
            self.index += 1;

            let cbor = tokio::fs::read(&path)
                .await
                .map_err(|_| WorkerError::Panic)?;

            let file_name = path
                .file_name()
                .and_then(|os_str| os_str.to_str())
                .unwrap_or("");

            let action = if file_name.contains("undo") {
                Action::Undo(hex::decode(cbor).unwrap())
            } else {
                Action::Apply(hex::decode(cbor).unwrap())
            };

            Ok(WorkSchedule::Unit(vec![action]))
        } else {
            Ok(WorkSchedule::Idle)
        }
    }

    async fn execute(&mut self, unit: &Vec<Action>, stage: &mut Stage) -> Result<(), WorkerError> {
        for action in unit {
            match action {
                Action::Apply(cbor) => {
                    let block = self.mapper.map_block_cbor(&cbor);
                    if block.body.is_some() {
                        let header = block.header.as_ref().unwrap();
                        let event = ChainEvent::Apply(
                            Point::Specific(header.slot, header.hash.to_vec()),
                            Record::UtxoRpcBlockPayload(block.clone()),
                        );

                        info!("Applying block {:?}", event.point());

                        stage.output.send(event.into()).await.or_panic()?;
                        stage.chain_tip.set(header.slot as i64);
                    }
                }
                Action::Undo(cbor) => {
                    let block = self.mapper.map_block_cbor(&cbor);
                    if block.body.is_some() {
                        let header = block.header.as_ref().unwrap();
                        let event = ChainEvent::Undo(
                            Point::Specific(header.slot, header.hash.to_vec()),
                            Record::UtxoRpcBlockPayload(block.clone()),
                        );

                        info!("Undoing block {:?}", event.point());

                        stage.output.send(event.into()).await.or_panic()?;
                        stage.chain_tip.set(header.slot as i64);
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Stage)]
#[stage(name = "source-cbor", unit = "Vec<Action>", worker = "Worker")]
pub struct Stage {
    config: Config,
    pub output: SourceOutputPort,
    #[metric]
    ops_count: gasket::metrics::Counter,
    #[metric]
    chain_tip: gasket::metrics::Gauge,
}

#[derive(Deserialize)]
pub struct Config {
    dir: String,
}

impl Config {
    pub fn bootstrapper(self, _ctx: &Context) -> Result<Stage, Error> {
        let stage = Stage {
            config: self,
            output: Default::default(),
            ops_count: Default::default(),
            chain_tip: Default::default(),
        };

        Ok(stage)
    }
}
