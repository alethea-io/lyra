use gasket::framework::*;
use serde::Deserialize;
use tracing::info;

use crate::framework::*;

pub struct Worker {}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {})
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<ChainEvent>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;

        if stage.should_finalize {
            return Ok(WorkSchedule::Done);
        }

        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &ChainEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        let point = unit.point().clone();
        
        info!("Stored block {:?}", point);

        stage.ops_count.inc(1);
        stage.latest_block.set(point.slot_or_default() as i64);

        if should_finalize(&stage.finalize, &point) {
            stage.should_finalize = true;
        }

        Ok(())
    }
}

#[derive(Stage)]
#[stage(name = "storage-none", unit = "ChainEvent", worker = "Worker")]
pub struct Stage {
    finalize: Option<FinalizeConfig>,
    should_finalize: bool,

    pub input: StorageInputPort,

    #[metric]
    ops_count: gasket::metrics::Counter,

    #[metric]
    latest_block: gasket::metrics::Gauge,
}

#[derive(Default, Deserialize)]
pub struct Config {}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Stage, Error> {
        let stage = Stage {
            finalize: ctx.finalize.clone(),
            should_finalize: false,
            ops_count: Default::default(),
            latest_block: Default::default(),
            input: Default::default(),
        };

        Ok(stage)
    }

    pub async fn load_cursor(&self) -> Result<Breadcrumbs, Error> {
        Ok(Breadcrumbs::new())
    }
}
