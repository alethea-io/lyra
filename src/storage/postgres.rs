use bb8_postgres::bb8::Pool;
use bb8_postgres::tokio_postgres::NoTls;
use bb8_postgres::PostgresConnectionManager;
use gasket::framework::*;
use serde::Deserialize;
use tracing::info;

use crate::framework::*;

pub struct Worker {
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let manager =
            PostgresConnectionManager::new_from_stringlike(stage.config.url.clone(), NoTls)
                .or_panic()?;
        let pool = Pool::builder().build(manager).await.or_panic()?;
        Ok(Self { pool })
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<ChainEvent>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &ChainEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        let point = unit.point().clone();
        let record = unit.record().cloned();

        if record.is_none() {
            return Ok(());
        }

        let record = record.unwrap();

        match record {
            Record::SQLCommand(commands) => {
                let conn = self.pool.get().await.or_restart()?;

                conn.execute("BEGIN", &[]).await.or_restart()?;
                for command in commands {
                    conn.execute(&command, &[]).await.or_restart()?;
                }

                // Update the cursor state
                stage.cursor.track(point.clone());

                let cursor_data = serde_json::to_string(&stage.cursor.to_data()).or_panic()?;

                let query = format!(
                    "INSERT INTO {}.cursor (name, data)
                     VALUES ($1, $2)
                     ON CONFLICT (name)
                     DO UPDATE SET data = EXCLUDED.data",
                    stage.config.schema
                );

                conn.execute(&query, &[&"default", &cursor_data])
                    .await
                    .or_restart()?;

                conn.execute("COMMIT", &[]).await.or_restart()?;
            }
            _ => {}
        }

        info!("Stored block {:?}", point);

        stage.ops_count.inc(1);
        stage.latest_block.set(point.slot_or_default() as i64);

        Ok(())
    }
}

#[derive(Stage)]
#[stage(name = "storage-postgres", unit = "ChainEvent", worker = "Worker")]
pub struct Stage {
    config: Config,
    cursor: Breadcrumbs,

    pub input: StorageInputPort,

    #[metric]
    ops_count: gasket::metrics::Counter,

    #[metric]
    latest_block: gasket::metrics::Gauge,
}

#[derive(Default, Deserialize)]
pub struct Config {
    pub url: String,
    pub schema: String,
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Stage, Error> {
        let stage = Stage {
            config: self,
            cursor: ctx.cursor.clone(),
            input: Default::default(),
            ops_count: Default::default(),
            latest_block: Default::default(),
        };

        Ok(stage)
    }

    pub async fn load_cursor(&self) -> Result<Breadcrumbs, Error> {
        let manager = PostgresConnectionManager::new_from_stringlike(self.url.clone(), NoTls)
            .map_err(Error::storage)?;
        let pool = Pool::builder()
            .build(manager)
            .await
            .map_err(Error::storage)?;

        let conn = pool.get().await.map_err(Error::storage)?;
        let query = format!(
            "SELECT data FROM {}.cursor WHERE name = 'default';",
            self.schema
        );
        let row = conn.query_opt(&query, &[]).await.map_err(Error::storage)?;

        match row {
            Some(row) => {
                let json = row.get::<_, String>("data");
                let data: Vec<(u64, String)> =
                    serde_json::from_str(&json).map_err(Error::parsing)?;
                Breadcrumbs::from_data(data)
            }
            None => Ok(Breadcrumbs::new()),
        }
    }
}
