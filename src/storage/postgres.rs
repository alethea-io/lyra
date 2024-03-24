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

        if stage.should_finalize {
            return Ok(WorkSchedule::Done);
        }

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
                let conn = self
                    .pool
                    .get()
                    .await
                    .expect("Failed to acquire a Postgres connection");

                conn.execute("BEGIN", &[])
                    .await
                    .expect("Failed to begin transaction");

                for command in commands {
                    conn.execute(&command, &[])
                        .await
                        .expect("Failed to execute transaction");
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

                conn.execute(&query, &[&stage.config.cursor_name, &cursor_data])
                    .await
                    .expect("Failed to save cursor");

                conn.execute("COMMIT", &[])
                    .await
                    .expect("Failed to commit transaction");
            }
            _ => {}
        }

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
#[stage(name = "storage-postgres", unit = "ChainEvent", worker = "Worker")]
pub struct Stage {
    config: Config,
    cursor: Breadcrumbs,
    finalize: Option<FinalizeConfig>,
    should_finalize: bool,

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
    pub cursor_name: String,
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Stage, Error> {
        let stage = Stage {
            config: self,
            cursor: ctx.cursor.clone(),
            finalize: ctx.finalize.clone(),
            should_finalize: false,
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
            "SELECT data FROM {}.cursor WHERE name = '{}';",
            self.schema, self.cursor_name,
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
