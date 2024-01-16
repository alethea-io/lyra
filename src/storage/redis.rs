use gasket::framework::*;
use r2d2_redis::r2d2;
use r2d2_redis::r2d2::Pool;
use r2d2_redis::redis;
use r2d2_redis::redis::Commands;
use r2d2_redis::redis::ToRedisArgs;
use r2d2_redis::RedisConnectionManager;
use serde::Deserialize;
use std::ops::DerefMut;
use tracing::info;

use crate::framework::*;

pub struct Worker {
    pool: Pool<RedisConnectionManager>,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let manager = RedisConnectionManager::new(stage.config.url.clone()).or_panic()?;
        let pool = r2d2::Pool::builder().build(manager).or_panic()?;

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
            Record::CRDTCommand(commands) => {
                let mut conn = self.pool.get().or_restart()?;

                redis::cmd("MULTI").query(conn.deref_mut()).or_retry()?;

                for command in commands {
                    match command {
                        model::CRDTCommand::GrowOnlySetAdd(key, value) => {
                            conn.sadd(key, value).or_restart()?;
                        }
                        model::CRDTCommand::TwoPhaseSetAdd(key, value) => {
                            conn.sadd(key, value).or_restart()?;
                        }
                        model::CRDTCommand::TwoPhaseSetRemove(key, value) => {
                            conn.sadd(format!("{}.ts", key), value).or_restart()?;
                        }
                        model::CRDTCommand::SetAdd(key, value) => {
                            conn.sadd(key, value).or_restart()?;
                        }
                        model::CRDTCommand::SetRemove(key, value) => {
                            conn.srem(key, value).or_restart()?;
                        }
                        model::CRDTCommand::LastWriteWins(key, value, slot) => {
                            conn.zadd(key, value, slot).or_restart()?;
                        }
                        model::CRDTCommand::SortedSetAdd(key, value, delta) => {
                            conn.zincr(key, value, delta).or_restart()?;
                        }
                        model::CRDTCommand::SortedSetRemove(key, value, delta) => {
                            conn.zincr(&key, value, delta).or_restart()?;

                            // removal of dangling scores  (aka garage collection)
                            conn.zrembyscore(&key, 0, 0).or_restart()?;
                        }
                        model::CRDTCommand::AnyWriteWins(key, value) => {
                            conn.set(key, value).or_restart()?;
                        }
                        model::CRDTCommand::PNCounter(key, value) => {
                            conn.incr(key, value).or_restart()?;
                        }
                        model::CRDTCommand::HashSetValue(key, member, value) => {
                            conn.hset(key, member, value).or_restart()?;
                        }
                        model::CRDTCommand::HashCounter(key, member, delta) => {
                            conn.hincr(key, member, delta).or_restart()?;
                        }
                        model::CRDTCommand::HashUnsetKey(key, member) => {
                            conn.hdel(member, key).or_restart()?;
                        }
                    }
                }

                // Update the cursor state
                stage.cursor.track(point.clone());

                let cursor_data = serde_json::to_string(&stage.cursor.to_data()).or_panic()?;

                conn.set(&stage.config.cursor_name, cursor_data)
                    .or_restart()?;

                redis::cmd("EXEC").query(conn.deref_mut()).or_retry()?;
            }
            _ => todo!(),
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
#[stage(name = "storage-redis", unit = "ChainEvent", worker = "Worker")]
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
    pub cursor_name: String,
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Stage, Error> {
        let stage = Stage {
            config: self,
            cursor: ctx.cursor.clone(),
            finalize: ctx.finalize.clone(),
            should_finalize: false,
            ops_count: Default::default(),
            latest_block: Default::default(),
            input: Default::default(),
        };

        Ok(stage)
    }

    pub async fn load_cursor(&self) -> Result<Breadcrumbs, Error> {
        let manager = RedisConnectionManager::new(self.url.clone()).map_err(Error::storage)?;
        let pool = r2d2::Pool::builder()
            .build(manager)
            .map_err(Error::storage)?;

        let mut conn = pool.get().map_err(Error::storage)?;

        match conn.get::<_, String>(&self.cursor_name) {
            Ok(json) => {
                let data: Vec<(u64, String)> =
                    serde_json::from_str(&json).map_err(Error::parsing)?;
                Breadcrumbs::from_data(data)
            }
            Err(e) => {
                if e.kind() == r2d2_redis::redis::ErrorKind::TypeError {
                    Ok(Breadcrumbs::new())
                } else {
                    Err(Error::storage(e))
                }
            }
        }
    }
}

impl ToRedisArgs for model::Value {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match self {
            model::Value::String(x) => x.write_redis_args(out),
            model::Value::BigInt(x) => x.to_string().write_redis_args(out),
            model::Value::Cbor(x) => x.write_redis_args(out),
            model::Value::Json(x) => todo!("{}", x),
        }
    }
}
