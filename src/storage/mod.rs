use gasket::runtime::Tether;
use serde::Deserialize;

use crate::framework::{errors::Error, *};

pub mod postgres;
pub mod redis;

pub enum Bootstrapper {
    Postgres(postgres::Stage),
    Redis(redis::Stage),
}

impl StageBootstrapper for Bootstrapper {
    fn connect_output(&mut self, _: OutputAdapter) {
        panic!("attempted to use storage stage as sender");
    }

    fn connect_input(&mut self, adapter: InputAdapter) {
        match self {
            Bootstrapper::Postgres(p) => p.input.connect(adapter),
            Bootstrapper::Redis(p) => p.input.connect(adapter),
        }
    }

    fn spawn(self, policy: gasket::runtime::Policy) -> Tether {
        match self {
            Bootstrapper::Postgres(x) => gasket::runtime::spawn_stage(x, policy),
            Bootstrapper::Redis(s) => gasket::runtime::spawn_stage(s, policy),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    Postgres(postgres::Config),
    Redis(redis::Config),
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Bootstrapper, Error> {
        match self {
            Config::Postgres(c) => Ok(Bootstrapper::Postgres(c.bootstrapper(ctx)?)),
            Config::Redis(c) => Ok(Bootstrapper::Redis(c.bootstrapper(ctx)?)),
        }
    }

    pub async fn load_cursor(&self) -> Result<Breadcrumbs, Error> {
        match self {
            Config::Postgres(c) => c.load_cursor().await,
            Config::Redis(c) => c.load_cursor().await,
        }
    }

    pub fn get_type(&self) -> &'static str {
        match self {
            Config::Postgres(_) => "Postgres",
            Config::Redis(_) => "Redis",
        }
    }
}
