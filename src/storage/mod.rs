use gasket::runtime::Tether;
use serde::Deserialize;

use crate::framework::{errors::Error, *};

pub mod none;
pub mod postgres;
pub mod redis;

pub enum Bootstrapper {
    None(none::Stage),
    Postgres(postgres::Stage),
    Redis(redis::Stage),
}

impl Bootstrapper {
    pub fn borrow_input(&mut self) -> &mut ReducerInputPort {
        match self {
            Bootstrapper::None(p) => &mut p.input,
            Bootstrapper::Postgres(p) => &mut p.input,
            Bootstrapper::Redis(p) => &mut p.input,
        }
    }

    pub fn spawn(self, policy: gasket::runtime::Policy) -> Tether {
        match self {
            Bootstrapper::None(x) => gasket::runtime::spawn_stage(x, policy),
            Bootstrapper::Postgres(x) => gasket::runtime::spawn_stage(x, policy),
            Bootstrapper::Redis(s) => gasket::runtime::spawn_stage(s, policy),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    None(none::Config),
    Postgres(postgres::Config),
    Redis(redis::Config),
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Bootstrapper, Error> {
        match self {
            Config::None(c) => Ok(Bootstrapper::None(c.bootstrapper(ctx)?)),
            Config::Postgres(c) => Ok(Bootstrapper::Postgres(c.bootstrapper(ctx)?)),
            Config::Redis(c) => Ok(Bootstrapper::Redis(c.bootstrapper(ctx)?)),
        }
    }

    pub async fn load_cursor(&self) -> Result<Breadcrumbs, Error> {
        match self {
            Config::None(c) => c.load_cursor().await,
            Config::Postgres(c) => c.load_cursor().await,
            Config::Redis(c) => c.load_cursor().await,
        }
    }

    pub fn get_type(&self) -> &'static str {
        match self {
            Config::None(_) => "None",
            Config::Postgres(_) => "Postgres",
            Config::Redis(_) => "Redis",
        }
    }
}
