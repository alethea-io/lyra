use gasket::runtime::Tether;
use serde::Deserialize;

use crate::framework::{errors::Error, *};

pub mod builtin;
pub mod deno;

pub enum Bootstrapper {
    BuiltIn(builtin::Stage),
    Deno(deno::Stage),
}

impl StageBootstrapper for Bootstrapper {
    fn connect_output(&mut self, adapter: OutputAdapter) {
        match self {
            Bootstrapper::BuiltIn(p) => p.output.connect(adapter),
            Bootstrapper::Deno(p) => p.output.connect(adapter),
        }
    }

    fn connect_input(&mut self, adapter: InputAdapter) {
        match self {
            Bootstrapper::BuiltIn(p) => p.input.connect(adapter),
            Bootstrapper::Deno(p) => p.input.connect(adapter),
        }
    }

    fn spawn(self, policy: gasket::runtime::Policy) -> Tether {
        match self {
            Bootstrapper::BuiltIn(s) => gasket::runtime::spawn_stage(s, policy),
            Bootstrapper::Deno(s) => gasket::runtime::spawn_stage(s, policy),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    BuiltIn(builtin::Config),
    Deno(deno::Config),
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Bootstrapper, Error> {
        match self {
            Config::BuiltIn(c) => Ok(Bootstrapper::BuiltIn(c.bootstrapper(ctx)?)),
            Config::Deno(c) => Ok(Bootstrapper::Deno(c.bootstrapper(ctx)?)),
        }
    }
}
