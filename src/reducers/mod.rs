use gasket::runtime::Tether;
use serde::Deserialize;

use crate::framework::{errors::Error, *};

pub mod builtin;
pub mod deno;
pub mod wasm;

pub enum Bootstrapper {
    BuiltIn(builtin::Stage),
    Deno(deno::Stage),
    Wasm(wasm::Stage),
}

impl Bootstrapper {
    pub fn borrow_output(&mut self) -> &mut ReducerOutputPort {
        match self {
            Bootstrapper::BuiltIn(p) => &mut p.output,
            Bootstrapper::Deno(p) => &mut p.output,
            Bootstrapper::Wasm(p) => &mut p.output,
        }
    }

    pub fn borrow_input(&mut self) -> &mut ReducerInputPort {
        match self {
            Bootstrapper::BuiltIn(p) => &mut p.input,
            Bootstrapper::Deno(p) => &mut p.input,
            Bootstrapper::Wasm(p) => &mut p.input,
        }
    }

    pub fn spawn(self, policy: gasket::runtime::Policy) -> Tether {
        match self {
            Bootstrapper::BuiltIn(s) => gasket::runtime::spawn_stage(s, policy),
            Bootstrapper::Deno(s) => gasket::runtime::spawn_stage(s, policy),
            Bootstrapper::Wasm(s) => gasket::runtime::spawn_stage(s, policy),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    BuiltIn(builtin::Config),
    Deno(deno::Config),
    Wasm(wasm::Config),
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Bootstrapper, Error> {
        match self {
            Config::BuiltIn(c) => Ok(Bootstrapper::BuiltIn(c.bootstrapper(ctx)?)),
            Config::Deno(c) => Ok(Bootstrapper::Deno(c.bootstrapper(ctx)?)),
            Config::Wasm(c) => Ok(Bootstrapper::Wasm(c.bootstrapper(ctx)?)),
        }
    }
}
