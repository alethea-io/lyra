use gasket::runtime::Tether;
use serde::Deserialize;

use crate::framework::{errors::Error, *};

pub mod n2c;
pub mod n2n;
pub mod u5c;

pub enum Bootstrapper {
    N2N(n2n::Stage),
    N2C(n2c::Stage),
    U5C(u5c::Stage),
}

impl Bootstrapper {
    pub fn borrow_output(&mut self) -> &mut SourceOutputPort {
        match self {
            Bootstrapper::N2N(p) => &mut p.output,
            Bootstrapper::N2C(p) => &mut p.output,
            Bootstrapper::U5C(p) => &mut p.output,
        }
    }

    pub fn spawn(self, policy: gasket::runtime::Policy) -> Tether {
        match self {
            Bootstrapper::N2N(s) => gasket::runtime::spawn_stage(s, policy),
            Bootstrapper::N2C(s) => gasket::runtime::spawn_stage(s, policy),
            Bootstrapper::U5C(s) => gasket::runtime::spawn_stage(s, policy),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    N2N(n2n::Config),
    #[cfg(target_family = "unix")]
    N2C(n2c::Config),
    U5C(u5c::Config),
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Bootstrapper, Error> {
        match self {
            Config::N2N(c) => Ok(Bootstrapper::N2N(c.bootstrapper(ctx)?)),
            Config::N2C(c) => Ok(Bootstrapper::N2C(c.bootstrapper(ctx)?)),
            Config::U5C(c) => Ok(Bootstrapper::U5C(c.bootstrapper(ctx)?)),
        }
    }
}
