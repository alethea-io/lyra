use clap;
use gasket::daemon::Daemon;
use lyra::framework::*;
use lyra::reducers;
use lyra::sources;
use lyra::storage;
use serde::Deserialize;
use std::time::Duration;
use tracing::info;

use crate::console;

#[derive(Deserialize)]
struct ConfigRoot {
    source: sources::Config,
    reducer: reducers::Config,
    storage: storage::Config,
    intersect: IntersectConfig,
    finalize: Option<FinalizeConfig>,
    chain: Option<ChainConfig>,
    retries: Option<gasket::retries::Policy>,
}

impl ConfigRoot {
    pub fn new(explicit_file: &Option<std::path::PathBuf>) -> Result<Self, config::ConfigError> {
        let mut s = config::Config::builder();

        // our base config will always be in /etc/lyra
        s = s.add_source(config::File::with_name("/etc/lyra/lyra.toml").required(false));

        // but we can override it by having a file in the working dir
        s = s.add_source(config::File::with_name("lyra.toml").required(false));

        // if an explicit file was passed, then we load it as mandatory
        if let Some(explicit) = explicit_file.as_ref().and_then(|x| x.to_str()) {
            s = s.add_source(config::File::with_name(explicit).required(true));
        }

        // finally, we use env vars to make some last-step overrides
        s = s.add_source(config::Environment::with_prefix("LYRA").separator("_"));

        s.build()?.try_deserialize()
    }
}

fn load_cursor_sync(config: &storage::Config) -> Result<Breadcrumbs, Error> {
    let runtime = tokio::runtime::Runtime::new().map_err(Error::runtime)?;
    runtime.block_on(config.load_cursor())
}

fn define_gasket_policy(config: Option<&gasket::retries::Policy>) -> gasket::runtime::Policy {
    let default_policy = gasket::retries::Policy {
        max_retries: 20,
        backoff_unit: Duration::from_secs(1),
        backoff_factor: 2,
        max_backoff: Duration::from_secs(60),
        dismissible: false,
    };

    gasket::runtime::Policy {
        tick_timeout: None,
        bootstrap_retry: config.cloned().unwrap_or(default_policy.clone()),
        work_retry: config.cloned().unwrap_or(default_policy.clone()),
        teardown_retry: config.cloned().unwrap_or(default_policy.clone()),
    }
}

fn connect_stages(
    mut source: sources::Bootstrapper,
    mut reducer: reducers::Bootstrapper,
    mut storage: storage::Bootstrapper,
    policy: gasket::runtime::Policy,
) -> Result<Daemon, Error> {
    gasket::messaging::tokio::connect_ports(source.borrow_output(), reducer.borrow_input(), 100);
    gasket::messaging::tokio::connect_ports(reducer.borrow_output(), storage.borrow_input(), 100);

    let mut tethers = vec![];
    tethers.push(source.spawn(policy.clone()));
    tethers.push(reducer.spawn(policy.clone()));
    tethers.push(storage.spawn(policy));

    let daemon = Daemon::new(tethers);

    Ok(daemon)
}

pub fn run(args: &Args) -> Result<(), Error> {
    console::initialize(&args.console);

    info!("Starting daemon...");

    let config = ConfigRoot::new(&args.config).map_err(Error::config)?;

    let current_dir = std::env::current_dir().unwrap();
    let chain = config.chain.unwrap_or_default();
    let intersect = config.intersect;
    let finalize = config.finalize;
    let storage_type = config.storage.get_type().to_owned();

    let cursor = load_cursor_sync(&config.storage).unwrap();

    if cursor.is_empty() {
        info!("No cursor found");
    } else {
        info!("Cursor found: {:?}", cursor.latest_known_point().unwrap());
    }

    let ctx = Context {
        current_dir,
        chain,
        intersect,
        cursor,
        finalize,
        storage_type,
    };

    let source = config.source.bootstrapper(&ctx)?;
    let reducer = config.reducer.bootstrapper(&ctx)?;
    let storage = config.storage.bootstrapper(&ctx)?;

    let retries = define_gasket_policy(config.retries.as_ref());

    let daemon = connect_stages(source, reducer, storage, retries)?;

    info!("lyra is running...");

    daemon.block();

    info!("lyra is stopping");

    Ok(())
}

#[derive(clap::Args)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(long, value_parser)]
    //#[clap(description = "config file to load by the daemon")]
    config: Option<std::path::PathBuf>,

    #[clap(long, value_parser)]
    //#[clap(description = "type of progress to display")],
    console: Option<console::Mode>,
}
