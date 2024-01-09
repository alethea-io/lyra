use clap::Parser;
use std::process;

mod console;
mod daemon;

#[derive(Parser)]
#[clap(name = "Lyra")]
#[clap(bin_name = "lyra")]
#[clap(author, version, about, long_about = None)]
enum Lyra {
    Daemon(daemon::Args),
}

fn main() {
    let args = Lyra::parse();

    let result = match args {
        Lyra::Daemon(x) => daemon::run(&x),
    };

    if let Err(err) = &result {
        eprintln!("ERROR: {:#?}", err);
        process::exit(1);
    }

    process::exit(0);
}
