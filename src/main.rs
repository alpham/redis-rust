#[macro_use]
extern crate lazy_static;
mod internal;

use clap::Parser;
use std::error::Error;

use crate::internal::{cli, server};

fn main() -> Result<(), Box<dyn Error>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.

    let args = cli::CliArgs::parse();
    // let address = format!("{}:{}", "127.0.0.1", args.port);
    let _server_metadata = server::start_server("127.0.0.1", args.port, args.replicaof).unwrap();

    Ok(())
}
