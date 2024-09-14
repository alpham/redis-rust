use std::num::ParseIntError;

use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(
    version = "1.0",
    about = "Running redis server that is written in rust"
)]
pub struct CliArgs {
    #[arg(short = 'p', long = "port", default_value = "6379", value_parser=|arg: &str| -> Result<u16, ParseIntError> {arg.parse::<u16>()})]
    pub port: u16,

    #[arg(long = "replicaof", required = false)]
    pub replicaof: Option<Replicaof>,
    // #[arg(long = "host", default_value = "127.0.0.1")]
    // pub host: &'cli str
}

#[derive(Debug, Clone)]
pub struct Replicaof {
    pub host: String,
    pub port: u16,
}

#[derive(Debug)]
pub enum ParseReplicaofError {
    #[allow(dead_code)]
    InvalidArguments,
    #[allow(dead_code)]
    ParseIntError(ParseIntError),
}

impl From<ParseIntError> for ParseReplicaofError {
    fn from(e: ParseIntError) -> Self {
        ParseReplicaofError::ParseIntError(e)
    }
}

impl From<String> for Replicaof {
    fn from(s: String) -> Self {
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() < 2 {
            return Replicaof {
                host: "".to_string(),
                port: 0,
            };
        }
        let host = parts.get(0).copied().unwrap();
        let port_str = parts.get(1).copied().unwrap();
        let port: u16 = match port_str.parse::<u16>() {
            Ok(p) => p,
            Err(_) => 0,
        };

        Replicaof {
            host: host.to_string(),
            port,
        }
    }
}
