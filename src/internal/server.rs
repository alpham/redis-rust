use std::{
    thread,
    sync::Arc,
    net::{TcpListener, TcpStream},
    io::{Read, Write},
    error::Error
};
use crate::internal:: {
    commands, parser
};

use super::cli::Replicaof;


#[derive(Debug)]
pub struct ServerMetadata {
    pub role: u8,
    pub master_replid: String,
    pub master_repl_offset: u8,
    _port: u16,
    _host: String
}


fn get_master_replid() -> String {
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()
}

fn get_master_repl_offset() -> u8 {
    0
}

pub fn start_server(host: &str, port: u16, replicaof: Option<Replicaof>) -> Result<(), Box<dyn Error>> {
    let address = format!("{}:{}", host, port);
    let listener = TcpListener::bind(address).unwrap();
    let metadata = Arc::new(ServerMetadata {
        _port: port,
        _host: host.to_string(),

        master_replid: get_master_replid(),
        master_repl_offset: get_master_repl_offset(),
        role: match replicaof {
            Some(_) => 1,
            None => 0,
        }
    });
    for stream in listener.incoming() {
        let cloned_metadata = Arc::clone(&metadata);
        thread::spawn(|| match stream {
            Ok(stream) => {
                handle_client(stream, cloned_metadata);
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        });
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream, server_metadata: Arc<ServerMetadata>) {
    let mut buf = [0u8; 255];
    let metadata = &*server_metadata;
    while let Ok(_) = stream.read(&mut buf) {
        let command = parser::parse_request(&buf).unwrap();
        let result = match commands::run_command(command, metadata) {
            Ok(res) => res,
            Err(e) => format!("+{}\r\n", e),
        };

        stream.write_all(result.as_bytes()).unwrap();
    }
}
