use crate::internal::{commands, parser};
use std::{
    error::Error,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
};

use super::cli::Replicaof;

#[derive(Debug)]
pub struct ServerMetadata {
    pub role: u8,
    pub master_replid: String,
    pub master_repl_offset: u8,
    _port: u16,
    _host: String,
}

fn get_master_replid() -> String {
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()
}

fn get_master_repl_offset() -> u8 {
    0
}

pub fn start_server(
    host: &str,
    port: u16,
    replicaof: Option<Replicaof>,
) -> Result<(), Box<dyn Error>> {
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
        },
    });

    // Configuring the replica.
    configure_replica(&replicaof);

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

fn configure_replica(replicaof: &Option<Replicaof>) {
    match replicaof {
        Some(replicaof) => {
            if let Ok(mut stream) = TcpStream::connect(format!("{}:{}", replicaof.host, replicaof.port)) {
               let _ = ping_master(&mut stream);
               let _ = replicaconf_master(&mut stream);
               let _ = psync_master(&mut stream);
            } else {
                return
            }
        },
        None => return
    }
}

fn handle_client(mut stream: TcpStream, server_metadata: Arc<ServerMetadata>) {
    let mut buf = [0u8; 255];
    let metadata = &*server_metadata;
    while let Ok(count) = stream.read(&mut buf) {
        if count == 0 {
            continue;
        }
        let command = parser::parse_request(&buf).unwrap();
        if let Err(command_err) = commands::run_command(&mut stream, &command, metadata) {
            stream.write_all(format!("+{}\r\n", command_err).as_bytes()).unwrap();
        };
    }
}

fn _send_message_to_master(stream: &mut TcpStream, message: String) -> Result<String, String> {
    let mut result = [0; 32];
    stream.write_all(message.as_bytes()).unwrap();
    if let Ok(_) = stream.flush() {
        let _ = stream.read(result.as_mut());
        let response = String::from_utf8_lossy(result.as_ref()).to_string();
        Ok(response)
    } else {
        Err("Cannot parse the response from server.".to_string())
    }
}

fn ping_master(stream: &mut TcpStream) -> Result<String, String> {
    let message = "*1\r\n$4\r\nPING\r\n";
    _send_message_to_master(stream, message.to_string())
}

fn replicaconf_master(stream: &mut TcpStream) -> Result<String, String>{
    let listening_port_msg = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
    let capa_psync2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";

    if let Ok(_) = _send_message_to_master(stream, listening_port_msg.to_string()) {
        _send_message_to_master(stream, capa_psync2.to_string())
    } else {
        Err("Cannot configure listening port of the replica..".to_string())
    }
}

fn psync_master(stream: &mut TcpStream) -> Result<String, String> {
    let psync_msg = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    _send_message_to_master(stream, psync_msg.to_string())
}
