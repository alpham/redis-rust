use crate::internal::{commands, parser};
use std::{error::Error, sync::Arc};

use super::cli::Replicaof;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, Mutex, RwLock},
};

#[derive(Debug)]
pub struct ServerMetadata {
    pub role: u8,
    pub master_replid: String,
    pub master_repl_offset: u8,
    pub broadcast: broadcast::Sender<Arc<Vec<u8>>>,
    _port: u16,
    _host: String,
}

fn get_master_replid() -> String {
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()
}

fn get_master_repl_offset() -> u8 {
    0
}

pub async fn start_server(
    host: &str,
    port: u16,
    replicaof: Option<Replicaof>,
) -> Result<(), Box<dyn Error>> {
    let address = format!("{}:{}", host, port);
    let listener = TcpListener::bind(address).await?;
    let metadata = Arc::new(RwLock::new(ServerMetadata {
        _port: port,
        _host: host.to_string(),

        master_replid: get_master_replid(),
        master_repl_offset: get_master_repl_offset(),
        role: match replicaof {
            Some(_) => 1,
            None => 0,
        },
        // The `0` here is to get the sender only, we don't need the receiver here.
        broadcast: broadcast::channel(16).0,
    }));

    // Configuring the replica.
    configure_replica(&replicaof).await;

    while let Ok((stream, _)) = listener.accept().await {
        let cloned_metadata = Arc::clone(&metadata);

        tokio::spawn(async move {
            handle_client(stream, cloned_metadata).await;
        });
    }
    Ok(())
}

async fn configure_replica(replicaof: &Option<Replicaof>) {
    match replicaof {
        Some(replicaof) => {
            if let Ok(mut stream) =
                TcpStream::connect(format!("{}:{}", replicaof.host, replicaof.port)).await
            {
                let _ = ping_master(&mut stream).await;
                let _ = replicaconf_master(&mut stream).await;
                let _ = psync_master(&mut stream).await;
                let _ = stream.flush().await;
            } else {
                return;
            }
        }
        None => return,
    }
}

async fn handle_client(stream: TcpStream, server_metadata: Arc<RwLock<ServerMetadata>>) {
    let mut buf = [0u8; 255];
    // let metadata = &*server_metadata;
    let stream = Arc::new(Mutex::new(stream));
    loop {
        let mut locked_stream = stream.lock().await;
        match locked_stream.read(&mut buf).await {
            Ok(0) => continue,
            Ok(length) => {
                drop(locked_stream);
                let command = parser::parse_request(&buf[..length]).unwrap();
                let stream_clone = Arc::clone(&stream);
                commands::run_command(stream_clone, command, &server_metadata).await
            }
            Err(_) => break,
        }
    }
}

async fn _send_message_to_master(
    stream: &mut TcpStream,
    message: String,
) -> Result<String, String> {
    let mut result = [0; 32];
    let _ = stream.write_all(message.as_bytes()).await;
    if let Ok(_) = stream.flush().await {
        let _ = stream.read(result.as_mut()).await;
        let response = String::from_utf8_lossy(result.as_ref()).to_string();
        Ok(response)
    } else {
        Err("Cannot parse the response from server.".to_string())
    }
}

async fn ping_master(stream: &mut TcpStream) -> Result<String, String> {
    let message = "*1\r\n$4\r\nPING\r\n";
    _send_message_to_master(stream, message.to_string()).await
}

async fn replicaconf_master(stream: &mut TcpStream) -> Result<String, String> {
    let listening_port_msg = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
    let capa_psync2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";

    if let Ok(_) = _send_message_to_master(stream, listening_port_msg.to_string()).await {
        _send_message_to_master(stream, capa_psync2.to_string()).await
    } else {
        Err("Cannot configure listening port of the replica..".to_string())
    }
}

async fn psync_master(stream: &mut TcpStream) -> Result<String, String> {
    let psync_msg = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    _send_message_to_master(stream, psync_msg.to_string()).await
}
