use crate::internal::{commands, parser};
use std::{
    error::Error,
    sync::{atomic::AtomicU64, Arc},
};

use super::cli::Replicaof;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, RwLock},
};

#[derive(Debug)]
pub struct ServerMetadata {
    pub role: u8,
    pub master_replid: String,
    pub master_repl_offset: AtomicU64,
    pub broadcast: broadcast::Sender<Arc<Vec<u8>>>,
    _port: u16,
    _host: String,
}

fn get_master_replid() -> String {
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()
}

fn get_master_repl_offset() -> AtomicU64 {
    AtomicU64::new(0)
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
    configure_replica(&replicaof, &metadata).await;

    while let Ok((stream, _)) = listener.accept().await {
        let cloned_metadata = Arc::clone(&metadata);
        let stream = Arc::new(RwLock::new(stream));
        tokio::spawn(async move {
            handle_client(stream, &cloned_metadata, None).await;
        });
    }
    Ok(())
}

async fn configure_replica(replicaof: &Option<Replicaof>, metadata: &Arc<RwLock<ServerMetadata>>) {
    if let Some(replicaof) = replicaof {
        if let Ok(mut stream) =
            TcpStream::connect(format!("{}:{}", replicaof.host, replicaof.port)).await
        {
            let _ = ping_master(&mut stream).await;
            let _ = replicaconf_master(&mut stream).await;
            let _ = psync_master(&mut stream).await;
            if let Err(e) = consume_rdb_file(&mut stream).await {
                eprintln!("Failed to consume RDB file: {}", e);
                return;
            }
            let stream = Arc::new(RwLock::new(stream));
            let cloned_metadata = Arc::clone(metadata);

            tokio::spawn(async move {
                handle_client(
                    stream,
                    &cloned_metadata,
                    Some(&commands::MASTER_REPLICA_COMMANDS),
                )
                .await;
            });
        }
    }
}

async fn consume_rdb_file(stream: &mut TcpStream) -> Result<(), String> {
    let mut byte = [0u8; 1];
    loop {
        stream
            .read_exact(&mut byte)
            .await
            .map_err(|e| e.to_string())?;
        if byte[0] == b'$' {
            break;
        }
    }

    let mut len_bytes = Vec::new();
    loop {
        stream
            .read_exact(&mut byte)
            .await
            .map_err(|e| e.to_string())?;
        if byte[0] == b'\r' {
            stream
                .read_exact(&mut byte)
                .await
                .map_err(|e| e.to_string())?;
            break;
        }
        len_bytes.push(byte[0]);
    }

    let len: usize = std::str::from_utf8(&len_bytes)
        .map_err(|e| e.to_string())?
        .parse()
        .map_err(|_| "Invalid RDB length".to_string())?;

    let mut rdb = vec![0u8; len];
    stream
        .read_exact(&mut rdb)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn handle_client(
    stream: Arc<RwLock<TcpStream>>,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
    command_registry: Option<&commands::CommandsReg>,
) {
    let mut buf = [0u8; 255];
    let command_reg = command_registry.unwrap_or(&commands::COMMANDS_REGISTRY);
    loop {
        let mut locked_stream = stream.write().await;
        match locked_stream.read(&mut buf).await {
            Ok(0) => continue,
            Ok(length) => {
                drop(locked_stream);
                let commands = parser::parse_request(&buf[..length]).unwrap();
                for command in commands {
                    let stream_clone = Arc::clone(&stream);
                    commands::run_command(stream_clone, command, server_metadata, command_reg).await
                }
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
    if stream.flush().await.is_ok() {
        let _ = stream.read(result.as_mut()).await;
        let response = String::from_utf8_lossy(result.as_ref()).to_string();
        let _ = stream.flush().await;
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

    if _send_message_to_master(stream, listening_port_msg.to_string())
        .await
        .is_ok()
    {
        _send_message_to_master(stream, capa_psync2.to_string()).await
    } else {
        Err("Cannot configure listening port of the replica..".to_string())
    }
}

async fn psync_master(stream: &mut TcpStream) -> Result<String, String> {
    let psync_msg = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    _send_message_to_master(stream, psync_msg.to_string()).await
}
