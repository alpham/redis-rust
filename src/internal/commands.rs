use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
    future::Future,
    io::{Error as IOError, ErrorKind},
    pin::Pin,
    sync::Arc,
};

use crate::internal::parser::Command;
use crate::internal::server::ServerMetadata;
use crate::internal::server_info;
use crate::internal::storage::{DBEntry, DBEntryValueType, STORAGE};
use hex;
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex};

#[derive(Debug)]
pub enum CommandError {
    CommandNotFound(String),
    InvalidArgument(String),
    StorageError(String),
    ErrorWhileExecution(String),
}

impl Error for CommandError {}

impl Display for CommandError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandError::CommandNotFound(cmd) => write!(f, "Command not found: {}", cmd),
            CommandError::InvalidArgument(msg) => write!(f, "Invalid arguments: {}", msg),
            CommandError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            CommandError::ErrorWhileExecution(msg) => {
                write!(f, "Error while executing the command: {}", msg)
            }
        }
    }
}

type CommandFn = Arc<
    dyn for<'a> Fn(
            Arc<Mutex<TcpStream>>,
            &'a Vec<String>,
            &'a Mutex<ServerMetadata>,
        )
            -> Pin<Box<dyn Future<Output = Result<String, CommandError>> + Send + 'a>>
        + Send
        + Sync,
>;

macro_rules! register_commands {
    ($($name:ident => $func:ident),* $(,)?) => {
        {
            let mut m: HashMap<&'static str, CommandFn> = HashMap::new();
            $(
                m.insert(
                    stringify!($name),
                    Arc::new(move |stream, args, metadata| Box::pin($func(stream, args, metadata)))
                );
            )*
            m
        }
    };
}

lazy_static! {
    static ref COMMANDS_REGISTRY: HashMap<&'static str, CommandFn> = register_commands! {
        ping => ping,
        echo => echo,
        get => get,
        set => set,
        info => info,
        replconf => replconf,
        psync => psync
    };
}

pub async fn run_command(
    stream: Arc<Mutex<TcpStream>>,
    command: &Command,
    server_metadata: &Mutex<ServerMetadata>,
) -> Result<String, CommandError> {
    let function = COMMANDS_REGISTRY
        .get(command.cmd.to_lowercase().as_str())
        .ok_or_else(|| CommandError::CommandNotFound(command.cmd.clone()))?;
    function(stream, &command.args, server_metadata).await
}

async fn psync(
    arc_stream: Arc<Mutex<TcpStream>>,
    _args: &Vec<String>,
    server_metadata: &Mutex<ServerMetadata>,
) -> Result<String, CommandError> {
    let mut stream = arc_stream.lock().await;
    let metadata = server_metadata.lock().await;
    let res = format!(
        "+FULLRESYNC {} {}\r\n",
        metadata.master_replid, metadata.master_repl_offset
    );
    let _ = stream.write_all(res.as_bytes()).await;
    let _ = stream.flush();

    let rdb_file = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
    .map_err(|decode_err| IOError::new(ErrorKind::InvalidData, decode_err.to_string())).unwrap();

    let _ = stream
        .write_all(format!("${}\r\n", rdb_file.len()).as_bytes())
        .await;
    let _ = stream.write_all(rdb_file.as_slice()).await;
    let _ = stream.flush().await;
    Ok(res)
}

async fn replconf(
    arc_stream: Arc<Mutex<TcpStream>>,
    _args: &Vec<String>,
    _server_metadata: &Mutex<ServerMetadata>,
) -> Result<String, CommandError> {
    let res = "+OK\r\n".to_string();
    let mut stream = arc_stream.lock().await;
    let _ = stream.write_all(res.as_bytes()).await;
    let _ = stream.flush().await;

    Ok(res)
}

async fn ping(
    arc_stream: Arc<Mutex<TcpStream>>,
    _args: &Vec<String>,
    _server_metadata: &Mutex<ServerMetadata>,
) -> Result<String, CommandError> {
    let res = "+PONG\r\n".to_string();
    let mut stream = arc_stream.lock().await;
    let _ = stream.write_all(res.as_bytes()).await;
    let _ = stream.flush().await;
    Ok(res)
}

async fn echo(
    arc_stream: Arc<Mutex<TcpStream>>,
    args: &Vec<String>,
    _server_metadata: &Mutex<ServerMetadata>,
) -> Result<String, CommandError> {
    let res = format!("+{}\r\n", args.get(0).unwrap()).to_string();
    let mutex_stream = &*arc_stream;
    let mut stream = mutex_stream.lock().await;
    let _ = stream.write_all(res.as_bytes()).await;
    let _ = stream.flush().await;
    Ok(res)
}

async fn set(
    arc_stream: Arc<Mutex<TcpStream>>,
    args: &Vec<String>,
    server_metadata: &Mutex<ServerMetadata>,
) -> Result<String, CommandError> {
    let key = args.get(0).unwrap();
    let value = args
        .get(1)
        .ok_or_else(|| CommandError::InvalidArgument("Missing arguments".to_string()))?;
    let mut db_entry = DBEntry::from_string(value, DBEntryValueType::StringType);
    if args.len() > 2 {
        if args[2] == "px".to_string() {
            db_entry.set_ttl(args.get(3))?;
        }
    }
    let mut storage = STORAGE.lock().await;
    storage.insert(key.to_string(), db_entry);
    let metadata = &*server_metadata.lock().await;
    let _ = _sync_replicas("SET", args, metadata);
    let res = "+OK\r\n".to_string();
    let mut stream = arc_stream.lock().await;
    let _ = stream.write_all(res.as_bytes()).await;
    let _ = stream.flush().await;
    Ok(res)
}

async fn _sync_replicas(
    _cmd: &str,
    _args: &Vec<String>,
    _server_metadata: &ServerMetadata,
) -> Result<String, CommandError> {
    // TODO: iterate all the replicas and send the same command to them.
    // let replicas = server_metadata.replicas.lock().await;
    // for replica in replicas.iter() {
    //     let mut command = String::from(format!(
    //         "*{}\\r\\n${}\\r\\n{}\\r\\n",
    //         args.len() + 1,
    //         cmd.len(),
    //         cmd
    //     ));
    //     for arg in args {
    //         command.push_str(format!("${}\\r\\n{}\\r\\n", arg.len(), arg).as_str());
    //     }
    //     println!("trying to connect to {}:{}", replica.host, replica.port);
    //     if let Ok(mut stream) =
    //         TcpStream::connect(format!("{}:{}", replica.host, replica.port)).await
    //     {
    //         stream.write_all(command.as_bytes()).await;
    //         let mut buffer = [0; 512];
    //         let bytes_read = stream.read(&mut buffer).await;
    //         println!(
    //             "Replica {}:{} responded with {}",
    //             replica.host,
    //             replica.port,
    //             String::from_utf8_lossy(&buffer[..bytes_read])
    //         );
    //     } else {
    //         println!("{}:{} cannot be synced", replica.host, replica.port);
    //     }
    // }
    Ok("".to_string())
}

async fn get(
    arc_stream: Arc<Mutex<TcpStream>>,
    args: &Vec<String>,
    _server_metadata: &Mutex<ServerMetadata>,
) -> Result<String, CommandError> {
    let key = args.get(0).unwrap();
    let mut stream = arc_stream.lock().await;
    let storage = STORAGE.lock().await;
    match storage.get(key) {
        Some(val) => {
            let res = format_result(val);
            let _ = stream.write_all(res.as_bytes()).await;
            let _ = stream.flush();
            Ok(res)
        }
        None => Err(CommandError::StorageError("$-1\r\n".to_string())),
    }
}

async fn info(
    arc_stream: Arc<Mutex<TcpStream>>,
    args: &Vec<String>,
    server_metadata: &Mutex<ServerMetadata>,
) -> Result<String, CommandError> {
    let info_section = args.get(0).unwrap();
    let response = String::new();
    let mut stream = arc_stream.lock().await;
    let metadata = server_metadata.lock().await;
    if info_section == "replication" {
        match server_info::get_server_info(&metadata) {
            Ok(res) => {
                let _ = stream.write_all(res.as_bytes()).await;
                let _ = stream.flush();
            }
            Err(_) => {
                return Err(CommandError::ErrorWhileExecution(
                    "Cannot return replication info".to_string(),
                ))
            }
        }
    }

    Ok(response)
}

fn format_result(value: &DBEntry) -> String {
    match value.to_string() {
        Ok(value) => format!("${}\r\n{}\r\n", value.len(), value),
        Err(_) => "$-1\r\n".to_string(),
    }
}
