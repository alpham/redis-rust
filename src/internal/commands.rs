use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
    future::Future,
    io::{Error as IOError, ErrorKind},
    pin::Pin,
    sync::Arc
};

use crate::internal::parser::Command;
use crate::internal::server::ServerMetadata;
use crate::internal::server_info;
use crate::internal::storage::{DBEntry, DBEntryValueType, STORAGE};
use hex;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{broadcast, Mutex, RwLock},
};

#[derive(Debug)]
pub enum CommandError {
    CommandNotFound(String),
    InvalidArgument(String),
    StorageError(String),
    _ErrorWhileExecution(String),
}

impl Error for CommandError {}

impl Display for CommandError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandError::CommandNotFound(cmd) => write!(f, "Command not found: {}", cmd),
            CommandError::InvalidArgument(msg) => write!(f, "Invalid arguments: {}", msg),
            CommandError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            CommandError::_ErrorWhileExecution(msg) => {
                write!(f, "Error while executing the command: {}", msg)
            }
        }
    }
}

type CommandFn = Arc<
    dyn for<'a> Fn(
            Arc<Mutex<TcpStream>>,
            Command,
            &'a Arc<RwLock<ServerMetadata>>,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
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
                    Arc::new(move |stream, command, metadata| Box::pin($func(stream, command, metadata)))
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
    command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    match COMMANDS_REGISTRY
        .get(command.cmd.to_lowercase().as_str())
        .ok_or_else(|| CommandError::CommandNotFound(command.cmd.clone()))
    {
        Ok(function) => function(stream, command, server_metadata).await,
        Err(_) => eprintln!("Cannot find function with name \"{}\"", command.cmd),
    }
}

async fn psync(
    arc_stream: Arc<Mutex<TcpStream>>,
    _command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let mut stream = arc_stream.lock().await;
    let metadata = server_metadata.read().await;
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
    loop {
        let mut receiver = metadata.broadcast.subscribe();
        let result = receiver.recv().await.unwrap();
        let _ = stream.write_all(result.as_slice()).await;
        let _ = stream.flush().await;
    }
}

async fn replconf(
    arc_stream: Arc<Mutex<TcpStream>>,
    _command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let res = "+OK\r\n".to_string();
    let mut stream = arc_stream.lock().await;
    let _ = stream.write_all(res.as_bytes()).await;
    let _ = stream.flush().await;
}

async fn ping(
    arc_stream: Arc<Mutex<TcpStream>>,
    _command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let res = "+PONG\r\n".to_string();
    let mut stream = arc_stream.lock().await;
    let _ = stream.write_all(res.as_bytes()).await;
    let _ = stream.flush().await;
}

async fn echo(
    arc_stream: Arc<Mutex<TcpStream>>,
    command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let args = command.args;
    let res = format!("+{}\r\n", args.get(0).unwrap()).to_string();
    let mutex_stream = &*arc_stream;
    let mut stream = mutex_stream.lock().await;
    let _ = stream.write_all(res.as_bytes()).await;
    let _ = stream.flush().await;
}

async fn set(
    arc_stream: Arc<Mutex<TcpStream>>,
    command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let metadata = server_metadata.read().await;
    let args = command.args;
    let key = args.get(0).unwrap();
    match args
        .get(1)
        .ok_or_else(|| CommandError::InvalidArgument("Missing arguments".to_string()))
    {
        Ok(value) => {
            let mut db_entry = DBEntry::from_string(value, DBEntryValueType::StringType);
            if args.len() > 2 {
                if args[2] == "px".to_string() {
                    let _ = db_entry.set_ttl(args.get(3));
                }
            }
            let mut storage = STORAGE.lock().await;
            storage.insert(key.to_string(), db_entry);
            let res = "+OK\r\n".to_string();
            let mut stream = arc_stream.lock().await;
            let _ = stream.write_all(res.as_bytes()).await;
            let _ = stream.flush().await;
            _sync_replicas(command.raw_cmd, &metadata.broadcast).await;
        }
        Err(_) => eprintln!("Error setting a value"),
    }
}

async fn _sync_replicas(raw_command: String, sender: &broadcast::Sender<Arc<Vec<u8>>>) {
    if sender.receiver_count() > 0 {
        let v = Arc::new(raw_command.into_bytes());
        let _ = sender.send(v);
    }
}

async fn get(
    arc_stream: Arc<Mutex<TcpStream>>,
    command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let args = command.args;
    let key = args.get(0).unwrap();
    let mut stream = arc_stream.lock().await;
    let storage = STORAGE.lock().await;
    match storage.get(key) {
        Some(val) => {
            let res = format_result(val);
            let _ = stream.write_all(res.as_bytes()).await;
            let _ = stream.flush();
        }
        None => eprintln!("$-1\r\n"),
    }
}

async fn info(
    arc_stream: Arc<Mutex<TcpStream>>,
    command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let args = command.args;
    let info_section = args.get(0).unwrap();
    let mut stream = arc_stream.lock().await;
    let metadata = server_metadata.read().await;
    if info_section == "replication" {
        match server_info::get_server_info(&metadata) {
            Ok(res) => {
                let _ = stream.write_all(res.as_bytes()).await;
                let _ = stream.flush();
            }
            Err(_) => {
                eprintln!("Cannot return replication info");
            }
        }
    }
}

fn format_result(value: &DBEntry) -> String {
    match value.to_string() {
        Ok(value) => format!("${}\r\n{}\r\n", value.len(), value),
        Err(_) => "$-1\r\n".to_string(),
    }
}
