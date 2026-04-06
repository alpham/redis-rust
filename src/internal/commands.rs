use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
    future::Future,
    io::{Error as IOError, ErrorKind},
    pin::Pin,
    sync::{atomic::Ordering, Arc},
};

use crate::internal::parser::Command;
use crate::internal::server::ServerMetadata;
use crate::internal::server_info;
use crate::internal::storage::{DBEntry, DBEntryValueType, STORAGE};
use hex;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{broadcast, RwLock},
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

pub type CommandFn = Arc<
    dyn for<'a> Fn(
            Arc<RwLock<TcpStream>>,
            Command,
            &'a Arc<RwLock<ServerMetadata>>,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
        + Send
        + Sync,
>;

#[derive(Default)]
pub struct CommandsReg {
    commands: HashMap<&'static str, CommandFn>,
}

impl PartialEq for CommandsReg {
    fn eq(&self, other: &Self) -> bool {
        self.commands.keys().eq(other.commands.keys())
    }
}

macro_rules! register_commands {
    ($($name:ident => $func:ident),* $(,)?) => {
        {
            let mut m: CommandsReg = CommandsReg::default();
            $(
                m.commands.insert(
                    stringify!($name),
                    Arc::new(move |stream, command, metadata| Box::pin($func(stream, command, metadata)))
                );
            )*
            m
        }
    };
}

lazy_static! {
    pub static ref MASTER_REPLICA_COMMANDS: CommandsReg = register_commands! {
        echo => echo,
        get => get,
        info => info,
        ping => ping,
        replconf => replconf,
        set => set,
    };
}

lazy_static! {
    pub static ref COMMANDS_REGISTRY: CommandsReg = register_commands! {
        echo => echo,
        get => get,
        info => info,
        ping => ping,
        psync => psync,
        replconf => replconf,
        set => set,
    };
}

pub async fn run_command(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
    command_reg: &CommandsReg,
) {
    match command_reg
        .commands
        .get(command.cmd.to_lowercase().as_str())
        .ok_or_else(|| CommandError::CommandNotFound(command.cmd.clone()))
    {
        Ok(function) => function(stream, command, server_metadata).await,
        Err(_) => eprintln!("Cannot find function with name \"{}\"", command.cmd),
    }
}

async fn psync(
    arc_stream: Arc<RwLock<TcpStream>>,
    _command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let mut stream = arc_stream.write().await;
    let metadata = server_metadata.read().await;
    let repl_offset = metadata.master_repl_offset.load(Ordering::SeqCst);
    let res = format!("+FULLRESYNC {} {}\r\n", metadata.master_replid, repl_offset);
    let _ = stream.write_all(res.as_bytes()).await;
    let _ = stream.flush().await;

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
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    match command.args.first() {
        Some(sub) => match sub.to_lowercase().as_str() {
            "getack" => _replconf_getack(stream, command, _server_metadata).await,
            "listening-port" => _replconf_listening_port(stream, command, _server_metadata).await,
            "capa" => _replconf_capa(stream, command, _server_metadata).await,
            _ => _replconf(stream, command, _server_metadata).await,
        },
        None => _replconf(stream, command, _server_metadata).await,
    }
}

async fn _replconf(
    stream: Arc<RwLock<TcpStream>>,
    _command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let res = "+OK\r\n".to_string();
    let mut stream = stream.write().await;
    let _ = stream.write_all(res.as_bytes()).await;
    let _ = stream.flush().await;
}

async fn _replconf_capa(
    stream: Arc<RwLock<TcpStream>>,
    _command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    // TODO: define capa functionality.
    _replconf(stream, _command, _server_metadata).await
}

async fn _replconf_listening_port(
    stream: Arc<RwLock<TcpStream>>,
    _command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    // TODO: define listening port functionality.
    _replconf(stream, _command, _server_metadata).await
}

async fn _replconf_getack(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let metadata = server_metadata.read().await;
    let repl_offset = metadata.master_repl_offset.load(Ordering::SeqCst);
    let offset_str = repl_offset.to_string();
    let res = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
        offset_str.len(),
        offset_str
    );
    let mut stream = stream.write().await;
    let _ = stream.write_all(res.as_bytes()).await;
    let _ = stream.flush().await;
    let command_size = command.raw_cmd.len() as u64;
    metadata
        .master_repl_offset
        .fetch_add(command_size, Ordering::SeqCst);
}

async fn ping(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let metadata = server_metadata.read().await;
    if metadata.role == 0 {
        let res = "+PONG\r\n".to_string();
        let mut stream = stream.write().await;
        let _ = stream.write_all(res.as_bytes()).await;
        let _ = stream.flush().await;
    } else if metadata.role == 1 {
        let command_size = command.raw_cmd.len() as u64;
        metadata
            .master_repl_offset
            .fetch_add(command_size, Ordering::SeqCst);
    }
}

async fn echo(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let args = command.args;
    let echo_arg = match args.first() {
        Some(val) => val,
        None => "",
    };
    let res = format!("${}\r\n{}\r\n", echo_arg.len(), echo_arg).to_string();
    let mut stream = stream.write().await;
    let _ = stream.write_all(res.as_bytes()).await;
    let _ = stream.flush().await;
}

async fn set(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let metadata = server_metadata.read().await;
    let args = command.args;
    let key = args.first().unwrap();
    match args
        .get(1)
        .ok_or_else(|| CommandError::InvalidArgument("Missing arguments".to_string()))
    {
        Ok(value) => {
            let mut db_entry = DBEntry::from_string(value, DBEntryValueType::StringType);
            if args.len() > 2 && args[2].to_lowercase() == "px" {
                let _ = db_entry.set_ttl(args.get(3));
            }
            let mut storage = STORAGE.lock().await;
            storage.insert(key.to_string(), db_entry);
            if metadata.role == 0 {
                let res = "+OK\r\n".to_string();
                let mut stream = stream.write().await;
                let _ = stream.write_all(res.as_bytes()).await;
                let _ = stream.flush().await;
            }
            let command_size = command.raw_cmd.len() as u64;
            _sync_replicas(command.raw_cmd, &metadata.broadcast).await;

            metadata
                .master_repl_offset
                .fetch_add(command_size, Ordering::SeqCst);
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
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let args = command.args;
    let key = args.first().unwrap();
    let mut stream = stream.write().await;
    let storage = STORAGE.lock().await;
    let response = match storage.get(key) {
        Some(val) => format_result(val),
        None => "$-1\r\n".to_string(),
    };
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
}

async fn info(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let args = command.args;
    let info_section = args.first().unwrap();
    let mut stream = stream.write().await;
    let metadata = server_metadata.read().await;
    if info_section == "replication" {
        match server_info::get_server_info(&metadata) {
            Ok(res) => {
                let _ = stream.write_all(res.as_bytes()).await;
                let _ = stream.flush().await;
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
