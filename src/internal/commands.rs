use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
    future::Future,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
};

use crate::internal::parser::Command;
use crate::internal::server::ServerMetadata;
use crate::internal::server_info;
use crate::internal::storage::{DBEntry, DBEntryValueType, STORAGE};
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
        config => config,
    };
}

lazy_static! {
    pub static ref COMMANDS_REGISTRY: CommandsReg = register_commands! {
        echo => echo,
        get => get,
        info => info,
        ping => ping,
        replconf => replconf,
        set => set,
        wait => wait,
        config => config,
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
    _write_stream_and_flush(&stream, res.as_str()).await;
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
        let res = "+PONG\r\n";
        _write_stream_and_flush(&stream, res).await;
    } else if metadata.role == 1 {
        let command_size = command.raw_cmd.len() as u64;
        metadata
            .master_repl_offset
            .fetch_add(command_size, Ordering::SeqCst);
    }
}

async fn wait(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let metadata = server_metadata.read().await;
    let num_replicas: usize = command
        .args
        .first()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let ms_timeout: u64 = command
        .args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let target = metadata.master_repl_offset.load(Ordering::SeqCst);

    if target == 0 {
        let res = format!(":{}\r\n", metadata.broadcast.receiver_count());
        _write_stream_and_flush(&stream, res.as_str()).await;
    } else {
        // Broadcast REPLCONF GETACK * to all replicas
        let getack_cmd = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".to_string();
        _sync_replicas(getack_cmd, &metadata.broadcast).await;

        // Wait for responses with timeout
        let timeout = tokio::time::sleep(tokio::time::Duration::from_millis(ms_timeout));
        tokio::pin!(timeout);

        let count = loop {
            let c = metadata
                .replica_offsets
                .iter()
                .filter(|o| o.load(Ordering::SeqCst) >= target)
                .count();
            if c >= num_replicas {
                break c;
            }

            tokio::select! {
                _ = metadata.ack_notify.notified() => continue,
                _ = &mut timeout => break c,
            }
        };
        let res = format!(":{}\r\n", count);
        _write_stream_and_flush(&stream, res.as_str()).await;
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
    let res = format!("${}\r\n{}\r\n", echo_arg.len(), echo_arg);
    _write_stream_and_flush(&stream, res.as_str()).await;
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
                let res = "+OK\r\n";
                _write_stream_and_flush(&stream, res).await;
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
    let storage = STORAGE.lock().await;
    let res = match storage.get(key) {
        Some(val) => format_result(val),
        None => "$-1\r\n".to_string(),
    };
    _write_stream_and_flush(&stream, res.as_str()).await;
}

async fn info(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let args = command.args;
    let info_section = args.first().unwrap();
    let metadata = server_metadata.read().await;
    if info_section == "replication" {
        match server_info::get_server_info(&metadata) {
            Ok(res) => {
                _write_stream_and_flush(&stream, res.as_str()).await;
            }
            Err(_) => {
                eprintln!("Cannot return replication info");
            }
        }
    }
}

async fn config(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let metadata = server_metadata.read().await;
    let operation = command.args.first().unwrap();
    if operation.to_lowercase() == "get" {
        let config_name = command.args.get(1).unwrap();
        let config_val = match config_name.to_lowercase().as_str() {
            "dir" => metadata.dir.to_string_lossy().to_string(),
            "dbfilename" => metadata.dbfilename.clone(),
            _ => String::new(),
        };
        let res = format!(
            "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            config_name.len(),
            config_name,
            config_val.len(),
            config_val
        );
        _write_stream_and_flush(&stream, res.as_str()).await;
    }
}

fn format_result(value: &DBEntry) -> String {
    match value.to_string() {
        Ok(value) => format!("${}\r\n{}\r\n", value.len(), value),
        Err(_) => "$-1\r\n".to_string(),
    }
}

async fn _write_stream_and_flush(stream: &Arc<RwLock<TcpStream>>, res: &str) {
    let mut stream = stream.write().await;
    let _ = stream
        .write_all(res.as_bytes())
        .await
        .map_err(|e| format!("Error while writing to the stream: {}", e));
    let _ = stream
        .flush()
        .await
        .map_err(|e| format!("Error while flushing the stream: {}", e));
}
