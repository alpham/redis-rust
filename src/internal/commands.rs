use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
    future::Future,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use crate::internal::server::ServerMetadata;
use crate::internal::server_info;
use crate::internal::storage::{with_storage, DBEntry, STREAM_ADDED};
use crate::internal::{
    parser::Command,
    types::{StreamId, StreamType},
};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{broadcast, RwLock},
    time::timeout,
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

impl CommandError {
    fn as_resp(&self) -> String {
        match self {
            CommandError::InvalidArgument(st) => format!("-ERR {}\r\n", st),
            CommandError::StorageError(st) => format!("-ERR {}\r\n", st),
            _ => "Error".to_string(),
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
        config => config,
        echo => echo,
        get => get,
        info => info,
        keys => keys,
        ping => ping,
        replconf => replconf,
        set => set,
        type_fn => type_fn,
        xadd => xadd,
        xrange => xrange,
        xread => xread,
        incr => incr,
    };
}

lazy_static! {
    pub static ref COMMANDS_REGISTRY: CommandsReg = register_commands! {
        config => config,
        echo => echo,
        get => get,
        info => info,
        keys=> keys,
        ping => ping,
        replconf => replconf,
        set => set,
        type_fn => type_fn,
        wait => wait,
        xadd => xadd,
        xrange => xrange,
        xread => xread,
        incr => incr,
    };
}

pub async fn run_command(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    server_metadata: &Arc<RwLock<ServerMetadata>>,
    command_reg: &CommandsReg,
) {
    let lowered = command.cmd.to_lowercase();
    let mut cmd_key = lowered.as_str();
    if cmd_key == "type" {
        cmd_key = "type_fn";
    }

    match command_reg
        .commands
        .get(cmd_key)
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
            let mut db_entry = DBEntry::from_string(value);
            if args.len() > 2 && args[2].to_lowercase() == "px" {
                let _ = db_entry.set_ttl(args.get(3));
            }
            set_inner(key, db_entry);
            if metadata.role == 0 {
                _write_stream_and_flush(&stream, "+OK\r\n").await;
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

fn set_inner(key: &str, db_entry: DBEntry) {
    with_storage(|storage| {
        storage.insert(key.to_string(), db_entry);
    })
}

async fn _sync_replicas(raw_command: String, sender: &broadcast::Sender<Arc<Vec<u8>>>) {
    if sender.receiver_count() > 0 {
        let v = Arc::new(raw_command.into_bytes());
        let _ = sender.send(v);
    }
}

enum Blocking {
    No,
    Forever,
    For(Duration),
}

struct XReadRequest {
    blocking: Blocking,
    pairs: Vec<(String, StreamId)>,
}
fn last_id(storage: &HashMap<String, DBEntry>, key: &str) -> StreamId {
    storage
        .get(key)
        .and_then(|entry| entry.value().ok())
        .and_then(|value| value.as_any().downcast_ref::<StreamType>())
        .and_then(StreamType::last_id)
        .unwrap_or_default()
}

fn parse_block(options: &[String]) -> Result<Blocking, CommandError> {
    let Some(position) = options.iter().position(|o| "block".eq_ignore_ascii_case(o)) else {
        return Ok(Blocking::No);
    };

    let value = options
        .get(position + 1)
        .ok_or_else(|| _wrong_args("xread"))?;
    let millis: u64 = value.parse().map_err(|_| {
        CommandError::InvalidArgument("timeout is not an integer or out of range".to_string())
    })?;
    Ok(match millis {
        0 => Blocking::Forever,
        n => Blocking::For(Duration::from_millis(n)),
    })
}

fn parse_xread(command: &Command) -> Result<XReadRequest, CommandError> {
    let args = &command.args;

    let position = args
        .iter()
        .position(|s| "streams".eq_ignore_ascii_case(s))
        .ok_or_else(|| _wrong_args("xread"))?;

    let blocking = parse_block(&args[..position])?;
    let rest = &args[position + 1..];
    if rest.is_empty() || rest.len() % 2 == 1 {
        return Err(CommandError::InvalidArgument(
            "Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified"
                .to_string(),
        ));
    }

    let (keys, ids) = rest.split_at(rest.len() / 2);

    let pairs = with_storage(|storage| {
        keys.iter()
            .zip(ids)
            .map(|(key, id)| {
                let after = if id == "$" {
                    last_id(storage, key)
                } else {
                    StreamId::try_from(id.as_str())?
                };
                Ok((key.clone(), after))
            })
            .collect::<Result<Vec<_>, CommandError>>()
    })?;

    Ok(XReadRequest { blocking, pairs })
}

fn stream_to_resp(
    entry: &DBEntry,
    key: &str,
    after: StreamId,
) -> Result<Option<String>, CommandError> {
    let stream = entry
        .value()?
        .as_any()
        .downcast_ref::<StreamType>()
        .ok_or_else(_wrong_type)?;
    let mut range = stream.entries_after(after).peekable();
    if range.peek().is_none() {
        return Ok(None);
    }

    let body = StreamType::to_resp(range);
    Ok(Some(format!("*2\r\n${}\r\n{}\r\n{}", key.len(), key, body)))
}

fn xread_once(pairs: &[(String, StreamId)]) -> Result<Option<String>, CommandError> {
    with_storage(|storage| {
        let mut res = Vec::new();

        for (key, after) in pairs {
            let Some(entry) = storage.get(key) else {
                continue;
            };

            if let Some(item) = stream_to_resp(entry, key, *after)? {
                res.push(item);
            }
        }

        if res.is_empty() {
            return Ok(None);
        }
        Ok(Some(format!("*{}\r\n{}", res.len(), res.join(""))))
    })
}

async fn poll_until_data(pairs: &[(String, StreamId)]) -> Result<String, CommandError> {
    loop {
        let notified = STREAM_ADDED.notified();
        if let Some(resp) = xread_once(pairs)? {
            return Ok(resp);
        }
        notified.await;
    }
}
async fn xread_run(command: &Command) -> Result<String, CommandError> {
    let request = parse_xread(command)?;

    let found = match request.blocking {
        Blocking::No => xread_once(&request.pairs)?,
        Blocking::Forever => Some(poll_until_data(&request.pairs).await?),
        Blocking::For(duration) => match timeout(duration, poll_until_data(&request.pairs)).await {
            Ok(result) => Some(result?),
            Err(_elapsed) => None,
        },
    };

    Ok(found.unwrap_or_else(|| "*-1\r\n".to_string()))
}
async fn xread(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let res = match xread_run(&command).await {
        Ok(resp) => resp,
        Err(e) => e.as_resp(),
    };
    _write_stream_and_flush(&stream, res.as_str()).await;
}

async fn xrange(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let res = match xrange_inner(command) {
        Ok(stream_entity) => stream_entity,
        Err(e) => e.as_resp(),
    };
    _write_stream_and_flush(&stream, res.as_str()).await;
}

fn xrange_inner(command: Command) -> Result<String, CommandError> {
    let args = command.args;

    // Create the stream
    let key = args.first().ok_or_else(|| _wrong_args("xrange"))?;
    let start = args.get(1).ok_or_else(|| _wrong_args("xrange"))?;
    let end = args.get(2).ok_or_else(|| _wrong_args("xrange"))?;
    with_storage(|storage| {
        let entry = storage.get(key).ok_or_else(|| _missing_entry("xrange"))?;

        let stream = entry
            .value()?
            .as_any()
            .downcast_ref::<StreamType>()
            .ok_or_else(_wrong_type)?;
        let start_stream = if start == "-" {
            StreamId { millis: 0, seq: 0 }
        } else {
            StreamId::try_from(start.as_str())?
        };

        let end_stream = if end == "+" {
            StreamId {
                millis: u64::MAX,
                seq: u64::MAX,
            }
        } else {
            StreamId::try_from(end.as_str())?
        };
        let range = stream.entries_range(start_stream, end_stream);

        Ok(StreamType::to_resp(range))
    })
}

async fn xadd(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let res = match xadd_inner(command) {
        Ok(id) => {
            let s = id.to_string();
            format!("${}\r\n{}\r\n", s.len(), s)
        }
        Err(e) => e.as_resp(),
    };

    _write_stream_and_flush(&stream, res.as_str()).await;
}

fn xadd_inner(command: Command) -> Result<StreamId, CommandError> {
    let args = command.args;

    // Create the stream
    let key = args.first().ok_or_else(|| _wrong_args("xadd"))?;
    let stream_id_str = args.get(1).ok_or_else(|| _wrong_args("xadd"))?;
    let rest = args.get(2..).unwrap_or(&[]);
    if rest.is_empty() {
        return Err(_wrong_args("xadd"));
    }

    let stream_id = with_storage(|storage| {
        let entry = storage
            .entry(key.clone())
            .or_insert_with(|| DBEntry::from_stream(StreamType::default()));
        let stream = entry
            .value_mut()?
            .as_any_mut()
            .downcast_mut::<StreamType>()
            .ok_or_else(_wrong_type)?;

        let stream_id = stream.parse_stream_id(stream_id_str)?;
        let fields: Vec<(String, String)> = rest
            .chunks_exact(2)
            .map(|c| (c[0].clone(), c[1].clone()))
            .collect();

        stream.add(stream_id, fields)
    })?;

    // Lock released. Wake readers only after the entry is visible — and only after
    // we're no longer holding the lock they'll immediately need.
    STREAM_ADDED.notify_waiters();

    Ok(stream_id)
}

async fn get(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let args = command.args;
    let key = args.first().unwrap();
    let res = get_inner(key);
    _write_stream_and_flush(&stream, res.as_str()).await;
}

fn get_inner(key: &str) -> String {
    with_storage(|storage| match storage.get(key) {
        Some(val) => format_result(val),
        None => "$-1\r\n".to_string(),
    })
}

async fn type_fn(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let args = command.args;
    let key = args.first().unwrap();
    let res = type_fn_inner(key);
    _write_stream_and_flush(&stream, res.as_str()).await;
}

fn type_fn_inner(key: &str) -> String {
    with_storage(|storage| match storage.get(key) {
        Some(entry) => format!("+{}\r\n", entry.value().unwrap().type_name()),
        None => "+none\r\n".to_string(),
    })
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

async fn keys(
    stream: Arc<RwLock<TcpStream>>,
    _command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let res = keys_inner();
    _write_stream_and_flush(&stream, res.as_str()).await;
}

fn keys_inner() -> String {
    with_storage(|storage| {
        let mut res = format!("*{}\r\n", storage.len());
        for key in storage.keys() {
            res.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
        }
        res
    })
}

async fn incr(
    stream: Arc<RwLock<TcpStream>>,
    command: Command,
    _server_metadata: &Arc<RwLock<ServerMetadata>>,
) {
    let args = command.args;
    let key = args.first().unwrap();
    let res = match incr_inner(key.as_str()) {
        Ok(value) => format!(":{}\r\n", value),
        Err(e) => e.as_resp(),
    };
    _write_stream_and_flush(&stream, res.as_str()).await;
}

fn incr_inner(key: &str) -> Result<i64, CommandError> {
    with_storage(|storage| {
        let entry = storage
            .entry(key.to_string())
            .or_insert_with(|| DBEntry::from_string("0"));
        let value = entry
            .value_mut()?
            .as_any_mut()
            .downcast_mut::<String>()
            .ok_or_else(_wrong_type)?;
        let new_value = value
            .parse::<i64>()
            .map_err(|_| _not_an_integer())?
            .checked_add(1)
            .ok_or_else(|| {
                CommandError::InvalidArgument("increment or decrement would overflow".to_string())
            })?;
        *value = new_value.to_string();
        Ok(new_value)
    })
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
    match value.value() {
        Ok(v) => format!("${}\r\n{}\r\n", v.len(), v),
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

fn _wrong_args(cmd: &str) -> CommandError {
    CommandError::InvalidArgument(format!("wrong number of arguments for '{}' command", cmd))
}

fn _wrong_type() -> CommandError {
    CommandError::StorageError(
        "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
    )
}

fn _missing_entry(cmd: &str) -> CommandError {
    CommandError::StorageError(format!(
        "The ID sent in {} command id not found in the storage",
        cmd
    ))
}

fn _not_an_integer() -> CommandError {
    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
}
