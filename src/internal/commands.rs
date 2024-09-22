use std::{
    collections::HashMap,
    error::Error,
    io::{Error as IOError, ErrorKind, Write},
    fmt::{Display, Formatter},
    net::TcpStream,
};

use hex; 
use crate::internal::parser::Command;
use crate::internal::server::ServerMetadata;
use crate::internal::storage::{DBEntry, DBEntryValueType, STORAGE};
use crate::internal::server_info;

#[derive(Debug)]
pub enum CommandError {
    CommandNotFound(String),
    InvalidArgument(String),
    StorageError(String),
    ErrorWhileExecution(String)
}

impl Error for CommandError {}

impl Display for CommandError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandError::CommandNotFound(cmd) => write!(f, "Command not found: {}", cmd),
            CommandError::InvalidArgument(msg) => write!(f, "Invalid arguments: {}", msg),
            CommandError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            CommandError::ErrorWhileExecution(msg) => write!(f, "Error while executing the command: {}", msg)
        }
    }
}

type CommandFn = fn(&mut TcpStream, &Vec<String>, &ServerMetadata) -> Result<String, CommandError>;
macro_rules! register_commands {
    ($($name:ident => $func:ident), *) => {
        {
            let mut m = HashMap::new();
            $(m.insert(stringify!($name), $func as CommandFn);)*
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

pub fn run_command(
    stream: &mut TcpStream,
    command: &Command,
    server_metadata: &ServerMetadata,
) -> Result<String, CommandError> {
    let function = COMMANDS_REGISTRY
        .get(command.cmd.to_lowercase().as_str())
        .ok_or_else(|| CommandError::CommandNotFound(command.cmd.clone()))?;
    function(stream, &command.args, server_metadata)
}

fn psync(stream: &mut TcpStream, _args: &Vec<String>, server_metadata: &ServerMetadata) -> Result<String, CommandError> {
    let res = format!("+FULLRESYNC {} {}\r\n", server_metadata.master_replid, server_metadata.master_repl_offset);
    stream.write_all(res.as_bytes()).unwrap();
    let _ = stream.flush();

    let rdb_file = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
    .map_err(|decode_err| IOError::new(ErrorKind::InvalidData, decode_err.to_string())).unwrap();
    
    stream.write_all(format!("${}\r\n", rdb_file.len()).as_bytes()).unwrap();
    stream.write_all(rdb_file.as_slice()).unwrap();
    let _ = stream.flush();
    Ok(res)
}

fn replconf(stream: &mut TcpStream, _args: &Vec<String>, _server_metadata: &ServerMetadata) -> Result<String, CommandError> {
    let res = "+OK\r\n".to_string();
    stream.write_all(res.as_bytes()).unwrap();
    let _ = stream.flush();
    
    Ok(res)
}

fn ping(stream: &mut TcpStream, _args: &Vec<String>, _server_metadata: &ServerMetadata) -> Result<String, CommandError> {
    let res = "+PONG\r\n".to_string();
    stream.write_all(res.as_bytes()).unwrap();
    let _ = stream.flush();
    Ok(res)
}

fn echo(stream: &mut TcpStream, args: &Vec<String>, _server_metadata: &ServerMetadata) -> Result<String, CommandError> {
    let res = format!("+{}\r\n", args.get(0).unwrap()).to_string();
    stream.write_all(res.as_bytes()).unwrap();
    let _ = stream.flush();
    Ok(res)
}

fn set(stream: &mut TcpStream, args: &Vec<String>, _server_metadata: &ServerMetadata) -> Result<String, CommandError> {
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
    STORAGE.lock().unwrap().insert(key.to_string(), db_entry);
    let res = "+OK\r\n".to_string();
    stream.write_all(res.as_bytes()).unwrap();
    let _ = stream.flush();
    Ok(res)
}

fn get(stream: &mut TcpStream, args: &Vec<String>, _server_metadata: &ServerMetadata) -> Result<String, CommandError> {
    let key = args.get(0).unwrap();
    match STORAGE.lock().unwrap().get(key) {
        Some(val) => {
            let res = format_result(val);
            stream.write_all(res.as_bytes()).unwrap();
            let _ = stream.flush();
            Ok(res)
        }
        None => Err(CommandError::StorageError("$-1\r\n".to_string())),
    }
}

fn info(stream: &mut TcpStream, args: &Vec<String>, server_metadata: &ServerMetadata) -> Result<String, CommandError> {
    let info_section = args.get(0).unwrap();
    let response = String::new();
    if info_section == "replication" {
        match server_info::get_server_info(server_metadata) {
            Ok(res) => {
                stream.write_all(res.as_bytes()).unwrap();
                let _ = stream.flush();
            }
            Err(_) => return Err(CommandError::ErrorWhileExecution("Cannot return replication info".to_string()))
        }
    }

    Ok(response)
}

fn format_result(value: &DBEntry) -> String {
    match value.to_string() {
        Ok(value) => format!("${}\r\n{}\r\n", value.len(), value),
        Err(_) => "$-1\r\n".to_string(),
        // Err(e) => match e {
        //     CommandError::StorageError(_),
        //     CommandError::InvalidArgument(_),
        //     CommandError::CommandNotFound(_) => "$-1\r\n".to_string(),
        // }
    }
}
