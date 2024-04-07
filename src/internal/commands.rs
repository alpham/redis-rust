use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
};

use crate::internal::parser::Command;
use crate::internal::storage::{DBEntry, DBEntryValueType, STORAGE};

#[derive(Debug)]
pub enum CommandError {
    CommandNotFound(String),
    InvalidArgument(String),
    StorageError(String),
}

impl Error for CommandError {}

impl Display for CommandError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandError::CommandNotFound(cmd) => write!(f, "Command not found: {}", cmd),
            CommandError::InvalidArgument(msg) => write!(f, "Invalid arguments: {}", msg),
            CommandError::StorageError(msg) => write!(f, "Storage error: {}", msg),
        }
    }
}

type CommandFn = fn(Vec<String>) -> Result<String, CommandError>;
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
        info => info
    };
}

pub fn run_command(command: Command) -> Result<String, CommandError> {
    let function = COMMANDS_REGISTRY
        .get(command.cmd.to_lowercase().as_str())
        .ok_or_else(|| CommandError::CommandNotFound(command.cmd))?;
    function(command.args)
}

fn ping(_args: Vec<String>) -> Result<String, CommandError> {
    Ok("+PONG\r\n".to_string())
}

fn echo(args: Vec<String>) -> Result<String, CommandError> {
    Ok(format!("+{}\r\n", args.get(0).unwrap()).to_string())
}

fn set(args: Vec<String>) -> Result<String, CommandError> {
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
    Ok("+OK\r\n".to_string())
}

fn get(args: Vec<String>) -> Result<String, CommandError> {
    let key = args.get(0).unwrap();
    match STORAGE.lock().unwrap().get(key) {
        Some(val) => {
            let res = format_result(val);
            Ok(res)
        }
        None => Err(CommandError::StorageError("$-1\r\n".to_string())),
    }
}

fn info(args: Vec<String>) -> Result<String, CommandError> {
    let info_section = args.get(0).unwrap();
    if info_section == "replication" {
        let response = "role:master".to_string();
        return Ok(format!("${}\r\n{}\r\n", response.len(), response));
    }
    Ok(String::new())
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
