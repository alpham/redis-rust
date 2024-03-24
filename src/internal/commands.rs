use std::{
    collections::HashMap,
    sync::Mutex,
    fmt::{ Display, Formatter},
    error::Error
};

use crate::internal::parser::Command;

#[derive(Debug)]
pub enum CommandError{
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
    }}

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
            set => set
        };
    static ref STORAGE: Mutex<HashMap<String, String>> = Mutex::new(HashMap::new());
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

fn echo(args: Vec<String>) -> Result<String, CommandError>  {
    Ok(
        format!("+{}\r\n", args.get(0).unwrap()).to_string()
    )
}

fn set(args: Vec<String>) -> Result<String, CommandError> {
    let key = args.get(0).unwrap();
    let value = args.get(1).ok_or_else(
        || CommandError::InvalidArgument("Missing arguments".to_string())
    )?;

    STORAGE.lock().unwrap().insert(key.to_string(), value.to_string());
    Ok("+OK\r\n".to_string())
}

fn get(args: Vec<String>) -> Result<String, CommandError> {
    let key = args.get(0).unwrap();
    match STORAGE.lock().unwrap().get(key) {
        Some(val) => {
            let res = format!("${}\r\n{}\r\n", val.len() ,val.to_string());
            Ok(res)
        }
        None => Err(CommandError::StorageError("$-1\r\n".to_string()))
    }
}
