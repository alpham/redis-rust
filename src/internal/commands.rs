use std::collections::HashMap;
use crate::internal::parser::Command;

type CommandFn = fn(Vec<String>) -> Result<String, String>;

lazy_static! {
        static ref COMMANDS_REGISTRY: HashMap<&'static str, CommandFn> = {
            let m = HashMap::from([
                ("ping", ping as CommandFn),
                ("echo", echo as CommandFn),
            ]);
            m
    };
}
pub fn run_command(command: Command) -> Result<String, ()> {
    let function = COMMANDS_REGISTRY
        .get(command.cmd.to_lowercase().as_str())
        .unwrap();
    match function(command.args) {
        Ok(value) => Ok(value),
        Err(e) => panic!("Cannot find command '{}': {}", command.cmd, e),
    }
}

fn ping(_args: Vec<String>) -> Result<String, String> {
    Ok("+PONG\r\n".to_string())
}

fn echo(args: Vec<String>) -> Result<String, String>  {
    Ok(
        format!("+{}\r\n", args.get(0).unwrap()).to_string()
    )
}
