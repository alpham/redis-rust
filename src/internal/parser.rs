#[derive(Debug)]
pub struct Command {
    pub cmd: String,
    pub args: Vec<String>,
    pub raw_cmd: String,
}

pub fn parse_request(buf: &[u8]) -> Result<Vec<Command>, String> {
    let body = String::from_utf8_lossy(&buf).into_owned();
    let commands_str = body.split("*");
    let mut commands: Vec<Command> = Vec::new();
    for chunk in commands_str {
        if chunk.is_empty() {
            continue;
        }
        let cmd_str = format!("*{}", chunk);
        println!(">> {}", cmd_str);
        match parse_command(cmd_str.as_bytes()) {
            Ok(Some(command)) => {
                commands.push(command);
            }
            Ok(None) => println!("Skipping a command..."),
            Err(_) => {
                println!("Error in parse_command for line");
            }
        }
    }
    Ok(commands)
}

fn parse_command(buf: &[u8]) -> Result<Option<Command>, String> {
    let body = String::from_utf8_lossy(&buf).into_owned();
    if is_valid_format(&body) {
        println!("body: {}", body);
        let mut lines = body.lines();
        // the first line is the global description.
        let global_desc = lines.next().unwrap();
        let number_args = global_desc[1..].parse::<i32>().unwrap();
        // ignore the length of the command;
        let _ = lines.next();
        let cmd = lines.next().unwrap().to_string();
        let mut args: Vec<String> = Vec::new();
        for _ in 1..number_args {
            // ignore number of characters of each line
            let _ = lines.next();
            let arg = lines.next().unwrap().to_string();
            args.push(arg);
        }

        Ok(Some(Command {
            cmd,
            args,
            raw_cmd: body,
        }))
    } else {
        // Then it's a database sync request, ignore for now.
        // TODO: parse the database to sync the replica.
        println!("body - skipped: {}", body);
        Ok(None)
    }
}

fn is_valid_format(input: &str) -> bool {
    // Get the first line
    if let Some(first_line) = input.lines().next() {
        // Check if it starts with '*' and ends with a single digit
        let mut chars = first_line.chars();

        // Ensure the first part contains one or more '*'
        let has_stars = Some('*') == chars.next();

        // Ensure the remaining part is exactly one digit
        has_stars && chars.clone().count() == 1 && chars.all(|c| c.is_digit(10))
    } else {
        false // Empty input
    }
}
