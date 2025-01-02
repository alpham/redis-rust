pub struct Command {
    pub cmd: String,
    pub args: Vec<String>,
    pub raw_cmd: String,
}

pub fn parse_request(buf: &[u8]) -> Result<Command, String> {
    let body = String::from_utf8(buf.to_vec()).unwrap();
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

    Ok(Command {
        cmd,
        args,
        raw_cmd: body,
    })
}
