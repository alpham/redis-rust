#[derive(Debug)]
pub struct Command {
    pub cmd: String,
    pub args: Vec<String>,
    pub raw_cmd: String,
}

pub fn parse_request(buf: &[u8]) -> Result<Vec<Command>, String> {
    // let body = String::from_utf8_lossy(buf).into_owned();
    // FIXME: spliting the commands with '*' breaks the command if it contains
    // an astrisk and example is `REPLCONF GETACK *`
    let mut cursor = 0;
    let mut commands: Vec<Command> = Vec::new();
    while cursor < buf.len() {
        match parse_command(buf, cursor)? {
            Some((command, new_cursor)) => {
                commands.push(command);
                cursor = new_cursor;
            }
            None => break,
        }
    }
    Ok(commands)
}

fn read_line(buf: &[u8], cursor: usize) -> Option<(&[u8], usize)> {
    for i in cursor..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some((&buf[cursor..i], i + 2));
        }
    }
    None
}

fn parse_command(buf: &[u8], cursor: usize) -> Result<Option<(Command, usize)>, String> {
    if cursor >= buf.len() || buf[cursor] != b'*' {
        return Ok(None);
    }
    let start = cursor;
    let (header, mut cursor) =
        read_line(buf, cursor).ok_or_else(|| "Incomplete array header".to_string())?;
    let n_args: usize = std::str::from_utf8(&header[1..])
        .map_err(|e| e.to_string())?
        .parse()
        .map_err(|_| "Invalid array length".to_string())?;

    if n_args == 0 {
        return Ok(None);
    }

    let mut parts: Vec<String> = Vec::with_capacity(n_args);
    for _ in 0..n_args {
        let (len_line, next) =
            read_line(buf, cursor).ok_or_else(|| "Incomplete bulk string length".to_string())?;
        cursor = next;

        if len_line.is_empty() || len_line[0] != b'$' {
            return Err("Expected bulk string".to_string());
        }

        let len: usize = std::str::from_utf8(&len_line[1..])
            .map_err(|e| e.to_string())?
            .parse()
            .map_err(|_| "Invalid bulk string length".to_string())?;

        if cursor + len + 2 > buf.len() {
            return Err("Incomplete bulk string data".to_string());
        }

        let data = &buf[cursor..cursor + len];
        cursor += len + 2;
        let s = std::str::from_utf8(data)
            .map_err(|e| e.to_string())?
            .to_string();
        parts.push(s);
    }

    let cmd = parts.remove(0);
    let args = parts;

    let raw_cmd = String::from_utf8_lossy(&buf[start..cursor]).into_owned();

    Ok(Some((Command { cmd, args, raw_cmd }, cursor)))
}
