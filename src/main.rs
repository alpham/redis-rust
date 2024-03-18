use std::error::Error;
use std::{
    net::{TcpListener, TcpStream},
    io::{BufRead, Write, BufReader},
};

fn main() -> Result<(), Box<dyn Error>>{
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_client(stream)?;
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }
    Ok(())
}


fn handle_client(mut stream: TcpStream) -> Result<(), Box<dyn Error>>{
    let mut buf = BufReader::new(&stream);
    let mut line = String::new();
    buf.read_line(&mut line)?;

    stream.write_all(b"+PONG\r\n").expect("Failed to write to client");
    Ok(())
}
