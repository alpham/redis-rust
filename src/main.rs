use std::error::Error;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};


fn main() -> Result<(), Box<dyn Error>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_client(stream);
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    };
    Ok(())
}

fn handle_client(mut stream: TcpStream) {
    let data = "+PONG\r\n";
    let mut buf = [0u8;255];
    while let Ok(_) = stream.read(&mut buf) {
        stream.write_all(data.as_bytes()).unwrap();
    }
}
