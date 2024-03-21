#[macro_use]
extern crate lazy_static;
mod internal;

use std::error::Error;
use std::{
    thread,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

use crate::internal::{
    parser,
    commands
};

fn main() -> Result<(), Box<dyn Error>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        thread::spawn(|| 
            match stream {
                Ok(stream) => {
                    handle_client(stream);
                }
                Err(e) => {
                    eprintln!("error: {}", e);
                }
            }
        );
    };
    Ok(())
}

fn handle_client(mut stream: TcpStream) {
    let mut buf = [0u8;255];
    while let Ok(_) = stream.read(&mut buf) {
        let buf_string = String::from_utf8_lossy(&buf);
        println!("{}", buf_string);
        let command = parser::parse_request(&buf).unwrap();
        let result = commands::run_command(command).unwrap();
        stream.write_all(result.as_bytes()).unwrap();
    }
}
