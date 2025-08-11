#![allow(unused_imports)]
use std::{
    io::{self, BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    process::Command,
    thread,
};

use codecrafters_redis::{AppCommand, AppCommandParser};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        thread::spawn(move || match stream {
            Ok(stream) => {
                handle_stream(stream).unwrap_or_else(|e| {
                    eprintln!("Error handling stream: {}", e);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        });
    }
}

fn handle_stream(mut stream: TcpStream) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    loop {
        let command_parser = AppCommandParser::new();

        let cmd_parts = command_parser.parse_resp_array(&mut reader)?;
        if cmd_parts.is_empty() {
            continue;
        }

        let command = AppCommand::from_parts_simple(cmd_parts);

        match command {
            Some(cmd) => {
                let response = cmd.compute();
                let res = format!("+{}\r\n", response);
                stream.write_all(res.as_bytes())?;
            }
            None => {
                let error_response = "-ERR unknown command\r\n";
                stream.write_all(error_response.as_bytes())?;
            }
        }
    }
}
