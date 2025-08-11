#![allow(unused_imports)]
use std::{
    collections::{hash_map, HashMap},
    io::{self, BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::{Arc, Mutex, RwLock},
    thread,
};

use codecrafters_redis::{AppCommand, AppCommandParser, Engine, HashMapEngine};

fn main() {
    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let engine_mutex = Arc::new(Mutex::new(HashMapEngine {
        hash_map: HashMap::new(),
    }));

    for stream in listener.incoming() {
        let engine_mutex = Arc::clone(&engine_mutex);
        thread::spawn(move || match stream {
            Ok(stream) => {
                handle_stream(stream, engine_mutex).unwrap_or_else(|e| {
                    eprintln!("Error handling stream: {}", e);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        });
    }
}

fn handle_stream<T: Engine>(mut stream: TcpStream, engine: Arc<Mutex<T>>) -> io::Result<()> {
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
                let response = cmd.compute(&engine);
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
