#![allow(unused_imports)]
use std::{
    io::{self, BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    thread,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        thread::spawn(move || match stream {
            Ok(stream) => {
                handle_stream(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        });
    }
}

fn handle_stream(mut stream: TcpStream) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());

    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).unwrap();
        if n == 0 {
            break; // connection closed
        }

        // Trim whitespace, CRLF, and any stray NULs.
        let req = line.trim_matches(|c: char| c.is_whitespace() || c == '\0');

        let mut res = if req.eq_ignore_ascii_case("PING") {
            "PONG".to_string()
        } else if let Some(after) = req.strip_prefix("ECHO ") {
            after.to_string()
        } else {
            String::new()
        };

        if res.is_empty() {
            eprintln!("Unknown command received: {req:?}");
            continue;
        }

        res.insert_str(0, "+");

        stream.write_all(res.as_bytes()).unwrap();
    }
}
