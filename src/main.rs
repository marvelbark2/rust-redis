#![allow(unused_imports)]
use std::{
    io::{self, BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    process::Command,
    thread,
};

use codecrafters_redis::AppCommand;

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
        let cmd_parts = parse_resp_array(&mut reader)?;
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
                let other_attempt_cmd = parse_resp_array_simple(&mut reader)?;
                if other_attempt_cmd.is_empty() {
                    continue;
                }
                if let Some(cmd) = AppCommand::from_parts_simple(other_attempt_cmd) {
                    let response = cmd.compute();
                    let res = format!("+{}\r\n", response);
                    stream.write_all(res.as_bytes())?;
                } else {
                    let err = "ERR unknown command\r\n";
                    stream.write_all(err.as_bytes())?;
                }
            }
        }
    }
}

fn parse_resp_array<R: BufRead>(reader: &mut R) -> io::Result<Vec<String>> {
    let mut line = String::new();

    reader.read_line(&mut line)?;
    if !line.starts_with('*') {
        return parse_resp_array_simple(reader);
    }
    let count: usize = line[1..]
        .trim()
        .parse()
        .map_err(|_| io::ErrorKind::InvalidData)?;

    let mut parts = Vec::with_capacity(count);

    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line)?;
        if !line.starts_with('$') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected bulk string",
            ));
        }
        let len: usize = line[1..]
            .trim()
            .parse()
            .map_err(|_| io::ErrorKind::InvalidData)?;

        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        parts.push(str::from_utf8(&buf).unwrap().to_string());

        // Consume trailing \r\n
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf)?;
    }

    Ok(parts)
}

fn parse_resp_array_simple<R: BufRead>(reader: &mut R) -> io::Result<Vec<String>> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    if !line.starts_with('*') {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected array"));
    }
    let count: usize = line[1..]
        .trim()
        .parse()
        .map_err(|_| io::ErrorKind::InvalidData)?;

    let mut parts = Vec::with_capacity(count);

    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line)?;
        if !line.starts_with('$') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected bulk string",
            ));
        }
        let len: usize = line[1..]
            .trim()
            .parse()
            .map_err(|_| io::ErrorKind::InvalidData)?;

        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        parts.push(str::from_utf8(&buf).unwrap().to_string());

        // Consume trailing \r\n
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf)?;
    }

    Ok(parts)
}
