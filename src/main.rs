use rand::distr::Alphanumeric;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use codecrafters_redis::{
    AppCommand, AppCommandParser, Engine, HashMapEngine, RespFormatter, StreamPayload,
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut port = "6379".to_string();
    let mut replica_of = String::new();

    let args: Vec<String> = std::env::args().collect();

    let mut i = 0;
    while i < args.len() {
        if args[i] == "--port" {
            if i + 1 < args.len() {
                port = args[i + 1].clone();
            }
        } else if args[i] == "--replicaof" {
            if i + 1 < args.len() {
                replica_of = args[i + 1].clone();
            }
        }
        i += 1;
    }

    let address = format!("0.0.0.0:{}", port);

    let master_replid: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(40)
        .map(char::from)
        .collect();

    println!("Starting server on {}", address);

    let listener = TcpListener::bind(address).await?;

    let engine_mutex = Arc::new(RwLock::new(HashMapEngine {
        hash_map: HashMap::new(),
        list_map: HashMap::new(),
        stream_map: HashMap::new(),
    }));

    let lock_set: HashSet<String> = HashSet::new();
    let lock_mutex = Arc::new(RwLock::new(lock_set));

    let payload = StreamPayload {
        writter: Arc::clone(&engine_mutex),
        lock: Arc::clone(&lock_mutex),
        replica_of: replica_of.clone(),
        master_replid: master_replid.clone(),
    };

    if !replica_of.is_empty() {
        let repli_host_port = replica_of.split(":").collect::<Vec<&str>>();

        let repli_host = if repli_host_port.len() > 1 {
            repli_host_port[0].to_string()
        } else {
            "localhost".to_string()
        };
        let repli_port = if repli_host_port.len() > 2 {
            repli_host_port[1].to_string()
        } else {
            "6379".to_string()
        };

        let repli_address = format!("{}:{}", repli_host, repli_port);

        let mut replica_stream = TcpStream::connect(repli_address).await?;

        replica_stream.write_all(b"*1\r\n$4\r\nPING\r\n").await?;

        replica_stream
            .write_all(
                format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
                    repli_port.len(),
                    repli_port
                )
                .as_bytes(),
            )
            .await?;

        replica_stream
            .write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
            .await?;
        replica_stream.flush().await?;
    }

    loop {
        let (stream, _) = listener.accept().await?;

        let payload = payload.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_stream(stream, &payload).await {
                eprintln!("Error handling stream: {}", e);
            }
        });
    }
}

async fn handle_stream<T: Engine + Send + Sync + 'static>(
    stream: TcpStream,
    payload: &StreamPayload<T>,
) -> std::io::Result<()> {
    // Split the stream so reads and writes can proceed independently.
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let mut multi_cmd: Vec<AppCommand> = Vec::new();

    loop {
        let cmd_parts = match AppCommandParser::new()
            .parse_resp_array_async(&mut reader)
            .await
        {
            Ok(parts) => parts,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => return Err(e),
        };

        if cmd_parts.is_empty() {
            continue;
        }

        let first_cmd = cmd_parts[0].clone().to_uppercase();

        match AppCommand::from_parts_simple(cmd_parts) {
            Some(cmd) => {
                if multi_cmd.len() == 0 {
                    let response = cmd.compute(payload); // see note below
                    write_half.write_all(response.await.as_bytes()).await?;
                } else {
                    multi_cmd.push(cmd);
                    write_half.write_all(b"+QUEUED\r\n").await?;
                }
                write_half.flush().await?;
            }
            None => {
                if first_cmd == "MULTI" {
                    multi_cmd.push(AppCommand::None);
                    write_half.write_all(b"+OK\r\n").await?;
                } else if first_cmd == "DISCARD" && !multi_cmd.is_empty() {
                    multi_cmd.clear();
                    write_half.write_all(b"+OK\r\n").await?;
                } else if first_cmd == "DISCARD" && multi_cmd.is_empty() {
                    write_half
                        .write_all(b"-ERR DISCARD without MULTI\r\n")
                        .await?;
                } else if first_cmd == "EXEC" {
                    if multi_cmd.is_empty() {
                        write_half.write_all(b"-ERR EXEC without MULTI\r\n").await?;
                    } else {
                        let mut responses = Vec::new();
                        for cmd_item in multi_cmd.drain(1..) {
                            let response = cmd_item.compute(payload).await;
                            responses.push(response);
                        }
                        write_half
                            .write_all(RespFormatter::format_array_result(&responses).as_bytes())
                            .await?;

                        multi_cmd.clear();
                    }
                    write_half.flush().await?;
                } else {
                    write_half.write_all(b"-ERR unknown command\r\n").await?;
                }
                write_half.flush().await?;
            }
        }
    }

    Ok(())
}
