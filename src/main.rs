use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use codecrafters_redis::{AppCommand, AppCommandParser, Engine, HashMapEngine, RespFormatter};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let port = std::env::args().nth(1).expect("6379");

    let address = format!("127.0.0.1:{}", port);

    let listener = TcpListener::bind(address).await?;

    let engine_mutex = Arc::new(RwLock::new(HashMapEngine {
        hash_map: HashMap::new(),
        list_map: HashMap::new(),
        stream_map: HashMap::new(),
    }));

    let lock_set: HashSet<String> = HashSet::new();
    let lock_mutex = Arc::new(RwLock::new(lock_set));

    loop {
        let (stream, _) = listener.accept().await?;
        let engine_mutex_clone = Arc::clone(&engine_mutex);
        let lock_mutex_clone = Arc::clone(&lock_mutex);
        tokio::spawn(async move {
            if let Err(e) = handle_stream(stream, engine_mutex_clone, lock_mutex_clone).await {
                eprintln!("Error handling stream: {}", e);
            }
        });
    }
}

async fn handle_stream<T: Engine + Send + Sync + 'static>(
    stream: TcpStream,
    engine: Arc<RwLock<T>>,
    lock_mutex: Arc<RwLock<HashSet<String>>>,
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
                    let response = cmd.compute(&engine, &lock_mutex); // see note below
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
                            let response = cmd_item.compute(&engine, &lock_mutex).await;
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
