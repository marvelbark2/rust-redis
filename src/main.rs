use rand::distr::Alphanumeric;
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use codecrafters_redis::{
    AppCommand, AppCommandParser, Engine, HashMapEngine, ReplicationClient, RespFormatter,
    StreamPayload,
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

    let engine_mutex = Arc::new(RwLock::new(HashMapEngine::new()));

    let lock_set: HashSet<String> = HashSet::new();
    let lock_mutex = Arc::new(RwLock::new(lock_set));

    let payload = StreamPayload {
        writter: Arc::clone(&engine_mutex),
        lock: Arc::clone(&lock_mutex),
        replica_of: replica_of.clone(),
        master_replid: master_replid.clone(),
    };

    if !replica_of.is_empty() {
        let mut repli_client = ReplicationClient::new(&replica_of, port);
        repli_client.connect_and_handshake().await?;

        let status = repli_client.psync(None, -1).await?;
        println!("PSYNC status: {}", String::from_utf8_lossy(&status));
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
                } else if first_cmd == "PSYNC" {
                    let first_frag = format!(
                        "+FULLRESYNC {} {}\r\n",
                        "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", 0
                    );

                    write_half.write_all(first_frag.as_bytes()).await?;

                    let rdb_file = payload.writter.read().await.rdb_file();

                    // Invalid RDB file: unexpected EOF
                    if !rdb_file.is_empty() {
                        write_half.write_all(b"$" as &[u8]).await?;
                        write_half
                            .write_all(rdb_file.len().to_string().as_bytes())
                            .await?;
                        write_half.write_all(b"\r\n").await?;
                        write_half.write_all(&rdb_file).await?;
                    } else {
                        write_half.write_all(b"$-1\r\n").await?;
                    }
                } else {
                    write_half.write_all(b"-ERR unknown command\r\n").await?;
                }
                write_half.flush().await?;
            }
        }
    }

    Ok(())
}
