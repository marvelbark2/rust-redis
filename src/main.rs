use rand::distr::Alphanumeric;
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use codecrafters_redis::{
    AppCommand, AppCommandParser, Engine, HashMapEngine, ReplicationClient, ReplicationsManager,
    RespFormatter, StreamPayload,
};
const EMPTY_RDB: &[u8] = include_bytes!("./../empty.rdb");

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
        replica_manager: Arc::new(RwLock::new(ReplicationsManager::new())),
        offset: 0,
    };
    if !replica_of.is_empty() {
        tokio::spawn({
            let replica_of = replica_of.clone();
            let port = port.clone();
            let payload = payload.clone();
            async move {
                let mut repli_client = ReplicationClient::new(&replica_of, port);

                println!("Connecting to master at {}", replica_of);
                if let Err(e) = repli_client.connect_and_handshake().await {
                    eprintln!("Error during handshake: {}", e);
                    return;
                }

                let status = match repli_client.psync(None, -1).await {
                    Ok(res) => res,
                    Err(e) => {
                        eprintln!("Error during PSYNC: {}", e);
                        return;
                    }
                };

                if status.len() > 0 {
                    repli_client.listen_for_replication(payload.clone()).await;
                }
            }
        });
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
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Wrap writer so it can be shared with ReplicationsManager
    let write_half = Arc::new(tokio::sync::Mutex::new(write_half));

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

        let cmd_parts2 = cmd_parts.clone();

        match AppCommand::from_parts_simple(cmd_parts) {
            Some(cmd) => {
                if multi_cmd.len() == 0 {
                    let response = cmd.compute(payload); // see note below
                    let resp = response.await;
                    {
                        let mut w = write_half.lock().await;
                        w.write_all(resp.as_bytes()).await?;
                        w.flush().await?;
                    }
                } else {
                    multi_cmd.push(cmd);
                    let mut w = write_half.lock().await;
                    w.write_all(b"+QUEUED\r\n").await?;
                    w.flush().await?;
                }
            }
            None => {
                if first_cmd == "MULTI" {
                    multi_cmd.push(AppCommand::None);
                    let mut w = write_half.lock().await;
                    w.write_all(b"+OK\r\n").await?;
                    w.flush().await?;
                } else if first_cmd == "DISCARD" && !multi_cmd.is_empty() {
                    multi_cmd.clear();
                    let mut w = write_half.lock().await;
                    w.write_all(b"+OK\r\n").await?;
                    w.flush().await?;
                } else if first_cmd == "DISCARD" && multi_cmd.is_empty() {
                    let mut w = write_half.lock().await;
                    w.write_all(b"-ERR DISCARD without MULTI\r\n").await?;
                    w.flush().await?;
                } else if first_cmd == "EXEC" {
                    if multi_cmd.is_empty() {
                        let mut w = write_half.lock().await;
                        w.write_all(b"-ERR EXEC without MULTI\r\n").await?;
                        w.flush().await?;
                    } else {
                        let mut responses = Vec::new();
                        for cmd_item in multi_cmd.drain(1..) {
                            let response = cmd_item.compute(payload).await;
                            responses.push(response);
                        }
                        let resp = RespFormatter::format_array_result(&responses);
                        let mut w = write_half.lock().await;
                        w.write_all(resp.as_bytes()).await?;
                        w.flush().await?;

                        multi_cmd.clear();
                    }
                } else if first_cmd == "REPLCONF" {
                    let mut w = write_half.lock().await;
                    w.write_all(b"+OK\r\n").await?;
                    w.flush().await?;
                } else if first_cmd == "PSYNC" {
                    {
                        payload
                            .replica_manager
                            .write()
                            .await
                            .add_client(write_half.clone());
                    }
                    let first_frag = format!("+FULLRESYNC {} {}\r\n", payload.master_replid, 0);
                    {
                        let mut w = write_half.lock().await;
                        w.write_all(first_frag.as_bytes()).await?;
                    }

                    let rdb = payload.writter.read().await.rdb_file();

                    let mut w = write_half.lock().await;
                    if !rdb.is_empty() {
                        let payload = if rdb == b"" {
                            EMPTY_RDB
                        } else {
                            rdb.as_slice()
                        };
                        let mut buf = Vec::from(b"$");
                        buf.extend_from_slice(payload.len().to_string().as_bytes()); // ASCII decimal length
                        buf.extend_from_slice(b"\r\n");
                        buf.extend_from_slice(payload); // binary-safe

                        //  buf.extend_from_slice(b"\r\n"); // final CRLF

                        w.write_all(&buf).await?;
                        w.flush().await?; // helpful if `w` is buffered (e.g., BufWriter)
                    } else {
                        w.write_all(b"$-1\r\n").await?;
                    }
                    w.flush().await?;
                } else {
                    let mut w = write_half.lock().await;
                    w.write_all(b"-ERR unknown command\r\n").await?;
                    w.flush().await?;
                }
            }
        }

        if payload.replica_of.is_empty() && (first_cmd == "SET") {
            if let Err(e) = payload
                .replica_manager
                .write()
                .await
                .broadcast(cmd_parts2.clone())
                .await
            {
                eprintln!("Error broadcasting command: {}", e);
            }
        }
    }

    Ok(())
}
