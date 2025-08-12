use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use codecrafters_redis::{AppCommand, AppCommandParser, Engine, HashMapEngine};
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let engine_mutex = Arc::new(RwLock::new(HashMapEngine {
        hash_map: HashMap::new(),
        list_map: HashMap::new(),
    }));
    loop {
        let (stream, _) = listener.accept().await?;
        let engine_mutex_clone = Arc::clone(&engine_mutex);
        tokio::spawn(async move {
            if let Err(e) = handle_stream(stream, engine_mutex_clone).await {
                eprintln!("Error handling stream: {}", e);
            }
        });
    }
}

async fn handle_stream<T: Engine + Send + Sync + 'static>(
    stream: TcpStream,
    engine: Arc<RwLock<T>>,
) -> std::io::Result<()> {
    // Split the stream so reads and writes can proceed independently.
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

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

        match AppCommand::from_parts_simple(cmd_parts) {
            Some(cmd) => {
                let response = cmd.compute(&engine); // see note below
                write_half.write_all(response.await.as_bytes()).await?;
                write_half.flush().await?;
            }
            None => {
                write_half.write_all(b"-ERR unknown command\r\n").await?;
                write_half.flush().await?;
            }
        }
    }

    Ok(())
}
