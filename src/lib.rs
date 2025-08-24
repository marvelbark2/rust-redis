use bytes::{BufMut, BytesMut};
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use std::{io, u64};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::RwLock;

use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
#[derive(Debug)]
pub struct ReplicationsManager {
    clients: Vec<Arc<tokio::sync::Mutex<OwnedWriteHalf>>>,
}

impl ReplicationsManager {
    pub fn new() -> Self {
        ReplicationsManager {
            clients: Vec::new(),
        }
    }

    pub fn add_client(&mut self, client: Arc<tokio::sync::Mutex<OwnedWriteHalf>>) {
        self.clients.push(client);
    }

    pub async fn broadcast(&mut self, messages: Vec<String>) -> io::Result<()> {
        let message = messages.as_slice();

        let mut disconnected_clients: Vec<usize> = Vec::new();
        for (idx, client) in self.clients.iter().enumerate() {
            let mut client = client.lock().await;

            if let Err(e) = client
                .write_all(RespFormatter::format_array(&message).as_bytes())
                .await
            {
                eprintln!("Error writing to client: {}", e);
                disconnected_clients.push(idx);
            } else {
                client.flush().await?;
            }
        }

        // Remove disconnected clients
        for idx in disconnected_clients.iter().rev() {
            self.clients.remove(*idx);
        }
        Ok(())
    }
}

pub struct ReplicationClient {
    master_host: String,
    master_port: u16,
    local_listen_port: String,

    reader: Option<BufReader<OwnedReadHalf>>,
    writer: Option<OwnedWriteHalf>,
}

impl ReplicationClient {
    pub fn new(master: &str, local_listen_port: String) -> Self {
        let mut it = master.split_whitespace();
        let host = it.next().unwrap_or("127.0.0.1").to_string();
        let port = it
            .next()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(6379);

        Self {
            master_host: host,
            master_port: port,
            local_listen_port,
            reader: None,
            writer: None,
        }
    }

    pub async fn connect_and_handshake(&mut self) -> io::Result<()> {
        let addr = format!("{}:{}", self.master_host, self.master_port);
        let stream = TcpStream::connect(&addr).await?;
        stream.set_nodelay(true)?;

        let (rd, mut wr) = stream.into_split();
        let mut reader = BufReader::new(rd);

        // 1) PING
        wr.write_all(&Self::resp_array(&[b"PING"])).await?;
        let pong = Self::read_resp_line(&mut reader).await?;
        if !pong.starts_with(b"+PONG") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unexpected PING reply: {:?}",
                    String::from_utf8_lossy(&pong)
                ),
            ));
        }

        // 2) REPLCONF listening-port <n>
        wr.write_all(&Self::resp_array(&[
            b"REPLCONF",
            b"listening-port",
            self.local_listen_port.as_bytes(),
        ]))
        .await?;
        let ok1 = Self::read_resp_line(&mut reader).await?;
        if !ok1.starts_with(b"+OK") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "REPLCONF listening-port not OK",
            ));
        }

        // 3) REPLCONF capa psync2
        wr.write_all(&Self::resp_array(&[b"REPLCONF", b"capa", b"psync2"]))
            .await?;
        let ok2 = Self::read_resp_line(&mut reader).await?;
        if !ok2.starts_with(b"+OK") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "REPLCONF capa psync2 not OK",
            ));
        }

        self.reader = Some(reader);
        self.writer = Some(wr);
        Ok(())
    }

    pub async fn psync(&mut self, runid: Option<&[u8]>, offset: i64) -> io::Result<Vec<u8>> {
        let runid = runid.unwrap_or(b"?");
        let off_s = offset.to_string();

        let w = self
            .writer
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "no writer"))?;
        let r = self
            .reader
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "no reader"))?;

        // Send PSYNC
        w.write_all(&Self::resp_array(&[b"PSYNC", runid, off_s.as_bytes()]))
            .await?;

        let status = Self::read_resp_line(r).await; // e.g. +FULLRESYNC ...\r\n

        status
    }

    /// If you prefer pulling the RDB *after* PSYNC, keep this;
    /// otherwise you can rely on psync() returning it.
    pub async fn after_psync_rdb_content(&mut self) -> io::Result<Vec<u8>> {
        let r = self
            .reader
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "no reader"))?;

        // Expect a bulk header: $<len>\r\n  or  $-1\r\n
        Self::read_rdb_frame(r).await
    }

    pub async fn listen_for_replication<T: Engine + Send + Sync + 'static>(
        mut self,
        payload: StreamPayload<T>,
    ) {
        let rdb_file = self.after_psync_rdb_content().await;
        if let Ok(rdb) = rdb_file {
            println!("Received RDB of {} bytes", rdb.len());
        } else {
            eprintln!("Error reading RDB after PSYNC: {}", rdb_file.err().unwrap());
        }
        tokio::spawn(async move {
            loop {
                let cmd_parts = match self.read_resp_array().await {
                    Ok(parts) => {
                        if parts.is_empty() {
                            continue;
                        }
                        println!("Received command parts: {:?}", parts);
                        parts
                    }
                    Err(_) => continue,
                };

                if let Some(cmd) = AppCommand::from_parts_simple(cmd_parts) {
                    if cmd == AppCommand::REPLCONF("GETACK".to_uppercase(), "0".to_string()) {
                        let bytes = cmd.compute(&payload).await;

                        let w = self
                            .writer
                            .as_mut()
                            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "no writer"))
                            .unwrap();

                        w.write_all(bytes.as_bytes()).await.unwrap();
                    } else {
                        cmd.compute(&payload).await;
                    }
                }
            }
        });
    }

    pub async fn read_line(&mut self) -> io::Result<Vec<u8>> {
        let r = self
            .reader
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "no reader"))?;
        Self::read_resp_line(r).await
    }

    pub async fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        let w = self
            .writer
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "no writer"))?;
        w.write_all(buf).await
    }

    fn resp_array(parts: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            out.extend_from_slice(p);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    async fn read_rdb_frame<R>(reader: &mut R) -> io::Result<Vec<u8>>
    where
        R: AsyncBufRead + Unpin,
    {
        // 1) Expect '$'
        let mut lead = [0u8; 1];
        reader.read_exact(&mut lead).await?;
        if lead[0] != b'$' {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected '$' for RDB bulk",
            ));
        }

        // 2) Read length line (must end with CRLF)
        let mut len_line = Vec::with_capacity(32);
        reader.read_until(b'\n', &mut len_line).await?;
        if len_line.len() < 2 || len_line[len_line.len() - 2] != b'\r' {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "length line missing CRLF",
            ));
        }
        len_line.truncate(len_line.len() - 2);

        // 3) Parse decimal length (handle $-1 as error for RDB)
        let s = std::str::from_utf8(&len_line)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "length not UTF-8"))?;
        let len: isize = s
            .trim()
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "length not a number"))?;
        if len == -1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "null bulk for RDB",
            ));
        }
        if len < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "negative bulk length",
            ));
        }
        let len = len as usize;

        // (Optional) guard against absurd sizes
        // const MAX_RDB: usize = 1 << 30; // 1 GiB
        // if len > MAX_RDB { return Err(io::Error::new(io::ErrorKind::InvalidData, "RDB too large")); }

        // 4) Read exactly `len` bytes (binary-safe)
        let mut rdb = vec![0u8; len];
        reader.read_exact(&mut rdb).await?;

        Ok(rdb)
    }

    /// Read a RESP Array into Vec<String>. Binary bulk elements are returned as
    /// a placeholder string noting length, to avoid UTF-8 loss.
    async fn read_resp_array(&mut self) -> io::Result<Vec<String>> {
        let r = self
            .reader
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "no reader"))?;

        // First token: either *<n>\r\n or an inline/simple string
        let first = Self::read_resp_line(r).await?;
        if first.is_empty() {
            return Ok(vec![]);
        }

        if first[0] != b'*' {
            // Treat as inline command (space-separated), trimming CRLF
            let s = String::from_utf8_lossy(&first)
                .trim()
                .trim_end_matches('\n')
                .trim_end_matches('\r')
                .to_string();
            if s.is_empty() {
                return Ok(vec![]);
            }
            return Ok(s.split_whitespace().map(|x| x.to_string()).collect());
        }

        // Parse array length
        let count: usize = Self::parse_len(&first[1..])?;

        let mut parts = Vec::with_capacity(count);
        for _ in 0..count {
            // Each bulk: $<len>\r\n, then payload + CRLF
            let bulk_hdr = Self::read_resp_line(r).await?;
            if bulk_hdr.is_empty() || bulk_hdr[0] != b'$' {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Expected bulk header",
                ));
            }

            // Read element; may be None for $-1
            let elem = Self::read_resp_bulk_from_header(bulk_hdr, r).await?;
            match elem {
                Some(bytes) => match String::from_utf8(bytes) {
                    Ok(s) => parts.push(s),
                    Err(_) => parts.push(format!("<binary:{}>", parts.len())),
                },
                None => parts.push("(nil)".to_string()),
            }
        }
        Ok(parts)
    }

    // ---------------------------
    // Helpers (RESP parsing)
    // ---------------------------

    /// Read exactly one RESP "line": bytes ending with LF, and ensure CR precedes LF.
    /// Returns the full line INCLUDING CRLF.
    async fn read_resp_line<R: tokio::io::AsyncBufRead + Unpin>(
        reader: &mut R,
    ) -> io::Result<Vec<u8>> {
        let mut line = Vec::new();
        let n = reader.read_until(b'\n', &mut line).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "EOF while reading line",
            ));
        }
        if line.len() < 2 || line[line.len() - 2] != b'\r' || *line.last().unwrap() != b'\n' {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "missing CRLF"));
        }

        Ok(line)
    }

    /// Parse decimal length from "<digits>\r\n" or " -1\r\n" slices (without the leading type byte).
    fn parse_len(bytes_with_crlf: &[u8]) -> io::Result<usize> {
        if bytes_with_crlf.len() < 2 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "short len"));
        }
        let s = std::str::from_utf8(&bytes_with_crlf[..bytes_with_crlf.len() - 2])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "len not ascii"))?
            .trim();
        if s == "-1" {
            // caller decides how to interpret nulls; for arrays it's invalid here
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "null length where not allowed",
            ));
        }
        s.parse::<usize>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "len not a number"))
    }

    /// Read a bulk value when you already consumed its `$<len>\r\n` header line.
    /// Returns Ok(Some(bytes)) for normal bulk, Ok(None) for $-1.
    async fn read_resp_bulk_from_header<R: tokio::io::AsyncBufRead + Unpin>(
        header: Vec<u8>,
        reader: &mut R,
    ) -> io::Result<Option<Vec<u8>>> {
        // header looks like: b"$123\r\n" or b"$-1\r\n"
        let s = std::str::from_utf8(&header[1..header.len() - 2])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid bulk length"))?;
        let len: isize = s.trim().parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid bulk length number")
        })?;

        if len == -1 {
            return Ok(None); // Null Bulk String
        }
        if len < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "negative bulk length",
            ));
        }

        let len = len as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;

        // consume trailing CRLF
        let mut tail = [0u8; 2];
        reader.read_exact(&mut tail).await?;
        if tail != *b"\r\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid bulk tail: {:?}", tail),
            ));
        }
        Ok(Some(buf))
    }
}

#[derive(Debug, Clone)]
pub struct StreamPayload<T: Engine> {
    pub writter: Arc<RwLock<T>>,
    pub lock: Arc<RwLock<HashSet<String>>>,
    pub replica_of: String,
    pub master_replid: String,
    pub replica_manager: Arc<RwLock<ReplicationsManager>>,
}

#[derive(PartialEq, Debug)]
pub enum AppCommand {
    Ping,
    Echo(String),
    Set(String, String, i32),
    Get(String),
    Del(String),
    Keys(String),
    Exists(String),
    RPush(String, String),
    LRANGE(String, i32, i32),
    LPush(String, String),
    LLen(String),
    LPOP(String, i32),
    BLPOP(String, f32),
    Type(String),
    XAdd(String, String, String),
    XRange(String, String, String),
    XRead(i32, String, String),
    INCR(String),
    INFO(String),
    REPLCONF(String, String),
    None,
}

pub trait Engine {
    fn get(&self, key: &str) -> Option<&String>;
    fn set(&mut self, key: String, value: String);
    fn del(&mut self, key: &str) -> bool;
    fn list_push_left(&mut self, key: String, value: String) -> usize;
    fn list_push_left_many(&mut self, key: String, values: Vec<String>) -> usize;
    fn list_push_right(&mut self, key: String, value: String) -> usize;
    fn list_push_right_many(&mut self, key: String, values: Vec<String>) -> usize;
    fn list_range(&self, key: &str, start: i32, end: i32) -> Vec<String>;
    fn list_count(&self, key: &str) -> usize;
    fn list_pop_left(&mut self, key: &str) -> Option<String>;
    fn stream_push(&mut self, key: String, id: String, value: String) -> String;
    fn stream_exists(&self, key: &str) -> bool;
    fn stream_id_exists(&self, key: &str, id: &str) -> bool;
    fn stream_last_id(&self, key: &str) -> Option<String>;
    fn stream_search_range(&self, key: &str, start: String, end: String) -> Vec<(String, String)>;

    fn rdb_file(&self) -> Vec<u8> {
        vec![]
    }
}
#[derive(Debug, Clone)]
pub struct HashMapEngine {
    pub hash_map: HashMap<String, String>,
    pub list_map: HashMap<String, VecDeque<String>>,
    pub stream_map: HashMap<String, BTreeMap<String, String>>,
    pub rdb_file: Vec<u8>,
}

impl HashMapEngine {
    pub fn new() -> Self {
        HashMapEngine {
            hash_map: HashMap::new(),
            list_map: HashMap::new(),
            stream_map: HashMap::new(),
            rdb_file: Self::init_empty_rdb(),
        }
    }
    pub fn init_empty_rdb() -> Vec<u8> {
        let mut rdb = BytesMut::with_capacity(128);

        // Header (RDB version 9)
        rdb.extend_from_slice(b"REDIS0009");

        // AUX fields
        rdb.put_u8(0xFA); // AUX opcode
        rdb.put_u8(9); // len("redis-ver")
        rdb.extend_from_slice(b"redis-ver");
        rdb.put_u8(5); // len("7.2.0")
        rdb.extend_from_slice(b"7.2.0");

        rdb.put_u8(0xFA);
        rdb.put_u8(10); // len("redis-bits")
        rdb.extend_from_slice(b"redis-bits");
        rdb.put_u8(0xC0);
        rdb.put_u8(64); // integer-encoded 64

        rdb.put_u8(0xFA);
        rdb.put_u8(5); // len("ctime")
        rdb.extend_from_slice(b"ctime");
        rdb.put_u8(0xC2); // int32 encoding
        rdb.extend_from_slice(&0x65BC086Du32.to_le_bytes()); // example timestamp

        rdb.put_u8(0xFA);
        rdb.put_u8(8); // len("used-mem")
        rdb.extend_from_slice(b"used-mem");
        rdb.put_u8(0xC2); // int32 encoding
        rdb.extend_from_slice(&0x0010C4B0u32.to_le_bytes()); // example mem

        rdb.put_u8(0xFA);
        rdb.put_u8(8); // len("aof-base")
        rdb.extend_from_slice(b"aof-base");
        rdb.put_u8(0xC0);
        rdb.put_u8(0); //rdb_bytes integer-encoded 0

        // EOF then CRC64 footer
        rdb.put_u8(0xFF);

        let crc = crc64_ecma(&rdb); // compute across everything so far
        rdb.extend_from_slice(&crc.to_le_bytes());

        rdb.to_vec()
    }
}

impl Engine for HashMapEngine {
    fn get(&self, key: &str) -> Option<&String> {
        self.hash_map.get(key)
    }

    fn set(&mut self, key: String, value: String) {
        self.hash_map.insert(key, value);
    }

    fn del(&mut self, key: &str) -> bool {
        self.hash_map.remove(key).is_some()
    }
    fn list_push_left(&mut self, key: String, value: String) -> usize {
        // if the key does not exist, create a new VecDeque
        // and push the value to the left
        let list = self.list_map.entry(key).or_default();
        list.push_front(value);
        list.len()
    }
    fn list_push_right(&mut self, key: String, value: String) -> usize {
        let list = self.list_map.entry(key).or_default();
        list.push_back(value);
        list.len()
    }
    fn list_range(&self, key: &str, start: i32, end: i32) -> Vec<String> {
        if let Some(list) = self.list_map.get(key) {
            let start = if start < 0 {
                list.len().checked_add_signed(start as isize).unwrap_or(0)
            } else {
                start as usize
            };
            let end = if end < 0 {
                list.len() as i32 + end
            } else {
                end
            };
            list.iter()
                .skip(start)
                .take((end - start as i32 + 1) as usize)
                .cloned()
                .collect()
        } else {
            vec![]
        }
    }
    fn list_count(&self, key: &str) -> usize {
        self.list_map.get(key).map_or(0, |list| list.len())
    }

    fn list_push_left_many(&mut self, key: String, values: Vec<String>) -> usize {
        let list = self.list_map.entry(key).or_default();
        for value in values.into_iter().rev() {
            list.push_front(value);
        }
        list.len()
    }

    fn list_push_right_many(&mut self, key: String, values: Vec<String>) -> usize {
        let list = self.list_map.entry(key).or_default();
        for value in values {
            list.push_back(value);
        }
        list.len()
    }
    fn list_pop_left(&mut self, key: &str) -> Option<String> {
        if let Some(list) = self.list_map.get_mut(key) {
            list.pop_front()
        } else {
            None
        }
    }
    fn stream_push(&mut self, key: String, id: String, value: String) -> String {
        let stream = self.stream_map.entry(key).or_default();
        stream.insert(id.clone(), value);
        id
    }
    fn stream_exists(&self, key: &str) -> bool {
        self.stream_map.contains_key(key)
    }

    fn stream_id_exists(&self, key: &str, id: &str) -> bool {
        if let Some(stream) = self.stream_map.get(key) {
            stream.contains_key(id)
        } else {
            false
        }
    }

    fn stream_last_id(&self, key: &str) -> Option<String> {
        if let Some(stream) = self.stream_map.get(key) {
            stream.last_key_value().map(|(id, _)| id.clone())
        } else {
            None
        }
    }

    fn stream_search_range(&self, key: &str, start: String, end: String) -> Vec<(String, String)> {
        if let Some(stream) = self.stream_map.get(key) {
            let mut results = Vec::new();

            if start == "-" && end == "+" {
                for (id, value) in stream.iter() {
                    results.push((id.clone(), value.clone()));
                }
                return results;
            } else if start == "-" {
                let end_index = if end.contains('-') {
                    end.clone()
                } else {
                    format!("{}-{}", end, u64::MAX)
                };

                for (id, value) in stream.range(..=end_index) {
                    results.push((id.clone(), value.clone()));
                }
                return results;
            } else if end == "++" {
                let start_index = if !start.contains('-') {
                    format!("{}-{}", start, u64::MIN)
                } else {
                    start.clone()
                };

                let start_range = Excluded(start_index);

                for (id, value) in stream.range((start_range, Unbounded)) {
                    results.push((id.clone(), value.clone()));
                }
                return results;
            } else if end == "+" {
                let start_index = if !start.contains('-') {
                    format!("{}-{}", start, u64::MIN)
                } else {
                    start.clone()
                };
                for (id, value) in stream.range(start_index..) {
                    results.push((id.clone(), value.clone()));
                }
                return results;
            } else {
                for (id, value) in stream.range(start..=end) {
                    results.push((id.clone(), value.clone()));
                }
                return results;
            }
        } else {
            return Vec::new(); // Return empty if the stream does not exist
        }
    }

    fn rdb_file(&self) -> Vec<u8> {
        self.rdb_file.clone()
    }
}

fn generate_duration(ms: i32) -> String {
    if ms <= 0 {
        return String::from("0");
    }
    let now = SystemTime::now();
    let deadline = now + Duration::from_millis(ms as u64);
    let epoch_ms = deadline.duration_since(UNIX_EPOCH).unwrap().as_millis();
    epoch_ms.to_string()
}

fn generate_duration_f(ms: f32) -> String {
    if ms <= 0f32 {
        return String::from("0");
    }
    let now = SystemTime::now();
    let deadline = now + Duration::from_millis(ms as u64);
    let epoch_ms = deadline.duration_since(UNIX_EPOCH).unwrap().as_millis();
    epoch_ms.to_string()
}

fn is_expired(time: String) -> bool {
    let Ok(deadline_ms) = time.parse::<u128>() else {
        return true; // invalid -> consider expired
    };

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    now_ms >= deadline_ms
}

impl AppCommand {
    pub async fn compute<T: Engine>(&self, payload: &StreamPayload<T>) -> String {
        match self {
            AppCommand::Ping => String::from("+PONG\r\n"),
            AppCommand::Echo(msg) => format!("+{}\r\n", msg),
            AppCommand::Set(key, value, ttl) => {
                let mut engine = payload.writter.write().await;
                engine.set(key.to_string(), value.to_string());
                if *ttl > 0 {
                    let duration = generate_duration(*ttl);
                    let expiration_key = format!("{}_expiration", key);
                    engine.set(expiration_key, duration);
                }
                format!("+OK\r\n")
            }
            AppCommand::Get(key) => {
                let mut engine = payload.writter.write().await;
                if let Some(v) = engine.get(key) {
                    let expiration_key = format!("{}_expiration", key);
                    if let Some(expiration) = engine.get(&expiration_key) {
                        if is_expired(expiration.clone()) {
                            engine.del(key);
                            engine.del(&expiration_key);

                            return RespFormatter::format_bulk_string("");
                        }
                    }
                    return RespFormatter::format_bulk_string(v);
                } else {
                    return RespFormatter::format_bulk_string("");
                }
            }
            AppCommand::Del(key) => format!("Deleted {}", key),
            AppCommand::Keys(pattern) => format!("Keys matching {} are ...", pattern),
            AppCommand::Exists(key) => format!("{} exists", key),
            AppCommand::RPush(key, value) => {
                let mut engine = payload.writter.write().await;
                let list_key = format!("{}_list", key);

                if value.contains("\r") {
                    let values: Vec<String> = value.split('\r').map(|s| s.to_string()).collect();
                    let count = engine.list_push_right_many(list_key, values);
                    return RespFormatter::format_integer(count);
                }
                let count = engine.list_push_right(list_key, value.clone());
                return RespFormatter::format_integer(count);
            }
            AppCommand::LRANGE(key, start_index, end_index) => {
                let engine = payload.writter.write().await;
                let list_key = format!("{}_list", key);

                if start_index > end_index && *end_index >= 0 && *start_index >= 0 {
                    return String::from("*0\r\n");
                }

                let values = engine.list_range(&list_key, *start_index, *end_index);

                return RespFormatter::format_array(&values);
            }
            AppCommand::LPush(key, values) => {
                let reversed_values: Vec<String> =
                    values.split('\r').rev().map(|s| s.to_string()).collect();

                let mut engine = payload.writter.write().await;
                let list_key = format!("{}_list", key);

                let count = engine.list_push_left_many(list_key, reversed_values);
                return RespFormatter::format_integer(count);
            }
            AppCommand::LLen(key) => {
                let engine = payload.writter.read().await;
                let list_key = format!("{}_list", key);
                let count = engine.list_count(&list_key);
                return RespFormatter::format_integer(count);
            }
            AppCommand::LPOP(key, len) => {
                let mut engine = payload.writter.write().await;
                let list_key = format!("{}_list", key);

                if len > &1 {
                    let mut values = Vec::new();
                    for _ in 0..*len {
                        if let Some(value) = engine.list_pop_left(&list_key) {
                            values.push(value);
                        } else {
                            break;
                        }
                    }
                    return RespFormatter::format_array(&values);
                } else if let Some(value) = engine.list_pop_left(&list_key) {
                    return RespFormatter::format_bulk_string(&value);
                } else {
                    return RespFormatter::format_bulk_string("");
                }
            }
            AppCommand::BLPOP(keys, seconds) => {
                let mut list_key: Vec<String> = keys.split('\r').map(|s| s.to_string()).collect();
                let mut result: Vec<String> = Vec::new();
                let ms_duration = generate_duration_f(*seconds * 1000_f32);

                loop {
                    let mut writter: tokio::sync::RwLockWriteGuard<'_, HashSet<String>> =
                        payload.lock.write().await;
                    let mut locked = false;
                    for key in &list_key {
                        if writter.contains(key) {
                            locked = true;
                            break;
                        } else {
                            writter.insert(key.clone());
                        }
                    }

                    if !locked {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                loop {
                    let mut engine = payload.writter.write().await;
                    let mut writter: tokio::sync::RwLockWriteGuard<'_, HashSet<String>> =
                        payload.lock.write().await;

                    list_key.retain(|key| {
                        let key_list = format!("{}_list", key);
                        if let Some(value) = engine.list_pop_left(&key_list) {
                            result.push(key.clone());
                            result.push(value);

                            writter.remove(key);
                            false
                        } else {
                            true
                        }
                    });

                    if list_key.is_empty()
                        || (seconds > &(0_f32) && is_expired(ms_duration.clone()))
                    {
                        if result.is_empty() {
                            return RespFormatter::format_bulk_string("");
                        } else {
                            return RespFormatter::format_array(&result);
                        }
                    } else if seconds == &(0_f32) {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
            AppCommand::Type(key) => {
                let engine = payload.writter.read().await;
                if engine.get(key).is_some() {
                    return String::from("+string\r\n");
                } else if engine.stream_exists(key) {
                    return String::from("+stream\r\n");
                } else {
                    return String::from("+none\r\n");
                }
            }
            AppCommand::XAdd(key, id, values) => {
                let mut engine = payload.writter.write().await;

                let mut id_str = String::from(id);

                if id_str == "*" {
                    let unix = generate_duration_f(0.0001_f32);
                    id_str = format!("{}-0", unix);
                } else {
                    let id_parts = id.split('-').collect::<Vec<&str>>();
                    if id_parts.len() != 2 {
                        return RespFormatter::format_error("Invalid ID format");
                    }

                    let ms = id_parts[0].parse::<u64>().map_err(|_| "Invalid ms");

                    if id_parts[1] == "*" {
                        if ms.is_err() {
                            return RespFormatter::format_error("Invalid ID format");
                        }

                        let ms = ms.unwrap();
                        let last_id = engine.stream_last_id(key);
                        let seq = if let Some(last_id) = last_id {
                            let last_parts = last_id.split('-').collect::<Vec<&str>>();
                            if last_parts.len() != 2 {
                                return RespFormatter::format_error("Invalid last ID format");
                            }

                            if last_parts[0] != id_parts[0] && id_parts[0] == "0" {
                                1
                            } else if last_parts[0] != id_parts[0] {
                                0
                            } else {
                                let last_seq = last_parts[1].parse::<u64>().unwrap_or(0);
                                last_seq + 1
                            }
                        } else if id_parts[0] == "0" {
                            1
                        } else {
                            0
                        };

                        id_str = format!("{}-{}", ms, seq);
                    } else {
                        let seq = id_parts[1].parse::<u64>().map_err(|_| "Invalid seq");

                        if ms.is_err() || seq.is_err() {
                            return RespFormatter::format_error("Invalid ID format");
                        }

                        if ms.unwrap() == 0 && seq.unwrap() == 0 {
                            return RespFormatter::format_error(
                                "The ID specified in XADD must be greater than 0-0",
                            );
                        }

                        //  &id.to_string() > first_id
                        let last_id = engine.stream_last_id(key);
                        if let Some(first_id) = last_id {
                            if id < &first_id || engine.stream_id_exists(key, id) {
                                return RespFormatter::format_error(
                            "The ID specified in XADD is equal or smaller than the target stream top item",);
                            }
                        }
                    }
                }

                let id = engine.stream_push(key.clone(), id_str, values.clone());
                return RespFormatter::format_bulk_string(&id);
            }
            AppCommand::XRange(key, min, max) => {
                let engine = payload.writter.read().await;
                if !engine.stream_exists(&key) {
                    return RespFormatter::format_array(&[]);
                }

                let start = min.clone();
                let end = max.clone();

                let results = engine.stream_search_range(key, start, end);
                if results.is_empty() {
                    return RespFormatter::format_array(&[]);
                }

                let results_vec: Vec<(String, Vec<String>)> = results
                    .into_iter()
                    .map(|(id, payload)| {
                        let fields: Vec<String> =
                            payload.split_whitespace().map(|s| s.to_string()).collect();
                        (id, fields)
                    })
                    .collect();

                let resp = RespFormatter::format_xrange(&results_vec);
                return resp;
            }
            AppCommand::XRead(duration, keys, ids) => {
                let keys: Vec<String> = keys.split('\r').map(|s| s.to_string()).collect();

                let ids: Vec<String> = {
                    let engine = payload.writter.read().await;
                    ids.split('\r')
                        .enumerate()
                        .map(|(i, s)| {
                            if s == "$" {
                                let last_id = engine.stream_last_id(&keys[i]);
                                if let Some(last_id) = last_id {
                                    return last_id;
                                }
                                return "".to_string();
                            } else {
                                return s.to_string();
                            }
                        })
                        .collect()
                }; // Lock is released here

                let check_streams = |engine: &T| -> Vec<(String, Vec<(String, Vec<String>)>)> {
                    let mut per_stream: Vec<(String, Vec<(String, Vec<String>)>)> = Vec::new();

                    for (key, id) in keys.iter().zip(ids.iter()) {
                        if !engine.stream_exists(key) {
                            continue;
                        }

                        let data = engine.stream_search_range(key, id.clone(), "++".to_string());

                        if !data.is_empty() {
                            let results_vec: Vec<(String, Vec<String>)> = data
                                .into_iter()
                                .map(|(id, payload)| {
                                    let fields: Vec<String> =
                                        payload.split_whitespace().map(|s| s.to_string()).collect();
                                    (id, fields)
                                })
                                .collect();
                            per_stream.push((key.clone(), results_vec));
                        }
                    }
                    per_stream
                };

                let initial_results = {
                    let engine = payload.writter.read().await;
                    check_streams(&*engine)
                };

                if !initial_results.is_empty() {
                    return RespFormatter::format_xread(&initial_results);
                }

                if *duration < 0 {
                    return RespFormatter::format_xread(&[]);
                }

                if *duration == 0 {
                    loop {
                        tokio::time::sleep(Duration::from_millis(10)).await;

                        let results = {
                            let engine = payload.writter.read().await;
                            check_streams(&*engine)
                        };

                        if !results.is_empty() {
                            return RespFormatter::format_xread(&results);
                        }
                    }
                } else {
                    let timeout = Duration::from_millis(*duration as u64);
                    tokio::time::sleep(timeout).await;

                    let final_results = {
                        let engine = payload.writter.read().await;
                        check_streams(&*engine)
                    };

                    return RespFormatter::format_xread(&final_results);
                }
            }
            AppCommand::INCR(key) => {
                let mut engine = payload.writter.write().await;
                let value = engine.get(key).cloned().unwrap_or_else(|| "0".to_string());
                let maybe_value = value.parse();
                if maybe_value.is_err() {
                    return RespFormatter::format_error("value is not an integer or out of range");
                }
                let new_value: i64 = maybe_value.unwrap_or(0) + 1;
                engine.set(key.clone(), new_value.to_string());
                return RespFormatter::format_integer(new_value as usize);
            }
            AppCommand::INFO(_key) => {
                let msg = if payload.replica_of.is_empty() {
                    "role:master"
                } else {
                    "role:slave"
                };

                // Add master_replid to the message
                let msg = format!(
                    "{}\r\nmaster_replid:{}\r\nmaster_repl_offset:0",
                    msg, payload.master_replid
                );

                return RespFormatter::format_bulk_string(msg.as_str());
            }
            AppCommand::None => String::from("-ERR Unknown command\r\n"),
            AppCommand::REPLCONF(subcommand, value) => {
                if subcommand == "GETACK" && value == "0" {
                    let data = [
                        "REPLCONF".to_uppercase(),
                        "ACK".to_uppercase(),
                        "0".to_string(),
                    ];
                    let bytes = RespFormatter::format_array(&data);
                    return bytes;
                }
                return String::from("+OK\r\n");
            }
        }
    }

    pub fn from_parts_simple(parts: Vec<String>) -> Option<Self> {
        if parts.is_empty() {
            return None;
        }

        let len = parts.len();

        match parts[0].to_uppercase().as_str() {
            "PING" => Some(AppCommand::Ping),
            "ECHO" if len > 1 => Some(AppCommand::Echo(parts[1].clone())),
            "SET" if len > 2 => Some(AppCommand::Set(
                parts[1].clone(),
                parts[2].clone(),
                if parts.len() > 4 {
                    let ex = parts[4].parse().unwrap_or(0);
                    let unit = parts[3].clone();
                    if unit == "EX" || unit == "ex" {
                        ex * 1000 // Convert seconds to milliseconds
                    } else if unit == "PX" || unit == "px" {
                        ex // Already in milliseconds
                    } else {
                        0 // Default to 0 if no valid unit is provided
                    }
                } else {
                    0
                },
            )),
            "GET" if len > 1 => Some(AppCommand::Get(parts[1].clone())),
            "DEL" if len > 1 => Some(AppCommand::Del(parts[1].clone())),
            "KEYS" if len > 1 => Some(AppCommand::Keys(parts[1].clone())),
            "EXISTS" if len > 1 => Some(AppCommand::Exists(parts[1].clone())),
            "RPUSH" if len > 2 => {
                let all_items = parts[2..].join("\r");
                Some(AppCommand::RPush(parts[1].clone(), all_items))
            }
            "LRANGE" if len > 3 => {
                let key = parts[1].clone();
                let start_index: i32 = parts[2].parse().unwrap_or(0);
                let end_index: i32 = parts[3].parse().unwrap_or(-1);
                Some(AppCommand::LRANGE(key, start_index, end_index))
            }
            "LPUSH" if len > 2 => Some(AppCommand::LPush(parts[1].clone(), parts[2..].join("\r"))),
            "LLEN" if len > 1 => Some(AppCommand::LLen(parts[1].clone())),
            "LPOP" if len > 2 => Some(AppCommand::LPOP(
                parts[1].clone(),
                parts[2].parse().unwrap_or(1),
            )),
            "LPOP" if len > 1 => Some(AppCommand::LPOP(parts[1].clone(), 1)),
            "BLPOP" if len > 2 => {
                let timeout_str: Result<f32, _> = parts[len - 1].parse();

                if !timeout_str.is_ok() {
                    return None;
                } else {
                    let keys = parts[1..len - 1].to_vec();
                    Some(AppCommand::BLPOP(keys.join("\r"), timeout_str.unwrap()))
                }
            }
            "TYPE" if len > 1 => Some(AppCommand::Type(parts[1].clone())),
            "XADD" if len > 2 => {
                if len < 4 || len % 2 == 0 {
                    return None;
                }
                let values = parts[3..].join("\r");
                Some(AppCommand::XAdd(parts[1].clone(), parts[2].clone(), values))
            }
            "XRANGE" if len > 3 => {
                if len < 4 {
                    return None;
                }
                Some(AppCommand::XRange(
                    parts[1].clone(),
                    parts[2].clone(),
                    parts[3].clone(),
                ))
            }
            "XREAD" if len > 3 => {
                let type_xread = parts[1].to_uppercase();
                if type_xread == "STREAMS" {
                    let after_streams = &parts[2..];

                    if after_streams.len() % 2 != 0 {
                        return None; // Must be even number of streams and ids
                    }
                    // split in half
                    let mid = after_streams.len() / 2;
                    let keys = after_streams[..mid].join("\r");
                    let ids = after_streams[mid..].join("\r");

                    Some(AppCommand::XRead(-1, keys, ids))
                } else {
                    let after_streams = &parts[4..];

                    if after_streams.len() % 2 != 0 {
                        return None; // Must be even number of streams and ids
                    }
                    // split in half
                    let mid = after_streams.len() / 2;
                    let keys = after_streams[..mid].join("\r");
                    let ids = after_streams[mid..].join("\r");

                    let duration = parts[2].parse::<i32>().unwrap_or(0);
                    Some(AppCommand::XRead(duration, keys, ids))
                }
            }
            "INCR" if len > 1 => Some(AppCommand::INCR(parts[1].clone())),
            "INFO" if len > 1 => Some(AppCommand::INFO(parts[1].clone())),
            "REPLCONF" if len > 2 => Some(AppCommand::REPLCONF(parts[1].clone(), parts[2].clone())),
            _ => None,
        }
    }
}

pub struct AppCommandParser {
    line: String,
}

impl AppCommandParser {
    pub fn new() -> Self {
        Self {
            line: String::new(),
        }
    }

    pub async fn parse_resp_array_async<R>(&mut self, reader: &mut R) -> io::Result<Vec<String>>
    where
        R: AsyncBufRead + Unpin,
    {
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        self.line = line;

        if !self.line.starts_with('*') {
            return self.parse_simple_async();
        }
        let count: usize = self.line[1..]
            .trim()
            .parse()
            .map_err(|_| io::ErrorKind::InvalidData)?;

        let mut parts = Vec::with_capacity(count);

        for _ in 0..count {
            let mut line = String::new();
            reader.read_line(&mut line).await?;
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
            reader.read_exact(&mut buf).await?;

            let part = match String::from_utf8(buf.clone()) {
                Ok(s) => s,
                Err(_) => format!("Value (binary) length: {}", buf.len()),
            };

            parts.push(part);

            // Consume trailing \r\n
            let mut crlf = [0u8; 2];
            reader.read_exact(&mut crlf).await?;
        }
        Ok(parts)
    }

    /// Inline/simple command parser (space-separated), returning bytes.
    fn parse_simple_async(&self) -> io::Result<Vec<String>> {
        let line = self.line.trim_end(); // drop trailing \r\n
        if line.is_empty() {
            return Ok(vec![]);
        }

        let parts = line
            .split_whitespace()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        Ok(parts)
    }
}

pub struct RespFormatter {}

impl RespFormatter {
    pub fn format_string(strings: &[String]) -> String {
        let mut result = String::new();
        for s in strings {
            result.push_str(&format!("${}\r\n{}\r\n", s.len(), s));
        }
        result
    }

    pub fn format_integer(value: usize) -> String {
        format!(":{}\r\n", value)
    }

    pub fn format_error(message: &str) -> String {
        format!("-ERR {}\r\n", message)
    }
    pub fn format_bulk_string(value: &str) -> String {
        if value.is_empty() {
            return String::from("$-1\r\n");
        }

        // remove double quotes if they exist like "one" -> one but not "one -> one
        if value.starts_with('"') && value.ends_with('"') {
            return format!(
                "${}\r\n{}\r\n",
                value.len() - 2,
                value[1..value.len() - 1].to_string()
            );
        }

        // REmove extra  \r\n if ended with \r\n
        let value = if value.ends_with("\r\n") {
            &value[..value.len() - 2]
        } else {
            value
        };

        format!("${}\r\n{}\r\n", value.len(), value)
    }
    pub fn format_array(values: &[String]) -> String {
        let mut result = String::new();
        result.push_str(&format!("*{}\r\n", values.len()));
        for value in values {
            result.push_str(&RespFormatter::format_bulk_string(value));
        }
        result
    }

    // values are already parsed
    pub fn format_array_result(values: &[String]) -> String {
        if values.is_empty() {
            return String::from("*0\r\n");
        }
        let mut result = String::new();
        result.push_str(&format!("*{}\r\n", values.len()));
        for value in values {
            result.push_str(value);
        }
        result
    }

    pub fn format_xrange(entries: &[(String, Vec<String>)]) -> String {
        let mut out = String::new();
        out.push_str(&format!("*{}\r\n", entries.len()));

        for (id, fields) in entries {
            out.push_str("*2\r\n");
            out.push_str(&Self::format_bulk_string(id));
            out.push_str(&Self::format_array(fields));
        }
        out
    }

    pub fn format_xread(streams: &[(String, Vec<(String, Vec<String>)>)]) -> String {
        if streams.is_empty() {
            return String::from("$-1\r\n");
        }
        let mut out = String::new();
        out.push_str(&format!("*{}\r\n", streams.len()));

        for (stream_name, entries) in streams {
            out.push_str("*2\r\n");

            out.push_str(&RespFormatter::format_bulk_string(stream_name));

            out.push_str(&format!("*{}\r\n", entries.len()));
            for (id, fields) in entries {
                out.push_str("*2\r\n");
                out.push_str(&RespFormatter::format_bulk_string(id));
                out.push_str(&RespFormatter::format_array(fields)); // fields is Vec<String>
            }
        }

        out
    }
}

const CRC64_ECMA_TABLE: [u64; 256] = {
    let mut table = [0u64; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u64;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xC96C5795D7870F42; // reversed poly
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// Compute CRC64 using ECMA-182 polynomial.
/// This is exactly what Redis uses in RDB checksum.
pub fn crc64_ecma(buf: &[u8]) -> u64 {
    let mut crc: u64 = 0;
    for &b in buf {
        let idx = ((crc ^ (b as u64)) & 0xFF) as usize;
        crc = CRC64_ECMA_TABLE[idx] ^ (crc >> 8);
    }
    crc
}
