use std::collections::{BTreeMap, HashSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use std::{io, u64};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};
use tokio::sync::RwLock;

#[derive(PartialEq)]
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
}
#[derive(Debug, Clone)]
pub struct HashMapEngine {
    pub hash_map: HashMap<String, String>,
    pub list_map: HashMap<String, VecDeque<String>>,
    pub stream_map: HashMap<String, BTreeMap<String, String>>,
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

                println!("Start range: {:?}", start_range);
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
    pub async fn compute<T: Engine>(
        &self,
        writter: &Arc<RwLock<T>>,
        lock: &Arc<RwLock<HashSet<String>>>,
    ) -> String {
        match self {
            AppCommand::Ping => String::from("+PONG\r\n"),
            AppCommand::Echo(msg) => format!("+{}\r\n", msg),
            AppCommand::Set(key, value, ttl) => {
                let mut engine = writter.write().await;
                engine.set(key.to_string(), value.to_string());
                if *ttl > 0 {
                    let duration = generate_duration(*ttl);
                    let expiration_key = format!("{}_expiration", key);
                    engine.set(expiration_key, duration);
                }
                format!("+OK\r\n")
            }
            AppCommand::Get(key) => {
                let mut engine = writter.write().await;
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
                let mut engine = writter.write().await;
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
                let engine = writter.write().await;
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

                let mut engine = writter.write().await;
                let list_key = format!("{}_list", key);

                let count = engine.list_push_left_many(list_key, reversed_values);
                return RespFormatter::format_integer(count);
            }
            AppCommand::LLen(key) => {
                let engine = writter.read().await;
                let list_key = format!("{}_list", key);
                let count = engine.list_count(&list_key);
                return RespFormatter::format_integer(count);
            }
            AppCommand::LPOP(key, len) => {
                let mut engine = writter.write().await;
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
                        lock.write().await;
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
                    let mut engine = writter.write().await;
                    let mut writter: tokio::sync::RwLockWriteGuard<'_, HashSet<String>> =
                        lock.write().await;

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
                    }
                }
            }
            AppCommand::Type(key) => {
                let engine = writter.read().await;
                if engine.get(key).is_some() {
                    return String::from("+string\r\n");
                } else if engine.stream_exists(key) {
                    return String::from("+stream\r\n");
                } else {
                    return String::from("+none\r\n");
                }
            }
            AppCommand::XAdd(key, id, values) => {
                let mut engine = writter.write().await;

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
                let engine = writter.read().await;
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
                let mut per_stream: Vec<(String, Vec<(String, Vec<String>)>)> = Vec::new();
                let keys: Vec<String> = keys.split('\r').map(|s| s.to_string()).collect();
                let ids: Vec<String> = ids.split('\r').map(|s| s.to_string()).collect();

                // Sleep with tokio to duration (ms)
                if *duration > 0 {
                    tokio::time::sleep(Duration::from_millis(*duration as u64)).await;
                }
                let engine: tokio::sync::RwLockReadGuard<'_, T> = writter.read().await;

                let mut handled_keys: HashSet<String> = HashSet::new();

                for _ in 0..2 {
                    for (key, id) in keys.iter().zip(ids.iter()) {
                        if !engine.stream_exists(key) || handled_keys.contains(key) {
                            continue;
                        }

                        let data = engine.stream_search_range(key, id.clone(), "++".to_string());

                        if !data.is_empty() {
                            handled_keys.insert(key.clone());
                        }

                        let results_vec: Vec<(String, Vec<String>)> = data
                            .into_iter()
                            .map(|(id, payload)| {
                                let fields: Vec<String> =
                                    payload.split_whitespace().map(|s| s.to_string()).collect();
                                (id, fields)
                            })
                            .collect();

                        if !results_vec.is_empty() {
                            per_stream.push((key.clone(), results_vec));
                        }

                        if handled_keys.len() == keys.len() {
                            break;
                        }

                        if *duration > 0 {
                            tokio::time::sleep(Duration::from_millis(*duration as u64)).await;
                        } else {
                            break;
                        }
                    }
                }

                return RespFormatter::format_xread(&per_stream);
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

                    Some(AppCommand::XRead(0, keys, ids))
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
