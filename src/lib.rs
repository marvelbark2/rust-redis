use std::{
    collections::{HashMap, VecDeque},
    io::{self, BufRead},
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(PartialEq, Eq)]
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
}
#[derive(Debug, Clone)]
pub struct HashMapEngine {
    pub hash_map: HashMap<String, String>,
    pub list_map: HashMap<String, VecDeque<String>>,
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
}

fn generate_duration(ms: i32) -> String {
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
    pub fn compute<T: Engine>(&self, writter: &Arc<RwLock<T>>) -> String {
        match self {
            AppCommand::Ping => String::from("+PONG\r\n"),
            AppCommand::Echo(msg) => format!("+{}\r\n", msg),
            AppCommand::Set(key, value, ttl) => {
                let mut engine = writter.write().unwrap();
                engine.set(key.to_string(), value.to_string());
                if *ttl > 0 {
                    let duration = generate_duration(*ttl);
                    let expiration_key = format!("{}_expiration", key);
                    engine.set(expiration_key, duration);
                }
                format!("+OK\r\n")
            }
            AppCommand::Get(key) => {
                let mut engine = writter.write().unwrap();
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
                let mut engine = writter.write().unwrap();
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
                let engine = writter.write().unwrap();
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

                let mut engine: std::sync::RwLockWriteGuard<'_, T> = writter.write().unwrap();
                let list_key = format!("{}_list", key);

                let count = engine.list_push_left_many(list_key, reversed_values);
                return RespFormatter::format_integer(count);
            }
            AppCommand::LLen(key) => {
                let engine = writter.read().unwrap();
                let list_key = format!("{}_list", key);
                let count = engine.list_count(&list_key);
                return RespFormatter::format_integer(count);
            }
            AppCommand::LPOP(key, len) => {
                let mut engine = writter.write().unwrap();
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
        }
    }

    pub fn from_parts_simple(parts: Vec<String>) -> Option<Self> {
        if parts.is_empty() {
            return None;
        }

        match parts[0].to_uppercase().as_str() {
            "PING" => Some(AppCommand::Ping),
            "ECHO" if parts.len() > 1 => Some(AppCommand::Echo(parts[1].clone())),
            "SET" if parts.len() > 2 => Some(AppCommand::Set(
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
            "GET" if parts.len() > 1 => Some(AppCommand::Get(parts[1].clone())),
            "DEL" if parts.len() > 1 => Some(AppCommand::Del(parts[1].clone())),
            "KEYS" if parts.len() > 1 => Some(AppCommand::Keys(parts[1].clone())),
            "EXISTS" if parts.len() > 1 => Some(AppCommand::Exists(parts[1].clone())),
            "RPUSH" if parts.len() > 2 => {
                let all_items = parts[2..].join("\r");
                Some(AppCommand::RPush(parts[1].clone(), all_items))
            }
            "LRANGE" if parts.len() > 3 => {
                let key = parts[1].clone();
                let start_index: i32 = parts[2].parse().unwrap_or(0);
                let end_index: i32 = parts[3].parse().unwrap_or(-1);
                Some(AppCommand::LRANGE(key, start_index, end_index))
            }
            "LPUSH" if parts.len() > 2 => {
                Some(AppCommand::LPush(parts[1].clone(), parts[2..].join("\r")))
            }
            "LLEN" if parts.len() > 1 => Some(AppCommand::LLen(parts[1].clone())),
            "LPOP" if parts.len() > 1 => Some(AppCommand::LPOP(
                parts[1].clone(),
                parts[2].parse().unwrap_or(1),
            )),
            _ => None,
        }
    }
}

pub struct AppCommandParser {
    line: String,
}

impl AppCommandParser {
    pub fn new() -> Self {
        AppCommandParser {
            line: String::new(),
        }
    }
    pub fn parse_resp_array<R: BufRead>(mut self: Self, reader: &mut R) -> io::Result<Vec<String>> {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        self.line = line;

        if !self.line.starts_with('*') {
            return self.parse_simple();
        }
        let count: usize = self.line[1..]
            .trim()
            .parse()
            .map_err(|_| io::ErrorKind::InvalidData)?;

        let mut parts = Vec::with_capacity(count);

        for _ in 0..count {
            let mut line = String::new();
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

        println!("[parse_resp_array] Parsed parts: {:?}", parts);

        Ok(parts)
    }

    pub fn parse_simple(self: &Self) -> io::Result<Vec<String>> {
        let line = &self.line;
        if line.is_empty() {
            return Ok(vec![]);
        }

        let parts: Vec<String> = line
            .trim()
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();
        println!("[parse_simple] Parsed parts: {:?}", parts);
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
}
