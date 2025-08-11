use std::{
    collections::HashMap,
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
}

pub trait Engine {
    fn get(&self, key: &str) -> Option<&String>;
    fn set(&mut self, key: String, value: String);
    fn del(&mut self, key: &str) -> bool;
}
#[derive(Debug, Clone)]
pub struct HashMapEngine {
    pub hash_map: HashMap<String, String>,
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
            AppCommand::Ping => "PONG".to_string(),
            AppCommand::Echo(msg) => msg.clone(),
            AppCommand::Set(key, value, ttl) => {
                let mut engine = writter.write().unwrap();
                engine.set(key.to_string(), value.to_string());
                if *ttl > 0 {
                    let duration = generate_duration(*ttl);
                    let expiration_key = format!("{}_expiration", key);
                    engine.set(expiration_key, duration);
                }
                "OK".to_string()
            }
            AppCommand::Get(key) => {
                let mut engine = writter.write().unwrap();
                if let Some(v) = engine.get(key) {
                    let expiration_key = format!("{}_expiration", key);
                    if let Some(expiration) = engine.get(&expiration_key) {
                        if is_expired(expiration.clone()) {
                            engine.del(key);
                            engine.del(&expiration_key);

                            return String::from("-1");
                        }
                    }
                    v.clone()
                } else {
                    String::from("-1")
                }
            }
            AppCommand::Del(key) => format!("Deleted {}", key),
            AppCommand::Keys(pattern) => format!("Keys matching {} are ...", pattern),
            AppCommand::Exists(key) => format!("{} exists", key),
            AppCommand::RPush(key, value) => {
                let mut engine = writter.write().unwrap();
                let list_key = format!("{}_list", key);

                if let Some(existing) = engine.get(&list_key) {
                    let mut new_value = existing.clone();
                    new_value.push_str(&format!("\r{}", value));
                    let count = new_value.split('\r').count();
                    engine.set(list_key, new_value);

                    return format!(":{}", count);
                } else {
                    engine.set(list_key, value.clone());
                    let count = value.split('\r').count();
                    return format!(":{}", count);
                }
            }
            AppCommand::LRANGE(key, start_index, end_index) => {
                let engine = writter.write().unwrap();
                let list_key = format!("{}_list", key);

                if let Some(existing) = engine.get(&list_key) {
                    let items: Vec<&str> = existing.split('\r').collect();
                    let start = *start_index as usize;
                    let end = if *end_index < 0 {
                        items.len()
                    } else {
                        (*end_index + 1) as usize
                    };

                    if start < items.len() && end <= items.len() {
                        // Return the requested range as a RESP array
                        let range_items: Vec<String> =
                            items[start..end].iter().map(|s| s.to_string()).collect();
                        let resp_array = format!("*{}\r\n", range_items.len())
                            + &range_items
                                .iter()
                                .map(|s| format!("${}\r\n{}\r\n", s.len(), s))
                                .collect::<String>();
                        return resp_array;
                    }
                }
                String::from("*0")
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
