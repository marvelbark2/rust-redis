use std::io::{self, BufRead};

pub enum AppCommand {
    Ping,
    Echo(String),
    Set(String, String),
    Get(String),
    Del(String),
    Keys(String),
    Exists(String),
}

impl AppCommand {
    pub fn compute(&self) -> String {
        match self {
            AppCommand::Ping => "PONG".to_string(),
            AppCommand::Echo(msg) => msg.clone(),
            AppCommand::Set(_, _) => "OK".to_string(),
            AppCommand::Get(key) => format!("Value for {} is ...", key),
            AppCommand::Del(key) => format!("Deleted {}", key),
            AppCommand::Keys(pattern) => format!("Keys matching {} are ...", pattern),
            AppCommand::Exists(key) => format!("{} exists", key),
        }
    }

    pub fn from_parts_simple(parts: Vec<String>) -> Option<Self> {
        if parts.is_empty() {
            return None;
        }

        match parts[0].to_uppercase().as_str() {
            "PING" => Some(AppCommand::Ping),
            "ECHO" if parts.len() > 1 => Some(AppCommand::Echo(parts[1].clone())),
            "SET" if parts.len() > 2 => Some(AppCommand::Set(parts[1].clone(), parts[2].clone())),
            "GET" if parts.len() > 1 => Some(AppCommand::Get(parts[1].clone())),
            "DEL" if parts.len() > 1 => Some(AppCommand::Del(parts[1].clone())),
            "KEYS" if parts.len() > 1 => Some(AppCommand::Keys(parts[1].clone())),
            "EXISTS" if parts.len() > 1 => Some(AppCommand::Exists(parts[1].clone())),
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
