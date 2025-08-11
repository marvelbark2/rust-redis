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
            AppCommand::Set(key, value) => format!("OK: {} set to {}", key, value),
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
