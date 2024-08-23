use std::str;

#[derive(Debug)]
pub enum Command {
    Ping(Option<String>),
    Echo(String),
    Unknown(String),
}

/** Parses Redis command */
pub fn parse_command(input: &[u8]) -> Result<Command, String> {
    let input = str::from_utf8(input).map_err(|e| format!("Invalid UTF-8 sequence: {}", e))?;
    let parts: Vec<&str> = input.split("\r\n").collect();

    if parts.len() < 4 || !parts[0].starts_with("*") {
        return Err("Invalid RESP format".to_string());
    }

    let command = parts[2].to_uppercase();
    match command.as_str() {
        "ECHO" => {
            if parts.len() < 6 {
                return Err("Invalid ECHO command format".to_string());
            } else {
                Ok(Command::Echo(parts[4].to_string()))
            }
        }
        "PING" => {
            if parts.len() < 4 {
                return Err("Invalid PING command format".to_string());
            } else if parts.len() >= 6 {
                Ok(Command::Ping(Some(parts[4].to_string())))
            } else {
                Ok(Command::Ping(None))
            }
        }
        _ => Ok(Command::Unknown(command)),
    }
}

/** Serializes response to match RESP format */
pub fn serialize_response(response: &str) -> String {
    format!("+{}\r\n", response)
}
