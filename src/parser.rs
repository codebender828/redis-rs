use std::str;

#[derive(Debug)]
pub enum Command {
    Ping(Option<String>),
    Echo(String),
    Set(String, String),
    Get(String),
    Unknown(String),
}

/** Parses Redis command */
pub fn parse_command(command_input: &[u8]) -> Result<Command, String> {
    let input =
        str::from_utf8(command_input).map_err(|e| format!("Invalid UTF-8 sequence: {}", e))?;

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
        "SET" => {
            if parts.len() < 7 {
                if parts.len() < 6 {
                    return Err("Invalid SET command format".to_string());
                } else {
                    return Err("Invalid SET command format: value not provided".to_string());
                }
            } else {
                Ok(Command::Set(parts[4].to_string(), parts[6].to_string()))
            }
        }
        "GET" => {
            if parts.len() < 6 {
                if parts.len() < 5 {
                    return Err("Invalid GET command format".to_string());
                } else {
                    return Err("Invalid GET command format: key not provided".to_string());
                }
            } else {
                Ok(Command::Get(parts[4].to_string()))
            }
        }
        _ => Ok(Command::Unknown(command)),
    }
}

/** Serializes response to match RESP format */
pub fn serialize_response(response: &str) -> String {
    format!("+{}\r\n", response)
}
