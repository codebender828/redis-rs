use std::str;

#[derive(Debug)]
pub enum Command {
  PING(Option<String>),
  ECHO(String),
  SET(String, String, Option<Vec<(String, String)>>),
  GET(String),
  CONFIGGET(String),
  UNKNOWN(String),
}

pub enum RedisValue {
  SimpleString(String),
  BulkString(Option<String>),
  Array(Vec<String>),
  Error(String),
}

/** Parses Redis command */
pub fn parse_command(command_input: &[u8]) -> Result<Command, String> {
  let input =
    str::from_utf8(command_input).map_err(|e| format!("Invalid UTF-8 sequence: {}", e))?;

  let parts: Vec<&str> = input.split("\r\n").collect();

  if parts.len() < 4 || !parts[0].starts_with("*") {
    return Err("Invalid RESP format".to_string());
  }

  let mut command = parts[2].to_uppercase();

  // Check if the command is CONFIG
  if command.starts_with("CONFIG") {
    command = format!("{} {}", command, parts[4].to_uppercase());
  }

  match command.as_str() {
    "ECHO" => {
      if parts.len() < 6 {
        return Err("Invalid ECHO command format".to_string());
      } else {
        Ok(Command::ECHO(parts[4].to_string()))
      }
    }
    "PING" => {
      if parts.len() < 4 {
        return Err("Invalid PING command format".to_string());
      } else if parts.len() >= 6 {
        Ok(Command::PING(Some(parts[4].to_string())))
      } else {
        Ok(Command::PING(None))
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
        // Check if the optional arguments are provided
        if parts.len() == 8 {
          Ok(Command::SET(
            parts[4].to_string(),
            parts[6].to_string(),
            None,
          ))
        } else if parts.len() > 8 {
          let mut optional_args: Vec<String> = Vec::with_capacity(parts.len() - 8);
          for i in 8..parts.len() {
            optional_args.push(parts[i].to_string());
          }

          let options: Vec<String> = optional_args
            .iter()
            .filter(|s| !s.starts_with("$"))
            .map(|f| f.clone())
            .collect();

          let processed_optional_arguments = group_redis_optional_arguments(options);

          Ok(Command::SET(
            parts[4].to_string(),
            parts[6].to_string(),
            Some(processed_optional_arguments),
          ))
        } else {
          return Err("Invalid SET command format: Unknown optional parameters".to_string());
        }
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
        Ok(Command::GET(parts[4].to_string()))
      }
    }
    "CONFIG GET" => {
      if parts.len() < 5 {
        return Err("Invalid CONFIG GET command format".to_string());
      } else {
        Ok(Command::CONFIGGET(parts[6].to_string()))
      }
    }
    _ => Ok(Command::UNKNOWN(command)),
  }
}

/** Serializes response to match RESP format */
pub fn serialize_response(value: RedisValue) -> String {
  match value {
    RedisValue::SimpleString(s) => format!("+{}\r\n", s),
    RedisValue::BulkString(Some(s)) => format!("${}\r\n{}\r\n", s.len(), s),
    RedisValue::BulkString(None) => "$-1\r\n".to_string(),
    RedisValue::Error(s) => format!("-{}\r\n", s),
    RedisValue::Array(values) => {
      let mut response = format!("*{}\r\n", values.len());
      for value in values {
        response.push_str(&serialize_response(RedisValue::BulkString(Some(value))));
      }
      response
    }
  }
}

/** Groups all optional arguments */
pub fn group_redis_optional_arguments(options: Vec<String>) -> Vec<(String, String)> {
  options
    .into_iter()
    .filter(|s| !s.is_empty())
    .collect::<Vec<String>>()
    .chunks(2)
    .filter_map(|chunk| {
      if chunk.len() == 2 {
        Some((chunk[0].to_uppercase(), chunk[1].to_string()))
      } else {
        None
      }
    })
    .collect()
}
