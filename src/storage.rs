use dashmap::DashMap;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug)]
pub struct StorageValue {
    created_at: Instant,
    value: String,
    expires_at: Option<Instant>,
}

impl StorageValue {
    pub fn new(value: String) -> Self {
        Self {
            created_at: Instant::now(),
            value,
            expires_at: None,
        }
    }
}

pub struct Storage {
    storage: DashMap<String, StorageValue>,
}

impl Storage {
    // Creates a new instance of the Storage struct
    pub fn new() -> Self {
        Self {
            storage: DashMap::new(),
        }
    }

    /** Creates a new entry to storage */
    pub fn set(&self, key: String, value: String, options: Vec<(String, String)>) {
        let mut value = StorageValue {
            value,
            created_at: Instant::now(),
            expires_at: None,
        };

        println!("Filtered Options: {:?}", options);

        for (argument, argument_value) in options {
            match argument.as_str() {
                "EX" => {
                    let duration = match argument_value.parse::<u64>() {
                        Ok(d) => d,
                        Err(e) => {
                            eprintln!("Failed to parse duration: {}", e);
                            continue;
                        }
                    };

                    value.expires_at = Some(value.created_at + Duration::from_secs(duration));
                }
                "PX" => {
                    let duration = match argument_value.parse::<u64>() {
                        Ok(d) => d,
                        Err(e) => {
                            eprintln!("Failed to parse duration: {}", e);
                            continue;
                        }
                    };

                    value.expires_at = Some(value.created_at + Duration::from_millis(duration));
                }
                _ => {
                    eprintln!("Unknown option: {}", argument);
                }
            }
        }

        self.storage.insert(key, value);
    }

    pub fn remove(&self, key: &str) {
        self.storage.remove(key);
    }

    /** Retrieves a value from storage */
    pub fn get(&self, key: &str) -> Option<String> {
        self.storage.get(key).and_then(|result| {
            let now = Instant::now();
            if let Some(expires_at) = result.expires_at {
                if expires_at < now {
                    drop(result);
                    self.remove(key);
                    None
                } else {
                    Some(result.value.clone())
                }
            } else {
                Some(result.value.clone())
            }
        })
    }
}
