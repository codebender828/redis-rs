use dashmap::DashMap;

pub struct Config {
    config: DashMap<String, String>,
}

pub struct ConfigOptions {
    pub dir: String,
    pub dbfilename: String,
}

impl Config {
    pub fn new() -> Self {
        Self {
            config: DashMap::new(),
        }
    }

    pub fn set(&self, key: String, value: String) {
        self.config.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.config.get(key).map(|v| v.value().clone())
    }

    pub fn get_all(&self) -> Vec<(String, String)> {
        self.config
            .iter()
            .map(|v| (v.key().clone(), v.value().clone()))
            .collect()
    }
}
