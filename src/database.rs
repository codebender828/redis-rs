use crate::{config::Config, storage::Storage};
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn populate_hot_storage(storage: &Arc<Mutex<Storage>>, config: &Arc<Mutex<Config>>) {
  // Extract the directory and dbfilename from the configuration
  // and populate the storage with the data

  let storage = storage.lock().await;
  let config = config.lock().await;

  let directory = config.get("dir").unwrap();
  let dbfilename = config.get("dbfilename").unwrap();
  let file_path = format!("{}/{}", directory, dbfilename);

  // Read the file and populate the storage
  let contents = std::fs::read_to_string(file_path).unwrap();
  println!("Contents: {}", contents);
}
