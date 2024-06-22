use std::env;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::str::FromStr;

use tokio::sync::Mutex;
use redis::Commands;
use redis::Client;

use reqwest;
use local_ip_address::local_ip;
use serde_json::json;
use serde::{Deserialize, Serialize};
use warp::{Filter, http::StatusCode};
use env_logger::Env;
use log::info;
use chrono;
use uuid;
use lazy_static::lazy_static;

#[derive(Serialize, Deserialize, Debug)]
enum ConsistencyModel {
    Eventual,
    Sequential
}

impl FromStr for ConsistencyModel {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "eventual" => Ok(ConsistencyModel::Eventual),
            "sequential" => Ok(ConsistencyModel::Sequential),
            _ => Ok(ConsistencyModel::Eventual),
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
struct Pair {
    key: String,
    value: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct ClientInfo {
    id: String,
    addr: String,
}

#[derive(Deserialize, Serialize)]
struct LogInfo {
    timestamp: String,
    sender: String,
    receiver: String,
    message_id: String,
    action: String
}

impl LogInfo {
    pub fn new(sender: String, receiver: String, action: String, message_id: String) -> LogInfo {
        LogInfo {
            timestamp: chrono::Utc::now().to_rfc3339(),
            sender,
            receiver,
            message_id,
            action,
        }
    }

    pub fn log(&self, log_server_url: &str) -> Result<(), reqwest::Error> {
        let client = reqwest::blocking::Client::new();
        let json = serde_json::to_string(self).expect("Failed to serialize log data");
    
        let _ = client.post(log_server_url)
            .header("Content-Type", "application/json")
            .body(json)
            .send()?;
    
        Ok(())
    }
}

lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

struct Replica {
    id: String,
    log_server_url: String,
    redis: Arc<Mutex<Client>>,
    neighbors: Arc<RwLock<Vec<ClientInfo>>>,
    primary_mutex: Arc<Mutex<()>>
}

impl Replica {
    pub fn new(id: String, log_server_url: String, redis_url: String) -> Replica {
        Replica {
            id,
            log_server_url,
            redis: Arc::new(Mutex::new(redis::Client::open(redis_url).expect("invalid Redis URL"))),
            neighbors: Arc::new(RwLock::new(Vec::new())),
            primary_mutex: Arc::new(Mutex::new(()))
        }
    }

    fn log_send(&self, message_id: String, action: String) {
        let log_record = LogInfo::new(
            format!("node-{}", self.id),
            String::from(""),
            action,
            message_id,
        );
        let _ = log_record.log(&self.log_server_url);
    }

    fn log_recv(&self, message_id: String, action: String) {
        let log_record = LogInfo::new(
            String::from(""),
            format!("node-{}", self.id),
            action,
            message_id,
        );
        let _ = log_record.log(&self.log_server_url);
    }

    fn hash_key(&self, key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn find_primary_replica(&self, key_hash: u64) -> ClientInfo {
        let neighbors = self.neighbors.read().unwrap();
        let index: usize = (key_hash % neighbors.len() as u64) as usize;
        neighbors[index].clone()
    }

    pub async fn non_blocking_read(&self, key: String) -> String {
        let client = self.redis.lock();
        let mut conn = client.await.get_connection().expect("Failed to connect to Redis");
        let value = conn.get(&key).expect("Failed to get key in Redis");    

        value
    }

    pub fn blocking_read(&self, key: String) -> String {
        // Hash the key
        let key_hash = self.hash_key(&key);

        // Find the primary replica
        let primary_replica = self.find_primary_replica(key_hash);

        let forward_id = uuid::Uuid::new_v4().to_string();

        self.log_send(forward_id.clone(), format!("Node {} forwarding read key: {} to Primary Node {}", self.id, key, primary_replica.id));

        let response = reqwest::blocking::Client::new()
            .get(format!("http://{}:6969/primary/{}", primary_replica.addr, key))
            .header("Forward-Id", forward_id.clone())
            .send();

        match response {
            Ok(res) => {
                let response_id = res
                    .headers()
                    .get("Message-Id")
                    .and_then(|header_value| header_value.to_str().ok())
                    .unwrap_or_default()
                    .to_string();

                self.log_recv(response_id.clone(), format!("Node {} received Primary Read Forward OK", self.id));

                // Extract the value from the response body
                match res.text() {
                    Ok(value) => {
                        // Log the received value
                        info!("Node {} received value: {} for key: {}", self.id, value, key);
                        value
                    },
                    Err(e) => {
                        eprintln!("Failed to read response body: {}", e);
                        String::from("Error reading response")
                    }
                }
            },
            Err(e) => {
                eprintln!("HTTP error occurred: {}", e);
                String::from("Error sending request")
            }
        }
    }

    pub async fn primary_read(&self, key: String) -> String {
        let _guard = self.primary_mutex.lock().await;
        let client = self.redis.lock();
        let mut conn = client.await.get_connection().expect("Failed to connect to Redis");
        let value = conn.get(&key).expect("Failed to get key in Redis");    

        value
    }

    pub async fn non_blocking_write(&self, update: Pair) {
        // Scope the lock to ensure it's released after use
        {
            let client = self.redis.lock();
            let mut conn = client.await.get_connection().expect("Failed to connect to Redis");
            let _: () = conn.set(&update.key, &update.value).expect("Failed to set key in Redis");
        } // The lock is automatically released here when `client` goes out of scope

        // Clone the necessary data instead of cloning the entire self
        let replica = self.clone();

        tokio::spawn(async move {
            replica.broadcast(update).await;
        });
    }

    pub async fn update(&self, update: Pair) {
        let client = self.redis.lock();
        let mut conn = client.await.get_connection().expect("Failed to connect to Redis");
        let _: () = conn.set(&update.key, &update.value).expect("Failed to set key in Redis");
    }

    pub async fn blocking_write(&self, update: Pair) {
        // Hash the key
        let key_hash = self.hash_key(&update.key);

        // Find the primary replica
        let primary_replica = self.find_primary_replica(key_hash);

        let forward_id = uuid::Uuid::new_v4().to_string();

        self.log_send(forward_id.clone(), format!("Node {} forwarding write key: {} = {} to Primary Node {}", self.id, update.key, update.value, primary_replica.id));

        let response = reqwest::Client::new()
            .post(format!("http://{}:6969/primary", primary_replica.addr))
            .header("Forward-Id", forward_id.clone())
            .json(&json!({
                "key": update.key,
                "value": update.value
            }))
            .send()
            .await;

        match response {
            Ok(res) => {
                let response_id = res
                    .headers()
                    .get("Message-Id")
                    .and_then(|header_value| header_value.to_str().ok())
                    .unwrap_or_default()
                    .to_string();

                self.log_recv(response_id.clone(), format!("Node {} received Primary Write Forward OK", self.id));

            }
            Err(e) => {
                eprintln!("Failed to send request: {}", e);
            }
        };
    }

    pub async fn primary_write(&self, update: Pair) {        
        let _guard = self.primary_mutex.lock().await;
        // Scope the lock to ensure it's released after use
        {
            let client = self.redis.lock();
            let mut conn = client.await.get_connection().expect("Failed to connect to Redis");
            let _: () = conn.set(&update.key, &update.value).expect("Failed to set key in Redis");    
        } // The lock is automatically released here when `client` goes out of scope

        self.broadcast(update).await;
    }

    pub async fn broadcast(&self, update: Pair) {
        let tasks = self.neighbors.read().unwrap().clone()
            .into_iter()
            .filter(|client_info| {
                let client_id_str = client_info.id.to_string();
                let is_same = client_id_str == self.id;
                !is_same
            })
            .map(|client_info| {

                let update = update.clone();
                let client_addr = client_info.addr.clone();

                let replica: Replica = self.clone();


                tokio::spawn(async move {
                    let broadcast_id = uuid::Uuid::new_v4().to_string();

                    replica.log_send(broadcast_id.clone(), format!("Node {} broadcasting set key: {} = {}", replica.id, update.key, update.value));

                    let response = reqwest::Client::new()
                        .post(format!("http://{}:6969/update", client_addr))
                        .header("Broadcast-Id", broadcast_id.clone())
                        .json(&json!({
                            "key": update.key,
                            "value": update.value
                        }))
                        .send()
                        .await;

                    match response {
                        Ok(res) => {
                            let response_id = res
                                .headers()
                                .get("Message-Id")
                                .and_then(|header_value| header_value.to_str().ok())
                                .unwrap_or_default()
                                .to_string();

                            replica.log_recv(response_id.clone(), format!("Node {} received broadcast OK", replica.id));
                        }
                        Err(e) => {
                            eprintln!("Failed to send request: {}", e);
                        }
                    }
                })
            });

        futures::future::join_all(tasks).await;
    }

    pub fn update_neighbors(&self, new_neighbors: Vec<ClientInfo>) {
        let neighbors_locked = self.neighbors.clone();
        let mut neighbors_locked = neighbors_locked.write().unwrap();
        *neighbors_locked = new_neighbors.into_iter().collect();
    }
}

// Implement Clone for Replica
impl Clone for Replica {
    fn clone(&self) -> Self {
        Replica {
            id: self.id.clone(),
            log_server_url: self.log_server_url.clone(),
            redis: Arc::clone(&self.redis),
            neighbors: Arc::clone(&self.neighbors),
            primary_mutex: Arc::clone(&self.primary_mutex)
        }
    }
}


#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let master_url: String = env::var("MASTER_URL").expect("MASTER_URL must be set");
    let log_server_url = env::var("LOG_SERVER_URL").expect("LOG_SERVER_URL must be set");
    let delay: u64 = env::var("DELAY").expect("DELAY must be set").parse().unwrap();

    let local_ip = local_ip().unwrap();
    info!("This is my local IP address: {:?}", local_ip);

    let body = json!(local_ip.to_string());

    let node_id = reqwest::blocking::Client::new()
        .post(format!("{}/register", master_url))
        .json(&body)
        .send()
        .expect("Failed to register node")
        .text()
        .expect("Failed to get node ID");

    let node_id: String = node_id.clone().to_string().replace("\"", "");

    info!("Node registered with ID: {}", node_id);

    let redis_url = env::var("REDIS_URL").expect("REDIS_URL must be set");

    let replica = Replica::new(node_id, log_server_url, redis_url);

    let replica_filter = warp::any().map(move || replica.clone());

    let set = warp::post()
        .and(warp::path("set"))
        .and(warp::header("Consistency-Model"))
        .and(warp::header("Client-Id"))
        .and(warp::header("Message-Id"))
        .and(warp::body::json())
        .and(replica_filter.clone())
        .and_then(move |consistency_model: ConsistencyModel, client_id: String, message_id: String, payload: Pair, replica: Replica| async move {
            replica.log_recv(message_id, format!("Node {} received Client {} set key {} = {}", replica.id, client_id, payload.key, payload.value));

            match consistency_model {
                ConsistencyModel::Eventual => {
                    // For eventual consistency, respond immediately and update in the background
                    replica.non_blocking_write(payload).await;
                    info!("Eventual consistency: local write.");

                    // Respond immediately
                },
                ConsistencyModel::Sequential => {
                    // For sequential consistency, send write(key, value) to the key's primary.
                    // primary will then broadcast write(key, value) to each of the replicas (including this one - in a different thread)
                    // this thread needs to wait from an acknowledgement from the primary stating that all the replicas are updated
                    // only then can this thread return to the client with an OK

                    replica.blocking_write(payload).await;

                    info!("Sequential consistency: Updated primary.");
                }
            }

            let response_id: String = uuid::Uuid::new_v4().to_string();

            replica.log_send(response_id.clone(), String::from("Node responding OK"));

            let response = warp::reply::with_status("Key set", StatusCode::OK);
            let custom_header = warp::reply::with_header(response, "Message-Id", response_id);
            Ok::<_, warp::Rejection>(custom_header)
        });

    let get = warp::get()
        .and(warp::path("get"))
        .and(warp::header("Consistency-Model"))
        .and(warp::header("Client-Id"))
        .and(warp::header("Message-Id"))
        .and(warp::path::param())
        .and(replica_filter.clone()) // Clone the client filter for use in this route
        .and_then(move |consistency_model: ConsistencyModel, client_id: String, message_id: String, key: String, replica: Replica| async move {
            replica.log_recv(message_id, format!("Node {} receiving client {} aks for key {}", replica.id, client_id, key));

            let value = match consistency_model {
                ConsistencyModel::Eventual => {
                    // For eventual consistency, respond immediately and update in the background
                    let value = replica.non_blocking_read(key.clone());
                    info!("Eventual consistency: Local read");

                    // Respond immediately
                    value
                }
                ConsistencyModel::Sequential => {
                    // For sequential consistency, send write(key, value) to the key's primary.
                    // primary will then broadcast write(key, value) to each of the replicas (including this one - in a different thread)
                    // this thread needs to wait from an acknowledgement from the primary stating that all the replicas are updated
                    // only then can this thread return to the client with an OK

                    let value = replica.non_blocking_read(key.clone());

                    info!("Sequential consistency: Local read.");
                    value
                }
            }.await;

            let response_id: String = uuid::Uuid::new_v4().to_string();

            replica.log_send(response_id.clone(), format!("Node {} responding with key {} = {}", replica.id, key, value));

            let response = warp::reply::with_status(warp::reply::json::<String>(&value), StatusCode::OK);
            let custom_header = warp::reply::with_header(response, "Message-Id", response_id);
            Ok::<_, warp::Rejection>(custom_header)
        });

    let update = warp::post()
        .and(warp::path("update"))
        .and(warp::header("Broadcast-Id"))
        .and(warp::body::json())
        .and(replica_filter.clone())
        .and_then(move |broadcast_id: String, payload: Pair, replica: Replica| async move {
            replica.log_recv(broadcast_id, format!("Node {} received broadcast to set key: {} = {}", replica.id, payload.key, payload.value));

            replica.update(payload.clone()).await;

            info!("Node {}: update key {} = {}", replica.id, payload.key, payload.value);

            let response_id: String = uuid::Uuid::new_v4().to_string();
            replica.log_send(response_id.clone(), format!("Node {} responding OK", replica.id));

            let response = warp::reply::with_status("Key set", StatusCode::OK);
            let custom_header = warp::reply::with_header(response, "Message-Id", response_id);
            Ok::<_, warp::Rejection>(custom_header)
        });

    let primary_get = warp::get()
        .and(warp::path("primary"))
        .and(warp::header("Forward-Id"))
        .and(warp::path::param())
        .and(replica_filter.clone()) // Clone the client filter for use in this route
        .and_then(move |message_id: String, key: String, replica: Replica| async move {
            replica.log_recv(message_id, format!("Primary {} receiving get forward for key {}", replica.id, key));

            let value = replica.primary_read(key.clone()).await;

            let response_id: String = uuid::Uuid::new_v4().to_string();

            replica.log_send(response_id.clone(), format!("Primary {} responding with key {} = {}", replica.id, key, value));

            let response = warp::reply::with_status(warp::reply::json::<String>(&value), StatusCode::OK);
            let custom_header = warp::reply::with_header(response, "Message-Id", response_id);
            Ok::<_, warp::Rejection>(custom_header)
        });

    let primary_set = warp::post()
        .and(warp::path("primary"))
        .and(warp::header("Forward-Id"))
        .and(warp::body::json())
        .and(replica_filter.clone())
        .and_then(move |message_id: String, payload: Pair, replica: Replica| async move {
            replica.log_recv(message_id, format!("Primary {} receiving set forward for key {} = {}", replica.id, payload.key, payload.value));

            replica.primary_write(payload).await;

            let response_id: String = uuid::Uuid::new_v4().to_string();

            replica.log_send(response_id.clone(), String::from("Primary responding OK"));

            let response = warp::reply::with_status("Key set", StatusCode::OK);
            let custom_header = warp::reply::with_header(response, "Message-Id", response_id);
            Ok::<_, warp::Rejection>(custom_header)
        });

    let health_check = warp::post()
        .and(warp::path("health-check"))
        .and(warp::body::json())
        .and(replica_filter.clone())
        .map(move |new_neighbors: Vec<ClientInfo>, replica: Replica| {
            replica.update_neighbors(new_neighbors.clone());
            info!("Cluster addresses updated: {:?}", new_neighbors);
            warp::reply::with_status("Database updated", StatusCode::OK)
        });

    let swarm_health_check = warp::get()
        .and(warp::path("swarm-check"))
        .map(|| {
            warp::reply::with_status("", StatusCode::OK)
        });

    let routes = health_check
        .or(swarm_health_check)
        .or(primary_set)
        .or(primary_get)
        .or(update)
        .or(get)
        .or(set);
    warp::serve(routes).run(([0, 0, 0, 0], 6969)).await;
}