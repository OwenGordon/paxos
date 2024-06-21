use redis::Commands;
use serde_json::json;
use std::env;
use local_ip_address::local_ip;

use serde::{Deserialize, Serialize};
use warp::{Filter, http::StatusCode};
use env_logger::Env;

use log::info;
use std::sync::{Arc, RwLock};
use std::{thread, time};
use futures::Future;

use reqwest;
use reqwest::{Response, Error};

use std::str::FromStr;
use chrono;
use uuid;

#[derive(Serialize, Deserialize, Debug)]
enum ConsistencyModel {
    Strong,
    Eventual,
    Causal,
}

impl FromStr for ConsistencyModel {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "strong" => Ok(ConsistencyModel::Strong),
            "eventual" => Ok(ConsistencyModel::Eventual),
            "causal" => Ok(ConsistencyModel::Causal),
            _ => Ok(ConsistencyModel::Eventual),
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
struct Pair {
    key: String,
    value: String,
}

#[derive(Deserialize, Serialize, Clone)]
struct Broadcast {
    node: String,
    key: String,
    value: String
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
}

fn total_broadcast(node_id_clone: String, payload: Pair, client: redis::Client, cluster_addrs: Arc<RwLock<Vec<ClientInfo>>>, log_server_url: String) -> Vec<impl Future<Output = Result<Response, Error>> + 'static> {
    // broadcast
    let tasks: Vec<_> = <Vec<ClientInfo> as Clone>::clone(&cluster_addrs.read().unwrap())
    .into_iter()
    .filter(|client_info| {
        let client_id_str = client_info.id.to_string();
        let is_same = client_id_str == node_id_clone;
        !is_same
    })
    .map(|client_info| {
        let params_clone = payload.clone();
        let node_id_clone =  node_id_clone.clone();
        let client_clone = client.clone();
        let log_server_clone = log_server_url.clone();
        async move {
            let mut con = client_clone.get_connection().expect("Failed to connect to redis");
            let value: String = con.get(&params_clone.key).unwrap();
            info!("Node {} Sending redis update to {:?}", node_id_clone, client_info);

            let broadcast_id = uuid::Uuid::new_v4().to_string();

            // Create a log record for the update operation
            let log_record = LogInfo::new(
                format!("node-{}", node_id_clone),
                "".to_string(),
                format!("Node {} broadcasting set key: {} = {}", node_id_clone, params_clone.key, value),
                broadcast_id.clone()
            );

            let _ = push_log(log_record, &log_server_clone);

            let response = reqwest::Client::new()
                .post(format!("http://{}:6969/update", client_info.addr))
                .header("Broadcast-Id", broadcast_id.clone())
                .json(&json!({
                    "node": node_id_clone,
                    "key": params_clone.key,
                    "value": value
                }))
                .send()
                .await;

            // Handle the Result to get the Response object
            let response = match response {
                Ok(res) => res,
                Err(e) => {
                    // Handle error appropriately, e.g., log or return an error
                    eprintln!("Failed to send request: {}", e);
                    return Err(e);
                }
            };

            // Extracting the "Response-Id" from the response headers
            let response_id = response
                .headers()
                .get("Message-Id")
                .and_then(|header_value| header_value.to_str().ok())
                .unwrap_or_default()
                .to_string();

            // Create a log record for the update operation
            let log_record = LogInfo::new(
                "".to_string(),
                format!("node-{}", node_id_clone),
                format!("Node {} received broadcast OK", node_id_clone),
                response_id.clone()
            );

            let _ = push_log(log_record, &log_server_clone);

            return Ok(response);
        }
    })
    .collect();

    return tasks;
}


fn push_log(log_record: LogInfo, log_server_url: &String) -> Result<(), reqwest::Error> {
    let client = reqwest::blocking::Client::new();
    let json = serde_json::to_string(&log_record).expect("Failed to serialize log data");

    let _ = client.post(log_server_url)
        .header("Content-Type", "application/json")
        .body(json)
        .send()?;

    Ok(())
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let master_url: String = env::var("MASTER_URL").expect("MASTER_URL must be set");

    let log_server = env::var("LOG_SERVER_URL").expect("LOG_SERVER_URL must be set");

    let delay: u64 = env::var("DELAY").expect("DELAY must be set").parse().unwrap();

    let local_ip = local_ip().unwrap();

    info!("This is my local IP address: {:?}", local_ip);

    let body = json!(local_ip.to_string());

    let node_id = reqwest::Client::new()
        .post(format!("{}/register", master_url))
        .json(&body)
        .send()
        .await
        .expect("Failed to register node")
        .text()
        .await
        .expect("Failed to get node ID");

    let node_id: String = node_id.clone().to_string().replace("\"", "");

    info!("Node registered with ID: {}", node_id);

    let redis_url = env::var("REDIS_URL").expect("REDIS_URL must be set");
    let client = redis::Client::open(redis_url).expect("invalid Redis URL");

    let cluster_addrs: Arc<RwLock<Vec<ClientInfo>>> = Arc::new(RwLock::new(Vec::new()));

    // Use warp::any to create a filter that clones the client for each request
    let client_filter = warp::any().map(move || client.clone());

    let cluster_addrs_clone = cluster_addrs.clone();

    let node_id_clone = node_id.clone();

    let log_server_clone = log_server.clone();

    let set = warp::post()
        .and(warp::path("set"))
        .and(warp::header("Consistency-Model"))
        .and(warp::header("Client-Id"))
        .and(warp::header("Message-Id"))
        .and(warp::body::json())
        .and(client_filter.clone())
        .and_then(move |consistency_model: ConsistencyModel, client_id: String, message_id: String, payload: Pair, client: redis::Client| {
            let cluster_addrs = cluster_addrs_clone.clone();
            let node_id_clone = node_id_clone.clone();
            let client_id_clone = client_id.clone();
            let message_id_clone = message_id.clone();
            let log_server_clone = log_server_clone.clone();
            async move {
                let mut con = client.get_connection().expect("Failed to connect to Redis");

                let _: () = con.set(&payload.key, &payload.value).expect("Failed to set key in Redis");

                info!("Node {}: set key {} = {}", node_id_clone, payload.key, payload.value);

                // Create a log record for the update operation
                let log_record = LogInfo::new(
                    "".to_string(),
                    format!("node-{}", node_id_clone),
                    format!("Node {} received client {} set key: {} = {}", node_id_clone, client_id_clone, payload.key, payload.value),
                    message_id_clone
                );

                let _ = push_log(log_record, &log_server_clone);

                let log_server_clone_clone = log_server_clone.clone();

                match consistency_model {
                    ConsistencyModel::Strong => {
                        // For strong consistency, ensure all nodes are updated before responding
                        let tasks = total_broadcast(node_id_clone.clone(), payload.clone(), client.clone(), cluster_addrs.clone(), log_server_clone_clone);
                        let duration = time::Duration::from_millis(delay);

                        thread::sleep(duration);
                        futures::future::join_all(tasks).await;

                        info!("Strong consistency: All nodes updated.");
                    },
                    ConsistencyModel::Eventual => {
                        // For eventual consistency, respond immediately and update in the background
                        let tasks = total_broadcast(node_id_clone.clone(), payload.clone(), client.clone(), cluster_addrs.clone(), log_server_clone_clone);

                        tokio::spawn(async move {
                            let duration = time::Duration::from_millis(delay);
                            thread::sleep(duration);

                            futures::future::join_all(tasks).await;
                            info!("Eventual consistency: Nodes update initiated.");
                        });

                        // Respond immediately
                    },
                    ConsistencyModel::Causal => {
                        // For causal consistency, updates depend on the causality chain, which might need custom logic
                        // Here, we just simulate a simple broadcast for demonstration
                        let tasks = total_broadcast(node_id_clone.clone(), payload.clone(), client.clone(), cluster_addrs.clone(), log_server_clone_clone);
                        futures::future::join_all(tasks).await;
                        info!("Causal consistency: Causally ordered updates completed.");
                    },
                }

                let response_id: String = uuid::Uuid::new_v4().to_string();

                let log_record = LogInfo::new(
                    format!("node-{}", node_id_clone),
                    "".to_string(),
                    "Node responding OK".to_string(),
                    response_id.clone()
                );

                let _ = push_log(log_record, &log_server_clone);

                let response = warp::reply::with_status("Key set", StatusCode::OK);
                let custom_header = warp::reply::with_header(response, "Message-Id", response_id);
                Ok::<_, warp::Rejection>(custom_header)
            }
        });

    let node_id_clone = node_id.clone();
    let log_server_clone = log_server.clone();

    let update = warp::post()
        .and(warp::path("update"))
        .and(warp::header("Broadcast-Id"))
        .and(warp::body::json())
        .and(client_filter.clone())
        .map(move |broadcast_id: String, payload: Broadcast, client: redis::Client| {
            let mut con = client.get_connection().expect("Failed to connect to Redis");

            // Create a log record for the update operation
            let log_record = LogInfo::new(
                "".to_string(),
                format!("node-{}", node_id_clone),
                format!("Node {} received broadcast to set key: {} = {}", node_id_clone, payload.key, payload.value),
                broadcast_id.clone()
            );

            let _ = push_log(log_record, &log_server_clone.clone());

            let _: () = con.set(&payload.key, &payload.value).expect("Failed to set key in Redis");

            info!("Node {}: update key {} = {}", node_id_clone.clone(), payload.key, payload.value);

            let response_id: String = uuid::Uuid::new_v4().to_string();

            // Create a log record for the update operation
            let log_record = LogInfo::new(
                format!("node-{}", node_id_clone),
                "".to_string(),
                format!("Node {} responding OK", node_id_clone),
                response_id.clone()
            );

            let _ = push_log(log_record, &log_server_clone.clone());

            let response = warp::reply::with_status("Key set", StatusCode::OK);
            let custom_header = warp::reply::with_header(response, "Message-Id", response_id);
            custom_header
        });

    let node_id_clone = node_id.clone();
    let log_server_clone = log_server.clone();

    let get = warp::get()
        .and(warp::path("get"))
        .and(warp::header("Client-Id"))
        .and(warp::path::param())
        .and(warp::header("Message-Id"))
        .and(client_filter.clone()) // Clone the client filter for use in this route
        .map(move |client_id: String, key: String, message_id: String, client: redis::Client| {
             // Create a log record for the update operation
             let log_record = LogInfo::new(
                "".to_string(),
                format!("node-{}", node_id_clone),
                format!("Node {} receiving client {} aks for key {}", node_id_clone, client_id, key),
                message_id.clone()
            );

            let _ = push_log(log_record, &log_server_clone.clone());

            let mut con = client.get_connection().expect("failed to connect to Redis");
            match con.get(&key) {
                Ok(value) => {
                    let response_id: String = uuid::Uuid::new_v4().to_string();

                    // Create a log record for the update operation
                    let log_record = LogInfo::new(
                        format!("node-{}", node_id_clone),
                        "".to_string(),
                        format!("Node {} responding with key {} = {}", node_id_clone, key, value),
                        response_id.clone()
                    );

                    let _ = push_log(log_record, &log_server_clone.clone());

                    let response = warp::reply::with_status(warp::reply::json::<String>(&value), StatusCode::OK);
                    let custom_header = warp::reply::with_header(response, "Message-Id", response_id);
                    custom_header
                }
                Err(_) => {
                    let response_id: String = uuid::Uuid::new_v4().to_string();

                    let response = warp::reply::with_status(warp::reply::json::<String>(&"key not found".to_string()), StatusCode::NOT_FOUND);
                    let custom_header = warp::reply::with_header(response, "Message-Id", response_id);
                    custom_header
                }
            }
        });

    let ack = warp::get()
        .and(warp::path("ack"))
        .and(warp::path::param())
        .map(|_key: String| {
            warp::reply::with_status(warp::reply::json(&"received"), StatusCode::OK)
        });

    let update_cluseter_addrs = cluster_addrs.clone();
    let health_check = warp::post()
        .and(warp::path("health-check"))
        .and(warp::body::json())
        .map(move |new_cluster_addrs: Vec<ClientInfo>| {
            let cluster_addrs_locked = update_cluseter_addrs.clone();
            let mut cluster_addrs_locked = cluster_addrs_locked.write().unwrap();
            *cluster_addrs_locked = new_cluster_addrs.into_iter().collect();
            info!("Cluster addresses updated: {:?}", cluster_addrs_locked);
            warp::reply::with_status("Database updated", StatusCode::OK)
        });

    let swarm_health_check = warp::get()
        .and(warp::path("swarm-check"))
        .map(|| {
            warp::reply::with_status("", StatusCode::OK)
        });

    let routes = set
        .or(get)
        .or(ack)
        .or(health_check)
        .or(swarm_health_check)
        .or(update);
    warp::serve(routes).run(([0, 0, 0, 0], 6969)).await;
}