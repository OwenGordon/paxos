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
    source: String,
    destination: String,
    action: String
}

impl LogInfo {
    pub fn new(source: String, destination: String, action: String) -> LogInfo {
        LogInfo {
            timestamp: chrono::Utc::now().to_rfc3339(),
            source,
            destination,
            action,
        }
    }
}

fn total_broadcast(node_id_clone: String, payload: Pair, client: redis::Client, cluster_addrs: Arc<RwLock<Vec<ClientInfo>>>) -> Vec<impl Future<Output = Result<Response, Error>>> {
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
        async move {
            let mut con = client_clone.get_connection().expect("Failed to connect to redis");
            let value: String = con.get(&params_clone.key).unwrap();
            info!("Node {} Sending redis update to {:?}", node_id_clone, client_info);
            reqwest::Client::new()
                .post(format!("http://{}:6969/update", client_info.addr))
                .json(&json!({
                    "node": node_id_clone,
                    "key": params_clone.key,
                    "value": value
                }))
                .send()
                .await
        }
    })
    .collect();

    return tasks;
}


fn push_log(log_record: LogInfo, log_server_url: &str) -> Result<(), reqwest::Error> {
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

    let log_server: String = env::var("LOG_SERVER_URL").expect("LOG_SERVER_URL must be set");

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
        .and(warp::body::json())
        .and(client_filter.clone())
        .and_then(move |consistency_model: ConsistencyModel, client_id: String, payload: Pair, client: redis::Client| {
            let cluster_addrs = cluster_addrs_clone.clone();
            let node_id_clone = node_id_clone.clone();
            let client_id_clone = client_id.clone();
            let log_server_clone = log_server_clone.clone();
            async move {
                let mut con = client.get_connection().expect("Failed to connect to Redis");

                let _: () = con.set(&payload.key, &payload.value).expect("Failed to set key in Redis");

                info!("Node {}: set key {} = {}", node_id_clone, payload.key, payload.value);

                // Create a log record for the update operation
                let log_record = LogInfo::new(
                    format!("client-{}", client_id_clone),
                    format!("node-{}", node_id_clone),
                    format!("Client Set key: {}, value: {}", payload.key, payload.value)
                );

                let _ = push_log(log_record, &log_server_clone);

                let duration = time::Duration::from_millis(delay * 0);
                thread::sleep(duration);

                match consistency_model {
                    ConsistencyModel::Strong => {
                        // For strong consistency, ensure all nodes are updated before responding
                        let tasks = total_broadcast(node_id_clone.clone(), payload.clone(), client.clone(), cluster_addrs.clone());
                        futures::future::join_all(tasks).await;
                        info!("Strong consistency: All nodes updated.");
                    },
                    ConsistencyModel::Eventual => {
                        // For eventual consistency, respond immediately and update in the background
                        let tasks = total_broadcast(node_id_clone.clone(), payload.clone(), client.clone(), cluster_addrs.clone());
                        tokio::spawn(async move {
                            futures::future::join_all(tasks).await;
                            info!("Eventual consistency: Nodes update initiated.");
                        });
                    },
                    ConsistencyModel::Causal => {
                        // For causal consistency, updates depend on the causality chain, which might need custom logic
                        // Here, we just simulate a simple broadcast for demonstration
                        let tasks = total_broadcast(node_id_clone.clone(), payload.clone(), client.clone(), cluster_addrs.clone());
                        futures::future::join_all(tasks).await;
                        info!("Causal consistency: Causally ordered updates completed.");
                    },
                }

                // let tasks = total_broadcast(node_id_clone.clone(), payload, client, cluster_addrs);
                // futures::future::join_all(tasks).await;

                Ok::<_, warp::Rejection>(warp::reply::with_status("Key set", StatusCode::OK))
            }
        });

    let node_id_clone = node_id.clone();
    let log_server_clone = log_server.clone();

    let update = warp::post()
        .and(warp::path("update"))
        .and(warp::body::json())
        .and(client_filter.clone())
        .map(move |payload: Broadcast, client: redis::Client| {
            let mut con = client.get_connection().expect("Failed to connect to Redis");

            let _: () = con.set(&payload.key, &payload.value).expect("Failed to set key in Redis");

            info!("Node {}: update key {} = {}", node_id_clone.clone(), payload.key, payload.value);

            // Create a log record for the update operation
            let log_record = LogInfo::new(
                format!("node-{}", payload.node.clone()),
                format!("node-{}", node_id_clone.clone()),
                format!("Broadcast Set key: {}, value: {}", payload.key, payload.value)
            );

            let _ = push_log(log_record, &log_server_clone.clone());

            warp::reply::with_status("Key set", StatusCode::OK)
        });

    let node_id_clone = node_id.clone();
    let log_server_clone = log_server.clone();

    let get = warp::get()
        .and(warp::path("get"))
        .and(warp::header("Client-Id"))
        .and(warp::path::param())
        .and(client_filter.clone()) // Clone the client filter for use in this route
        .map(move |client_id: String, key: String, client: redis::Client| {
            let mut con = client.get_connection().expect("failed to connect to Redis");
            match con.get(&key) {
                Ok(value) => {
                    // Create a log record for the update operation
                    let log_record = LogInfo::new(
                        format!("client-{}", client_id.clone()),
                        format!("node-{}", node_id_clone.clone()),
                        format!("Client Got key: {}, value: {}", key, value)
                    );

                    let _ = push_log(log_record, &log_server_clone.clone());

                    warp::reply::with_status(warp::reply::json::<String>(&value), StatusCode::OK)
                }
                Err(_) => warp::reply::with_status(warp::reply::json(&"key not found"), StatusCode::NOT_FOUND)
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