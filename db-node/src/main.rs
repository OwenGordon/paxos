use redis::Commands;
use serde_json::json;
use std::env;
use local_ip_address::local_ip;

use serde::{Deserialize, Serialize};
use warp::{Filter, http::StatusCode};
use env_logger::Env;

use log::info;
use std::sync::{Arc, RwLock};

use reqwest;

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

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let master_url: String = env::var("MASTER_URL").expect("MASTER_URL must be set");

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

    info!("Node registered with ID: {}", node_id);

    let redis_url = env::var("REDIS_URL").expect("REDIS_URL must be set");
    let client = redis::Client::open(redis_url).expect("invalid Redis URL");

    let cluster_addrs: Arc<RwLock<Vec<ClientInfo>>> = Arc::new(RwLock::new(Vec::new()));

    // Use warp::any to create a filter that clones the client for each request
    let client_filter = warp::any().map(move || client.clone());

    let cluster_addrs_clone = cluster_addrs.clone();

    let set = warp::post()
        .and(warp::path("set"))
        .and(warp::body::json())
        .and(client_filter.clone())
        .and_then(move |params: Pair, client: redis::Client| {
            let cluster_addrs = cluster_addrs_clone.clone();
            let node_id_clone: String = node_id.clone().to_string().replace("\"", "");
            async move {
                let mut con = client.get_connection().expect("Failed to connect to Redis");

                let _: () = con.set(&params.key, &params.value).expect("Failed to set key in Redis");

                info!("Node {}: key {} = {}", node_id_clone, params.key, params.value);

                let tasks: Vec<_> = <Vec<ClientInfo> as Clone>::clone(&cluster_addrs.read().unwrap())
                    .into_iter()
                    .filter(|client_info| {
                        let client_id_str = client_info.id.to_string();
                        let is_same = client_id_str == node_id_clone;
                        !is_same
                    })
                    .map(|client_info| {
                        let params_clone = params.clone();
                        let node_id_clone =  node_id_clone.clone();
                        async move {
                            info!("I am: id={}, addr={}, Sending redis update to {:?}", node_id_clone, local_ip, client_info);
                            reqwest::Client::new()
                                .post(format!("http://{}:6969/update", client_info.addr))
                                .json(&json!({
                                    "key": params_clone.key,
                                    "value": params_clone.value
                                }))
                                .send()
                                .await
                        }
                    })
                    .collect();

                futures::future::join_all(tasks).await;

                Ok::<_, warp::Rejection>(warp::reply::with_status("Key set", StatusCode::OK))
            }
        });

    let update = warp::post()
        .and(warp::path("update"))
        .and(warp::body::json())
        .and(client_filter.clone())
        .map(move |params: Pair, client: redis::Client| {
            let mut con = client.get_connection().expect("Failed to connect to Redis");

            let _: () = con.set(&params.key, &params.value).expect("Failed to set key in Redis");

            info!("key {} = {}", params.key, params.value);

            warp::reply::with_status("Key set", StatusCode::OK)
        });

    let get = warp::get()
        .and(warp::path("get"))
        .and(warp::path::param())
        .and(client_filter.clone()) // Clone the client filter for use in this route
        .map(|key: String, client: redis::Client| {
            let mut con = client.get_connection().expect("failed to connect to Redis");
            match con.get(&key) {
                Ok(result) => warp::reply::with_status(warp::reply::json::<String>(&result), StatusCode::OK),
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