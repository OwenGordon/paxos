use warp::{Filter, http::StatusCode};
use log::info;
use env_logger::Env;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap; // Added this line to import HashMap
use reqwest;

use serde::{Deserialize, Serialize};

// Create a struct to hold the client information
#[derive(Deserialize, Serialize, Clone)]
struct ClientInfo {
    id: String,
    addr: String,
}

// Create a type alias for the database
type Database = Arc<RwLock<HashMap<String, ClientInfo>>>;


#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    // Shared state
    let counter = Arc::new(AtomicUsize::new(1));

    // Initialize the database
    let db: Database = Arc::new(RwLock::new(HashMap::new()));

    let register_db = db.clone();

    let register = warp::post()
        .and(warp::path("register"))
        .and(warp::body::json::<String>())
        .map(move |addr: String| {
            info!("Registering client {}", addr);
            let id = counter.fetch_add(1, Ordering::Relaxed).to_string();

            let client_info = ClientInfo {
                id: id.clone(),
                addr: addr.clone(),
            };

            register_db.write().unwrap().insert(addr.clone(), client_info);

            info!("Registering client {} with id: {}. {} Total clients registered.", addr, id, register_db.read().unwrap().len());

            warp::reply::with_status(warp::reply::json(&id), StatusCode::OK)
        });

    info!("Master running on port 3000");

    // Start the health checker
    let health_check_db = db.clone();
    tokio::spawn(async move {
        health_checker(health_check_db, 30).await; // Check every 30 seconds
    });

    let routes = register;
    warp::serve(routes).run(([0, 0, 0, 0], 3000)).await;
}

async fn health_checker(db: Database, interval: u64) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval));

    loop {
        interval.tick().await;

        let clients_to_check = {
            let db_read = db.read().unwrap();
            db_read.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<HashMap<_, _>>()
        };

        let client_addrs = clients_to_check.clone();

        let mut healthy_clients = HashMap::new();

        for (key, client_info) in clients_to_check {
            let health = is_client_healthy(client_info, client_addrs.clone().values().cloned().collect()).await;
            
            if health {
                healthy_clients.insert(key, true);
            }
        }

        let mut db_write = db.write().unwrap();
        db_write.retain(|key, _| healthy_clients.contains_key(key));
    }
}

async fn is_client_healthy(client_info: ClientInfo, addrs: Vec<ClientInfo>) -> bool {
    info!("Checking client health for {}", client_info.addr);
    let client_url = format!("http://{}:6969/health-check", client_info.addr);
    
    // Correctly handle serialization of data within RwLock
    let serialized_data = serde_json::to_string(&addrs).expect("Failed to serialize data");

    // Wrap the reqwest::Client in a Mutex to make it Send
    let client = reqwest::Client::new();
    let response = client.post(&client_url).body(serialized_data).send().await;

    match response {
        Ok(resp) => resp.status() == reqwest::StatusCode::OK,
        Err(_) => false,
    }
}