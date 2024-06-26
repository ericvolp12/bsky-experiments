#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use axum::{
    routing::{get, post},
    Extension, Router,
};
use axum_prometheus::{
    metrics_exporter_prometheus::{Matcher, PrometheusBuilder},
    PrometheusMetricLayer, AXUM_HTTP_REQUESTS_DURATION_SECONDS,
};
use log::{info, warn};
use metrics_process::Collector;
use std::sync::Arc;
use tokio;

mod graph;
mod handlers;

#[tokio::main]
async fn main() {
    env_logger::init();

    let port = std::env::var("PORT").unwrap_or("8000".to_string());
    let csv_path = std::env::var("CSV_PATH").unwrap_or("data/follows.csv".to_string());
    let expected_node_count = std::env::var("EXPECTED_NODE_COUNT")
        .unwrap_or("5000000".to_string())
        .parse::<u32>()
        .unwrap();

    info!("Starting up");
    let graph = graph::Graph::new(expected_node_count);
    let graph = Arc::new(graph); // Wrap the graph in Arc and Mutex for safe concurrent access

    let graph_clone = graph.clone();
    let csv_path_clone = csv_path.clone();
    tokio::spawn(async move {
        match graph_clone.load_from_csv(&csv_path_clone) {
            Ok(_) => info!("Loaded graph from CSV"),
            Err(e) => warn!("Failed to load graph: {}", e),
        }
    });

    let state = handlers::AppState { graph };

    let collector = Collector::default();
    collector.describe();

    let metric_layer = PrometheusMetricLayer::new();
    // This is the default if you use `PrometheusMetricLayer::pair`.
    let metric_handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(AXUM_HTTP_REQUESTS_DURATION_SECONDS.to_string()),
            &[
                0.00001, 0.00002, 0.00005, 0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02,
                0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0,
            ],
        )
        .unwrap()
        .install_recorder()
        .unwrap();

    let app = Router::new()
        .route("/health", get(handlers::health))
        .route("/follow", post(handlers::post_follow))
        .route("/follows", post(handlers::post_follows))
        .route("/unfollow", post(handlers::post_unfollow))
        .route("/following", get(handlers::get_following))
        .route("/followers", get(handlers::get_followers))
        .route(
            "/followers_not_following",
            get(handlers::get_followers_not_following),
        )
        .route(
            "/follows_following",
            get(handlers::get_intersect_following_and_followers),
        )
        .route("/does_follow", get(handlers::get_does_follow))
        .route("/flush_updates", get(handlers::get_flush_updates))
        .layer(Extension(state))
        .route(
            "/metrics",
            get(|| async move {
                collector.collect();
                metric_handle.render()
            }),
        )
        .layer(metric_layer);

    println!("Listening on port {}", port);

    let listen_address = format!("0.0.0.0:{}", port);

    let listener = tokio::net::TcpListener::bind(listen_address).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
