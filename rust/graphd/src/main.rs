use axum::{
    routing::{get, post},
    Extension, Router,
};
use axum_prometheus::{
    metrics_exporter_prometheus::{Matcher, PrometheusBuilder},
    PrometheusMetricLayer, AXUM_HTTP_REQUESTS_DURATION_SECONDS,
};
use log::info;
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

    info!("Starting up");
    let graph = graph::Graph::new(5_000_000);
    graph.load_from_csv(&csv_path).unwrap();
    info!("Loaded graph with {} users", graph.get_usercount());

    let state = handlers::AppState {
        graph: Arc::new(graph),
    };

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
        .layer(Extension(state))
        .route(
            "/metrics",
            get(|| async move {
                collector.collect();
                metric_handle.render()
            }),
        )
        .layer(metric_layer);

    println!("🚀 Server started successfully");

    let listen_address = format!("0.0.0.0:{}", port);

    let listener = tokio::net::TcpListener::bind(listen_address).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
