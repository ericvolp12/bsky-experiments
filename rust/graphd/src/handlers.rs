use axum::{
    extract::Query,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::graph;

#[derive(Clone)]
pub struct AppState {
    pub graph: Arc<graph::Graph>,
}

pub enum Errors {
    StillLoading,
}

impl IntoResponse for Errors {
    fn into_response(self) -> Response {
        let body = match self {
            Errors::StillLoading => "graph data is still loading",
        };

        // its often easiest to implement `IntoResponse` by calling other implementations
        (StatusCode::SERVICE_UNAVAILABLE, body).into_response()
    }
}

#[derive(Deserialize)]
pub struct HealthStatusQuery {
    stats: String,
}

#[derive(Serialize)]
pub struct HealthStatus {
    status: &'static str,
    version: &'static str,
    user_count: Option<u32>,
    follow_count: Option<u32>,
    loaded: bool,
    pending_queue_len: Option<usize>,
}

pub async fn health(
    state: Extension<AppState>,
    Query(query): Query<HealthStatusQuery>,
) -> impl IntoResponse {
    let mut status = HealthStatus {
        status: "ok",
        version: "0.1.0",
        user_count: None,
        follow_count: None,
        loaded: *state.graph.is_loaded.read().unwrap(),
        pending_queue_len: None,
    };

    if query.stats == "true" {
        status.user_count = Some(state.graph.get_usercount());
        status.pending_queue_len = Some(state.graph.pending_queue_len());
    }

    Json(status)
}

#[derive(Deserialize)]
pub struct FollowsBody {
    follows: Vec<Follow>,
}

#[derive(Deserialize)]
pub struct Follow {
    actor_did: String,
    target_did: String,
}

pub async fn post_follows(
    state: Extension<AppState>,
    Json(body): Json<FollowsBody>,
) -> impl IntoResponse {
    for follow in body.follows {
        let actor_uid = state.graph.acquire_did(&follow.actor_did);
        let target_uid = state.graph.acquire_did(&follow.target_did);

        // If the graph isn't loaded yet, enqueue the follow request
        if !state.graph.is_loaded.read().unwrap().clone() {
            state.graph.enqueue_follow(actor_uid, target_uid);
            continue;
        }

        state.graph.add_follow(actor_uid, target_uid);
    }

    StatusCode::OK
}

pub async fn post_follow(state: Extension<AppState>, body: Json<Follow>) -> impl IntoResponse {
    let actor_uid = state.graph.acquire_did(&body.actor_did);
    let target_uid = state.graph.acquire_did(&body.target_did);

    // If the graph isn't loaded yet, enqueue the follow request
    if !state.graph.is_loaded.read().unwrap().clone() {
        state.graph.enqueue_follow(actor_uid, target_uid);
        return StatusCode::OK;
    }

    state.graph.add_follow(actor_uid, target_uid);
    StatusCode::OK
}

#[derive(Serialize)]
pub struct FollowingResponse {
    dids: Vec<String>,
}

#[derive(Deserialize)]
pub struct FollowingQuery {
    did: String,
}

pub async fn get_following(
    state: Extension<AppState>,
    Query(query): Query<FollowingQuery>,
) -> Result<Json<FollowingResponse>, Errors> {
    // If not loaded, return an error
    if !state.graph.is_loaded.read().unwrap().clone() {
        return Err(Errors::StillLoading);
    }

    let uid = state.graph.get_uid(&query.did);
    if uid.is_none() {
        return Ok(Json(FollowingResponse { dids: vec![] }));
    }
    let dids = state
        .graph
        .get_following(uid.unwrap())
        .iter()
        .map(|uid| state.graph.get_did(uid).unwrap())
        .collect();
    Ok(Json(FollowingResponse { dids }))
}

#[derive(Serialize)]
pub struct FollowersResponse {
    dids: Vec<String>,
}

#[derive(Deserialize)]
pub struct FollowersQuery {
    did: String,
}

pub async fn get_followers(
    state: Extension<AppState>,
    Query(query): Query<FollowersQuery>,
) -> Result<Json<FollowersResponse>, Errors> {
    // If not loaded, return an error
    if !state.graph.is_loaded.read().unwrap().clone() {
        return Err(Errors::StillLoading);
    }

    let uid = state.graph.get_uid(&query.did);
    if uid.is_none() {
        return Ok(Json(FollowersResponse { dids: vec![] }));
    }
    let dids = state
        .graph
        .get_followers(uid.unwrap())
        .iter()
        .map(|uid| state.graph.get_did(uid).unwrap())
        .collect();
    Ok(Json(FollowersResponse { dids }))
}

#[derive(Serialize)]
pub struct FollowersNotFollowingResponse {
    dids: Vec<String>,
}

#[derive(Deserialize)]
pub struct FollowersNotFollowingQuery {
    did: String,
}

pub async fn get_followers_not_following(
    state: Extension<AppState>,
    Query(query): Query<FollowersNotFollowingQuery>,
) -> Result<Json<FollowersNotFollowingResponse>, Errors> {
    // If not loaded, return an error
    if !state.graph.is_loaded.read().unwrap().clone() {
        return Err(Errors::StillLoading);
    }

    let uid = state.graph.get_uid(&query.did);
    if uid.is_none() {
        return Ok(Json(FollowersNotFollowingResponse { dids: vec![] }));
    }
    let dids = state
        .graph
        .get_followers_not_following(uid.unwrap())
        .iter()
        .map(|uid| state.graph.get_did(uid).unwrap())
        .collect();
    Ok(Json(FollowersNotFollowingResponse { dids }))
}

#[derive(Serialize)]
pub struct IntersectFollowingAndFollowersResponse {
    dids: Vec<String>,
}

#[derive(Deserialize)]
pub struct IntersectFollowingAndFollowersQuery {
    actor_did: String,
    target_did: String,
}

pub async fn get_intersect_following_and_followers(
    state: Extension<AppState>,
    Query(query): Query<IntersectFollowingAndFollowersQuery>,
) -> Result<Json<IntersectFollowingAndFollowersResponse>, Errors> {
    // If not loaded, return an error
    if !state.graph.is_loaded.read().unwrap().clone() {
        return Err(Errors::StillLoading);
    }

    let actor_uid = state.graph.get_uid(&query.actor_did);
    let target_uid = state.graph.get_uid(&query.target_did);
    if actor_uid.is_none() || target_uid.is_none() {
        return Ok(Json(IntersectFollowingAndFollowersResponse {
            dids: vec![],
        }));
    }
    let dids = state
        .graph
        .intersect_following_and_followers(actor_uid.unwrap(), target_uid.unwrap())
        .iter()
        .map(|uid| state.graph.get_did(uid).unwrap())
        .collect();
    Ok(Json(IntersectFollowingAndFollowersResponse { dids }))
}

#[derive(Serialize)]
pub struct DoesFollowResponse {
    does_follow: bool,
}

#[derive(Deserialize)]
pub struct DoesFollowQuery {
    actor_did: String,
    target_did: String,
}

pub async fn get_does_follow(
    state: Extension<AppState>,
    Query(query): Query<DoesFollowQuery>,
) -> Result<Json<DoesFollowResponse>, Errors> {
    // If not loaded, return an error
    if !state.graph.is_loaded.read().unwrap().clone() {
        return Err(Errors::StillLoading);
    }

    let actor_uid = state.graph.get_uid(&query.actor_did);
    let target_uid = state.graph.get_uid(&query.target_did);
    if actor_uid.is_none() || target_uid.is_none() {
        return Ok(Json(DoesFollowResponse { does_follow: false }));
    }
    let does_follow = state
        .graph
        .does_follow(actor_uid.unwrap(), target_uid.unwrap());
    Ok(Json(DoesFollowResponse { does_follow }))
}

#[derive(Deserialize)]
pub struct UnfollowRequest {
    actor_did: String,
    target_did: String,
}

pub async fn post_unfollow(
    state: Extension<AppState>,
    body: Json<UnfollowRequest>,
) -> impl IntoResponse {
    let actor_uid = state.graph.acquire_did(&body.actor_did);
    let target_uid = state.graph.acquire_did(&body.target_did);

    // If the graph isn't loaded yet, enqueue the unfollow request
    if !state.graph.is_loaded.read().unwrap().clone() {
        state.graph.enqueue_unfollow(actor_uid, target_uid);
        return StatusCode::OK;
    }

    state.graph.remove_follow(actor_uid, target_uid);
    StatusCode::OK
}
