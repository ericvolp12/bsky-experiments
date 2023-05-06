use actix_web::{middleware::Logger, post, web, App, HttpResponse, HttpServer, Result};
use serde::{Deserialize, Serialize};
use forceatlas2::{self, Settings, Nodes};
use forceatlas2::Layout as ForceAtlas2Layout;
use log::{info, LevelFilter};

#[derive(Deserialize)]
struct EdgeList {
    edges: Vec<(usize, usize)>,
    ka: f32,
    kg: f32,
    kr: f32,
    chunk_size: usize,
    nb_nodes: usize,
    iterations: usize,
}

#[derive(Serialize)]
struct PointListJson {
    points: Vec<Vec<f32>>,
}

struct LayoutWrapper {
    layout: ForceAtlas2Layout<f32>,
}

impl Serialize for LayoutWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let points: Vec<Vec<f32>> = self
            .layout
            .points
            .iter()
            .map(|point| vec![point[0], point[1]])
            .collect();
        PointListJson { points }.serialize(serializer)
    }
}

#[post("/fa2_layout")]
async fn get_fa2_layout(edge_list: web::Json<EdgeList>) -> Result<HttpResponse> {
    let settings = Settings {
        ka: edge_list.ka,
        kg: edge_list.kg,
        kr: edge_list.kr,
        chunk_size: Some(edge_list.chunk_size),
        ..Default::default()
    };

    let mut layout = forceatlas2::Layout::from_graph(
        edge_list.edges.clone(),
        Nodes::Degree(edge_list.nb_nodes),
        None,
        settings,
    );

    for i in 0..edge_list.iterations {
        layout.iteration();
        if (i + 1) % 25 == 0 {
            info!("Iteration {}: Completed", i + 1);
        }
    }

    let layout_wrapper = LayoutWrapper { layout };

    Ok(HttpResponse::Ok().json(&layout_wrapper))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    HttpServer::new(|| {
        App::new()
            .wrap(Logger::default())
            .service(get_fa2_layout)
    })
    .bind("0.0.0.0:8086")?
    .run()
    .await
}
