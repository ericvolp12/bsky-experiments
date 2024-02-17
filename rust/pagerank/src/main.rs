use hashbrown::HashMap;
use log::info;
use std::collections::HashSet;
use std::fs::File;
use tokio_postgres::{Error, NoTls};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Get connection string from environment
    let connection_string = std::env::var("DATABASE_URL").unwrap();

    if &connection_string == "" {
        panic!("DATABASE_URL environment variable not set");
    }

    // Connect to the PostgreSQL database
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Enable info logging
    env_logger::init();

    info!("loading actors");

    // Fetch and map DIDs to UIDs
    let mut did_to_uid = HashMap::new();
    let mut uid_to_did = HashMap::new();
    let mut next_uid: i64 = 1;
    let rows = client.query("SELECT did FROM actors", &[]).await?;
    for row in rows {
        let did: String = row.get(0);
        did_to_uid.insert(did.clone(), next_uid);
        uid_to_did.insert(next_uid, did);
        next_uid += 1;
    }

    info!("loaded {} actors", next_uid - 1);

    // Pre-load target UIDs for each UID into memory
    let mut num_rows = 0;
    let mut uid_to_targets = HashMap::new();
    let follow_rows = client
        .query("SELECT actor_did, target_did FROM follows", &[])
        .await?;
    for row in follow_rows {
        num_rows += 1;
        if num_rows % 1_000_000 == 0 {
            info!("Loaded {} follow rows", num_rows);
        }
        let actor_did: String = row.get(0);
        let target_did: String = row.get(1);
        if let Some(&actor_uid) = did_to_uid.get(&actor_did) {
            if let Some(&target_uid) = did_to_uid.get(&target_did) {
                uid_to_targets
                    .entry(actor_uid)
                    .or_insert_with(HashSet::new)
                    .insert(target_uid);
            }
        }
    }

    info!("loaded {} follow rows", num_rows);

    // Initialize PageRank values
    let mut pageranks = HashMap::new();
    for &uid in did_to_uid.values() {
        pageranks.insert(uid, 1.0);
    }

    info!("running pagerank");

    // Compute PageRank
    const MAX_ITERATIONS: usize = 100;
    const DAMPING_FACTOR: f64 = 0.85;
    for _iteration in 0..MAX_ITERATIONS {
        let mut new_pageranks = HashMap::new();

        for (&uid, &rank) in &pageranks {
            if let Some(targets) = uid_to_targets.get(&uid) {
                let share = rank / targets.len() as f64;
                for &target_uid in targets {
                    *new_pageranks.entry(target_uid).or_insert(0.0) += share;
                }
            }
        }

        let mut updated_ranks = pageranks.clone();

        for uid in pageranks.keys() {
            let rank = *new_pageranks.get(uid).unwrap_or(&0.0);
            updated_ranks.insert(
                *uid,
                (1.0 - DAMPING_FACTOR) / did_to_uid.len() as f64 + DAMPING_FACTOR * rank,
            );
        }

        info!("Iteration {}/{}", _iteration, MAX_ITERATIONS);

        pageranks = updated_ranks;
    }

    // Create a new CSV writer that writes to `pageranks.csv`
    let file = File::create("pageranks.csv").expect("Unable to create file");
    let mut wtr = csv::Writer::from_writer(file);

    info!("writing pageranks.csv");

    // Write the header row
    wtr.write_record(&["DID", "PageRank"])
        .expect("Unable to write header");

    // Write PageRank results to the CSV
    for (uid, rank) in &pageranks {
        let did = uid_to_did.get(uid).expect("DID not found");
        wtr.write_record(&[did, &format!("{:.4}", rank)])
            .expect("Unable to write row");
    }

    // Ensure all writes are flushed properly
    wtr.flush().expect("Failed to flush writer");

    info!("pageranks.csv written");

    Ok(())
}
