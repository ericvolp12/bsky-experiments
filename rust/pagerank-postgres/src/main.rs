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
    let mut uid_to_did: Vec<Option<String>> = Vec::new(); // Change to use Vec
    let mut next_uid: usize = 1;

    // Preallocate 5M elements for the vector
    uid_to_did.reserve(5_000_000);

    let rows = client.query("SELECT did FROM actors", &[]).await?;
    for row in rows {
        let did: String = row.get(0);
        did_to_uid.insert(did.clone(), next_uid);
        if uid_to_did.len() <= next_uid {
            uid_to_did.resize((next_uid + 1) as usize, None); // Ensure vector is large enough
        }
        uid_to_did[next_uid] = Some(did); // Directly assign DID to the vector
        next_uid += 1;
    }

    info!("loaded {} actors", next_uid - 1);

    // Pre-load target UIDs for each UID into memory
    let mut uid_to_targets: Vec<HashSet<usize>> = vec![HashSet::new(); next_uid];
    let mut num_rows = 0;
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
                uid_to_targets[actor_uid].insert(target_uid);
            }
        }
    }

    info!("loaded {} follow rows", num_rows);

    // Initialize PageRank values
    let mut pageranks = vec![1.0; next_uid];

    info!("running pagerank");

    let next_uid_f64 = next_uid as f64;

    // Compute PageRank with adjustments for vector use
    const MAX_ITERATIONS: usize = 100;
    const DAMPING_FACTOR: f64 = 0.85;
    for _iteration in 0..MAX_ITERATIONS {
        let mut new_pageranks = vec![0.0; next_uid];

        for (uid, &rank) in pageranks.iter().enumerate() {
            if let Some(targets) = uid_to_targets.get(uid) {
                let share = rank / targets.len() as f64;
                for &target_uid in targets {
                    new_pageranks[target_uid] += share;
                }
            }
        }

        for uid in 0..next_uid {
            let rank = new_pageranks[uid];
            pageranks[uid] = (1.0 - DAMPING_FACTOR) / next_uid_f64 + DAMPING_FACTOR * rank;
        }

        info!("Iteration {}/{}", _iteration + 1, MAX_ITERATIONS);
    }

    // Sort pageranks by rank
    let mut ranks: Vec<_> = (0..next_uid).collect();
    ranks.sort_by(|&a, &b| pageranks[b].partial_cmp(&pageranks[a]).unwrap());

    // CSV writing part needs minor adjustments for using uid_to_did vector
    let file = File::create("pageranks.csv").expect("Unable to create file");
    let mut wtr = csv::Writer::from_writer(file);

    info!("writing pageranks.csv");

    for &uid in &ranks {
        if let Some(did) = uid_to_did[uid].as_ref() {
            let rank = format!("{:.10}", pageranks[uid]);
            wtr.write_record(&[did, &rank])
                .expect("Unable to write record");
        }
    }

    wtr.flush().unwrap();

    info!("done");

    Ok(())
}
