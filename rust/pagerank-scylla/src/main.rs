use hashbrown::HashMap;
use log::info;
use scylla::Session;
use std::collections::HashSet;
use std::fs::File;

#[tokio::main]
async fn main() -> Result<(), scylla::transport::errors::QueryError> {
    // Get connection string from environment
    let connection_string = std::env::var("SCYLLA_URI").unwrap();

    if &connection_string == "" {
        panic!("SCYLLA_URI environment variable not set");
    }

    // Connect to the ScyllaDB cluster
    let session = Session::connect(&connection_string, None).await?;

    // Enable info logging
    env_logger::init();

    info!("loading actors");

    // Fetch and map DIDs to UIDs
    let mut did_to_uid = HashMap::new();
    let mut uid_to_did: Vec<Option<String>> = Vec::new();
    let mut next_uid: usize = 1;

    // Preallocate 5M elements for the vector
    uid_to_did.reserve(5_000_000);

    let rows = session
        .query("SELECT did FROM actors", &[])
        .await?
        .rows;
    for row in rows {
        let did: String = row.columns[0].as_text().unwrap().to_string();
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
    let follow_rows = session
        .query("SELECT actor_did, subject_did FROM follows", &[])
        .await?
        .rows;
    for row in follow_rows {
        num_rows += 1;
        if num_rows % 1_000_000 == 0 {
            info!("Loaded {} follow rows", num_rows);
        }
        let actor_did: String = row.columns[0].as_text().unwrap().to_string();
        let target_did: String = row.columns[1].as_text().unwrap().to_string();
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
