use hashbrown::HashMap;
use log::info;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Enable info logging
    env_logger::init();

    // Get file paths from environment variables or use default values
    let actors_file_path =
        std::env::var("ACTORS_FILE").unwrap_or_else(|_| "actors.csv".to_string());
    let follows_file_path =
        std::env::var("FOLLOWS_FILE").unwrap_or_else(|_| "follows.csv".to_string());
    let output_file_path =
        std::env::var("OUTPUT_FILE").unwrap_or_else(|_| "pageranks.csv".to_string());
        
    let expected_actor_count: usize = std::env::var("EXPECTED_ACTOR_COUNT")
        .unwrap_or_else(|_| "5000000".to_string())
        .parse()
        .unwrap();

    info!("loading actors");

    // Fetch and map DIDs to UIDs
    let mut did_to_uid = HashMap::new();
    let mut uid_to_did: Vec<Option<String>> = Vec::new();
    let mut next_uid: usize = 1;

    did_to_uid.reserve(expected_actor_count);
    uid_to_did.reserve(expected_actor_count);

    let actors_file = File::open(&actors_file_path)?;
    let actors_reader = BufReader::new(actors_file);

    for line in actors_reader.lines() {
        let did = line?;
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

    let follows_file = File::open(&follows_file_path)?;
    let follows_reader = BufReader::new(follows_file);

    for line in follows_reader.lines() {
        let row = line?;
        num_rows += 1;
        if num_rows % 1_000_000 == 0 {
            info!("Loaded {} follow rows", num_rows);
        }
        let mut iter = row.split(',');
        let actor_did = iter.next().unwrap().to_string();
        let target_did = iter.next().unwrap().to_string();
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

    // Compute PageRank
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

    let output_file = File::create(&output_file_path)?;
    let mut wtr = csv::Writer::from_writer(output_file);

    info!("writing {}", output_file_path);

    for &uid in &ranks {
        if let Some(did) = uid_to_did[uid].as_ref() {
            let rank = format!("{:.10}", pageranks[uid]);
            wtr.write_record(&[did, &rank])
                .expect("Unable to write record");
        }
    }

    wtr.flush()?;

    info!("done");

    Ok(())
}
