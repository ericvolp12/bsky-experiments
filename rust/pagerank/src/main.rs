use tokio_postgres::{NoTls, Error};
use hashbrown::HashMap;
use std::collections::HashSet;


#[tokio::main]
async fn main() -> Result<(), Error> {
    // Connect to the PostgreSQL database
    let (client, connection) =
        tokio_postgres::connect("secret", NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

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

    // Initialize PageRank values
    let mut pageranks = HashMap::new();
    for &uid in did_to_uid.values() {
        pageranks.insert(uid, 1.0);
    }

    // Compute PageRank
    const MAX_ITERATIONS: usize = 100;
    const DAMPING_FACTOR: f64 = 0.85;
    for _ in 0..MAX_ITERATIONS {
        let mut new_pageranks = HashMap::new();

        for (&uid, &rank) in &pageranks {
            let target_uids = fetch_targets(&client, uid, &did_to_uid).await?;
            let share = rank / target_uids.len() as f64;

            for target_uid in target_uids {
                *new_pageranks.entry(target_uid).or_insert(0.0) += share;
            }
        }

        for uid in pageranks.keys() {
            let rank = new_pageranks.entry(*uid).or_insert(0.0);
            *rank = (1.0 - DAMPING_FACTOR) + DAMPING_FACTOR * (*rank);
        }

        pageranks = new_pageranks;
    }

    // Print PageRank results
    for (uid, rank) in pageranks {
        println!("DID: {}, PageRank: {}", uid_to_did[&uid], rank);
    }

    Ok(())
}

// Fetches target UIDs for a given UID
async fn fetch_targets(client: &tokio_postgres::Client, uid: i64, did_to_uid: &HashMap<String, i64>) -> Result<HashSet<i64>, Error> {
    let did = did_to_uid.iter().find_map(|(k, &v)| if v == uid { Some(k) } else { None }).unwrap();
    let rows = client.query("SELECT target_did FROM follows WHERE actor_did = $1", &[&did]).await?;
    let mut targets = HashSet::new();
    for row in rows {
        let target_did: String = row.get(0);
        if let Some(&target_uid) = did_to_uid.get(&target_did) {
            targets.insert(target_uid);
        }
    }
    Ok(targets)
}
