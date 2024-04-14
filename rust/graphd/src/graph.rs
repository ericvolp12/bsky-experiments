use log::info;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::BufReader,
    sync::RwLock,
};

use csv;

pub struct Graph {
    follows: RwLock<HashMap<u64, HashSet<u64>>>,
    followers: RwLock<HashMap<u64, HashSet<u64>>>,
    uid_to_did: RwLock<HashMap<u64, String>>,
    did_to_uid: RwLock<HashMap<String, u64>>,
    next_uid: RwLock<u64>,
}

impl Graph {
    pub fn new(expected_node_count: u64) -> Self {
        Graph {
            follows: RwLock::new(HashMap::with_capacity(expected_node_count as usize)),
            followers: RwLock::new(HashMap::with_capacity(expected_node_count as usize)),
            uid_to_did: RwLock::new(HashMap::with_capacity(expected_node_count as usize)),
            did_to_uid: RwLock::new(HashMap::with_capacity(expected_node_count as usize)),
            next_uid: RwLock::new(0),
        }
    }

    pub fn add_follow(&self, actor: u64, target: u64) {
        self.follows
            .write()
            .unwrap()
            .entry(actor)
            .or_insert(HashSet::new())
            .insert(target);

        self.followers
            .write()
            .unwrap()
            .entry(target)
            .or_insert(HashSet::new())
            .insert(actor);
    }

    pub fn remove_follow(&self, actor: u64, target: u64) {
        if let Some(set) = self.follows.write().unwrap().get_mut(&actor) {
            set.remove(&target);
        }
        if let Some(set) = self.followers.write().unwrap().get_mut(&target) {
            set.remove(&actor);
        }
    }

    pub fn get_followers(&self, uid: u64) -> HashSet<u64> {
        self.followers
            .read()
            .unwrap()
            .get(&uid)
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_following(&self, uid: u64) -> HashSet<u64> {
        self.follows
            .read()
            .unwrap()
            .get(&uid)
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_moots(&self, uid: u64) -> HashSet<u64> {
        let follows = self.get_following(uid);
        let followers = self.get_followers(uid);
        follows.intersection(&followers).cloned().collect()
    }

    pub fn intersect_followers(&self, uids: Vec<u64>) -> HashSet<u64> {
        // Sort by number of followers ascending so we can start with the smallest set
        let mut uids = uids;
        uids.sort_by_key(|uid| self.get_followers(*uid).len());

        let mut result = self.get_followers(uids[0]);
        for uid in uids.iter().skip(1) {
            result = result
                .intersection(&self.get_followers(*uid))
                .cloned()
                .collect();
        }
        result
    }

    pub fn intersect_following(&self, uids: Vec<u64>) -> HashSet<u64> {
        // Sort by number of follows ascending so we can start with the smallest set
        let mut uids = uids;
        uids.sort_by_key(|uid| self.get_following(*uid).len());

        let mut result = self.get_following(uids[0]);
        for uid in uids.iter().skip(1) {
            result = result
                .intersection(&self.get_following(*uid))
                .cloned()
                .collect();
        }
        result
    }

    pub fn intersect_following_and_followers(&self, actor: u64, target: u64) -> HashSet<u64> {
        self.get_following(actor)
            .intersection(&self.get_followers(target))
            .cloned()
            .collect()
    }

    pub fn does_follow(&self, actor: u64, target: u64) -> bool {
        self.get_following(actor).contains(&target)
    }

    pub fn get_followers_not_following(&self, uid: u64) -> HashSet<u64> {
        self.get_followers(uid)
            .difference(&self.get_following(uid))
            .cloned()
            .collect()
    }

    pub fn acquire_did(&self, did: &str) -> u64 {
        let mut uid_to_did = self.uid_to_did.write().unwrap();
        let mut did_to_uid = self.did_to_uid.write().unwrap();
        let mut next_uid = self.next_uid.write().unwrap();

        if let Some(uid) = did_to_uid.get(did) {
            return *uid;
        }

        let uid = *next_uid;
        *next_uid += 1;
        uid_to_did.insert(uid, did.to_string());
        did_to_uid.insert(did.to_string(), uid);
        uid
    }

    pub fn get_usercount(&self) -> u64 {
        *self.next_uid.read().unwrap()
    }

    pub fn get_did(&self, uid: u64) -> Option<String> {
        self.uid_to_did.read().unwrap().get(&uid).cloned()
    }

    pub fn get_uid(&self, did: &str) -> Option<u64> {
        self.did_to_uid.read().unwrap().get(did).cloned()
    }

    pub fn get_dids(&self, vec: Vec<u64>) -> Vec<String> {
        vec.iter()
            .map(|uid| self.uid_to_did.read().unwrap().get(uid).unwrap().clone())
            .collect()
    }

    pub fn get_uids(&self, vec: Vec<String>) -> Vec<u64> {
        vec.iter()
            .map(|did| self.did_to_uid.read().unwrap().get(did).unwrap().clone())
            .collect()
    }
}

impl Graph {
    pub fn load_from_csv(&self, path: &str) -> std::io::Result<()> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        let mut rows = csv::Reader::from_reader(reader);

        let mut row_count = 0;

        for row in rows.records() {
            let record = row?;
            row_count += 1;
            if row_count % 1_000_000 == 0 {
                info!("Processed {} rows", row_count);
            }

            let actor_did = record[0].to_string();
            let target_did = record[1].to_string();

            let actor_uid = self.acquire_did(&actor_did);
            let target_uid = self.acquire_did(&target_did);

            self.add_follow(actor_uid, target_uid);
        }

        Ok(())
    }
}
