use dashmap::DashMap as HashMap;
use hashbrown::HashSet;
use log::info;
use std::{fs::File, io::BufReader, sync::RwLock};

use csv;

enum Action {
    Follow,
    Unfollow,
}

struct QueueItem {
    action: Action,
    actor: u64,
    target: u64,
}

pub struct Graph {
    follows: HashMap<u64, HashSet<u64>>,
    followers: HashMap<u64, HashSet<u64>>,
    uid_to_did: HashMap<u64, String>,
    did_to_uid: HashMap<String, u64>,
    next_uid: RwLock<u64>,
    pending_queue: RwLock<Vec<QueueItem>>,
    pub is_loaded: RwLock<bool>,
}

impl Graph {
    pub fn new(expected_node_count: u64) -> Self {
        Graph {
            follows: HashMap::with_capacity(expected_node_count as usize),
            followers: HashMap::with_capacity(expected_node_count as usize),
            uid_to_did: HashMap::with_capacity(expected_node_count as usize),
            did_to_uid: HashMap::with_capacity(expected_node_count as usize),
            next_uid: RwLock::new(0),
            pending_queue: RwLock::new(Vec::new()),
            is_loaded: RwLock::new(false),
        }
    }

    pub fn enqueue_follow(&self, actor: u64, target: u64) {
        self.pending_queue.write().unwrap().push(QueueItem {
            action: Action::Follow,
            actor,
            target,
        });
    }

    pub fn enqueue_unfollow(&self, actor: u64, target: u64) {
        self.pending_queue.write().unwrap().push(QueueItem {
            action: Action::Unfollow,
            actor,
            target,
        });
    }

    pub fn pending_queue_len(&self) -> usize {
        self.pending_queue.read().unwrap().len()
    }

    pub fn add_follow(&self, actor: u64, target: u64) {
        self.follows
            .entry(actor)
            .or_insert(HashSet::new())
            .insert(target);

        self.followers
            .entry(target)
            .or_insert(HashSet::new())
            .insert(actor);
    }

    pub fn remove_follow(&self, actor: u64, target: u64) {
        if let Some(mut set) = self.follows.get_mut(&actor) {
            set.remove(&target);
        }
        if let Some(mut set) = self.followers.get_mut(&target) {
            set.remove(&actor);
        }
    }

    pub fn get_followers(&self, uid: u64) -> HashSet<u64> {
        self.followers.get(&uid).unwrap().clone()
    }

    pub fn get_following(&self, uid: u64) -> HashSet<u64> {
        self.follows.get(&uid).unwrap().clone()
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
        let mut next_uid = self.next_uid.write().unwrap();

        if let Some(uid) = self.did_to_uid.get(did) {
            return *uid;
        }

        let uid = *next_uid;
        *next_uid += 1;
        self.uid_to_did.insert(uid, did.to_string());
        self.did_to_uid.insert(did.to_string(), uid);
        uid
    }

    pub fn get_usercount(&self) -> u64 {
        *self.next_uid.read().unwrap()
    }

    pub fn get_did(&self, uid: u64) -> Option<String> {
        self.uid_to_did.get(&uid).map(|s| s.clone())
    }

    pub fn get_uid(&self, did: &str) -> Option<u64> {
        self.did_to_uid.get(did).map(|u| *u)
    }

    pub fn get_dids(&self, vec: Vec<u64>) -> Vec<String> {
        vec.iter()
            .map(|uid| self.uid_to_did.get(uid).unwrap().clone())
            .collect()
    }

    pub fn get_uids(&self, vec: Vec<String>) -> Vec<u64> {
        vec.iter()
            .map(|did| self.did_to_uid.get(did).unwrap().clone())
            .collect()
    }
}

impl Graph {
    pub fn load_from_csv(&self, path: &str) -> std::io::Result<()> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        let mut rows = csv::Reader::from_reader(reader);
        let mut rec = csv::StringRecord::new();
        let mut row_count = 0;

        while rows.read_record(&mut rec)? {
            row_count += 1;
            if row_count % 1_000_000 == 0 {
                info!("Processed {} rows", row_count);
            }

            let actor_did = rec.get(0).unwrap();
            let target_did = rec.get(1).unwrap();

            let actor_uid = self.acquire_did(actor_did);
            let target_uid = self.acquire_did(target_did);

            self.add_follow(actor_uid, target_uid);
        }

        // Note: There's a race condition here where if someone follows and then unfollows in the
        // window between the graph being loaded and the follow queue being processed, the unfollow
        // will be ignored. For now that's an acceptable limitation of the design.

        *self.is_loaded.write().unwrap() = true;

        // Play through the pending queue
        for item in self.pending_queue.write().unwrap().iter() {
            match item.action {
                Action::Follow => self.add_follow(item.actor, item.target),
                Action::Unfollow => self.remove_follow(item.actor, item.target),
            }
        }

        // Clear the queue
        self.pending_queue.write().unwrap().clear();

        info!("Loaded graph with {} users", self.get_usercount());

        Ok(())
    }
}
