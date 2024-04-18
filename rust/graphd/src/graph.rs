use hashbrown::HashMap;
use log::info;
use roaring::bitmap::RoaringBitmap;
use std::{fs::File, io::BufReader, ops::BitAnd, sync::RwLock};

use csv;

enum Action {
    Follow,
    Unfollow,
}

struct QueueItem {
    action: Action,
    actor: u32,
    target: u32,
}

struct ActorMap {
    following: RwLock<RoaringBitmap>,
    followers: RwLock<RoaringBitmap>,
}

pub struct Graph {
    actors: RwLock<HashMap<u32, ActorMap>>,
    uid_to_did: RwLock<HashMap<u32, String>>,
    did_to_uid: RwLock<HashMap<String, u32>>,
    next_uid: RwLock<u32>,
    pending_queue: RwLock<Vec<QueueItem>>,
    pub is_loaded: RwLock<bool>,
}

impl Graph {
    pub fn new(expected_node_count: u32) -> Self {
        Graph {
            actors: RwLock::new(HashMap::with_capacity(expected_node_count as usize)),
            uid_to_did: RwLock::new(HashMap::with_capacity(expected_node_count as usize)),
            did_to_uid: RwLock::new(HashMap::with_capacity(expected_node_count as usize)),
            next_uid: RwLock::new(0),
            pending_queue: RwLock::new(Vec::new()),
            is_loaded: RwLock::new(false),
        }
    }

    pub fn enqueue_follow(&self, actor: u32, target: u32) {
        self.pending_queue.write().unwrap().push(QueueItem {
            action: Action::Follow,
            actor,
            target,
        });
    }

    pub fn enqueue_unfollow(&self, actor: u32, target: u32) {
        self.pending_queue.write().unwrap().push(QueueItem {
            action: Action::Unfollow,
            actor,
            target,
        });
    }

    pub fn pending_queue_len(&self) -> usize {
        self.pending_queue.read().unwrap().len()
    }

    pub fn add_follow(&self, actor: u32, target: u32) {
        let mut actors = self.actors.write().unwrap();
        let actor_map = actors.entry(actor).or_insert_with(|| ActorMap {
            following: RwLock::new(RoaringBitmap::new()),
            followers: RwLock::new(RoaringBitmap::new()),
        });
        actor_map.following.write().unwrap().insert(target);

        let target_map = actors.entry(target).or_insert_with(|| ActorMap {
            following: RwLock::new(RoaringBitmap::new()),
            followers: RwLock::new(RoaringBitmap::new()),
        });
        target_map.followers.write().unwrap().insert(actor);
    }

    pub fn remove_follow(&self, actor: u32, target: u32) {
        let mut actors = self.actors.write().unwrap();
        if let Some(actor_map) = actors.get_mut(&actor) {
            actor_map.following.write().unwrap().remove(target);
        }

        if let Some(target_map) = actors.get_mut(&target) {
            target_map.followers.write().unwrap().remove(actor);
        }
    }

    pub fn get_followers(&self, uid: u32) -> RoaringBitmap {
        if let Some(actor) = self.actors.read().unwrap().get(&uid) {
            actor.followers.read().unwrap().clone()
        } else {
            RoaringBitmap::new()
        }
    }

    pub fn get_following(&self, uid: u32) -> RoaringBitmap {
        if let Some(actor) = self.actors.read().unwrap().get(&uid) {
            actor.following.read().unwrap().clone()
        } else {
            RoaringBitmap::new()
        }
    }

    pub fn intersect_following_and_followers(&self, actor: u32, target: u32) -> RoaringBitmap {
        BitAnd::bitand(&self.get_following(actor), &self.get_followers(target))
    }

    pub fn does_follow(&self, actor: u32, target: u32) -> bool {
        self.get_following(actor).contains(target)
    }

    pub fn get_followers_not_following(&self, uid: u32) -> RoaringBitmap {
        &self.get_followers(uid) - &self.get_following(uid)
    }

    pub fn acquire_did(&self, did: &str) -> u32 {
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

    pub fn get_usercount(&self) -> u32 {
        *self.next_uid.read().unwrap()
    }

    pub fn get_did(&self, uid: u32) -> Option<String> {
        self.uid_to_did.read().unwrap().get(&uid).cloned()
    }

    pub fn get_uid(&self, did: &str) -> Option<u32> {
        self.did_to_uid.read().unwrap().get(did).cloned()
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
