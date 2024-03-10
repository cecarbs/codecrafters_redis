use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(Debug)]
struct TimedValue<T> {
    value: T,
    expiration: Option<Instant>,
}

impl<T> TimedValue<T> {
    fn add(value: T, ttl: Option<Duration>) -> Self {
        let duration = if ttl.is_none() {
            None
        } else {
            let ttl = ttl.unwrap();
            Some(Instant::now() + ttl)
        };

        if duration.is_some() {
            println!("Duration is: {:?}", duration.unwrap());
        }

        Self {
            value,
            expiration: duration,
        }
    }

    fn is_expired(&self) -> bool {
        if self.expiration.is_some() {
            let expiration = self.expiration.unwrap();
            println!(
                "Expiration is: {:?} and current time is: {:?}",
                expiration,
                Instant::now()
            );
            self.expiration.unwrap() > Instant::now()
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct TimedHashMap<K, V> {
    map: HashMap<K, TimedValue<V>>,
}

impl<K, V> TimedHashMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: V, ttl: Option<Duration>) {
        let timed_value: TimedValue<V> = TimedValue::add(value, ttl);
        self.map.insert(key, timed_value);
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key).and_then(|timed_value| {
            if timed_value.is_expired() {
                None
            } else {
                Some(&timed_value.value)
            }
        })
    }

    pub fn remove_expired_entries(&mut self) {
        self.map.retain(|_, timed_value| !timed_value.is_expired());
    }
}
