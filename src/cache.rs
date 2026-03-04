use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, RwLock};

#[derive(Clone)]
struct CacheEntry<V> {
    value: V,
    expires_at: Instant,
}

#[derive(Clone)]
pub struct TtlCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    ttl: Duration,
    entries: Arc<RwLock<HashMap<K, CacheEntry<V>>>>,
    key_locks: Arc<Mutex<HashMap<K, Arc<Mutex<()>>>>>,
}

impl<K, V> TtlCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Arc::new(RwLock::new(HashMap::new())),
            key_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_or_try_insert_with<E, F, Fut>(&self, key: K, compute: F) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
    {
        if let Some(value) = self.get_if_fresh(&key).await {
            return Ok(value);
        }

        let key_lock = {
            let mut locks = self.key_locks.lock().await;
            locks
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };

        let _guard = key_lock.lock().await;

        if let Some(value) = self.get_if_fresh(&key).await {
            return Ok(value);
        }

        let computed = compute().await?;
        self.insert(key, computed.clone()).await;
        Ok(computed)
    }

    async fn get_if_fresh(&self, key: &K) -> Option<V> {
        let now = Instant::now();
        let entries = self.entries.read().await;
        entries
            .get(key)
            .filter(|entry| entry.expires_at > now)
            .map(|entry| entry.value.clone())
    }

    async fn insert(&self, key: K, value: V) {
        let entry = CacheEntry {
            value,
            expires_at: Instant::now() + self.ttl,
        };
        let mut entries = self.entries.write().await;
        entries.insert(key, entry);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::task;

    use super::TtlCache;

    #[tokio::test]
    async fn test_single_flight_behavior() {
        let cache = Arc::new(TtlCache::<String, usize>::new(Duration::from_secs(60)));
        let calls = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let calls = calls.clone();
            handles.push(task::spawn(async move {
                cache
                    .get_or_try_insert_with("key".to_string(), || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<usize, ()>(42)
                    })
                    .await
            }));
        }

        for handle in handles {
            assert_eq!(handle.await.expect("join").expect("value"), 42);
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
