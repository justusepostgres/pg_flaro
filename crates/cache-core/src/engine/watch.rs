//! Key-local WATCH versions. Transaction rollback restores touched versions,
//! so failed admission cannot invalidate another connection's watches.
use super::{Bytes, Engine};
use std::sync::{Arc, Weak};
#[derive(Clone)]
pub(super) struct Version {
    revision: u64,
    observed_at: i64,
    life: Weak<()>,
}
#[derive(Clone)]
pub(crate) struct Watch {
    revision: u64,
    expires: Option<i64>,
    _life: Arc<()>,
}
impl Engine {
    pub(crate) fn watch(&mut self, key: &[u8], now: i64) -> Watch {
        self.expire_hash_fields(now);
        let expires = self.get(key, now).and_then(|entry| {
            entry
                .expires_at_ms
                .into_iter()
                .chain(entry.value.hash_expiration())
                .min()
        });
        let version = self
            .watches
            .entry(Bytes::from(key))
            .or_insert_with(|| Version {
                revision: 0,
                observed_at: now,
                life: Weak::new(),
            });
        version.observed_at = version.observed_at.max(now);
        let life = version.life.upgrade().unwrap_or_else(|| {
            let life = Arc::new(());
            version.life = Arc::downgrade(&life);
            life
        });
        Watch {
            revision: version.revision,
            expires,
            _life: life,
        }
    }
    pub(crate) fn watch_dirty(&self, key: &[u8], watch: &Watch, now: i64) -> bool {
        watch.expires.is_some_and(|expiry| expiry <= now)
            || self
                .watches
                .get(key)
                .is_none_or(|v| v.revision != watch.revision)
    }
    pub(super) fn touch_watch(&mut self, key: &[u8]) {
        self.record_change(key);
        if let Some(version) = self.watches.get_mut(key) {
            version.revision = version
                .revision
                .checked_add(1)
                .expect("WATCH revision exhausted");
        }
    }
    pub(super) fn touch_watch_deletion(&mut self, key: &[u8]) {
        self.record_change(key);
        let Some(entry) = self.entries.get(key) else {
            return;
        };
        if self.watches.get(key).is_some_and(|version| {
            entry
                .expires_at_ms
                .is_some_and(|expiry| expiry <= version.observed_at)
        }) {
            // Earlier watchers already fail their captured expiration check;
            // watchers registered after expiration observed an absent key.
            return;
        }
        self.touch_watch(key);
    }
    pub(super) fn prune_watches(&mut self) {
        self.watches.retain(|_, v| v.life.strong_count() > 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn aba_and_same_value_writes_invalidate_but_rejected_admission_does_not() {
        let mut e = Engine::new(1024 * 1024);
        e.execute(&[b"SET", b"k", b"old"], 0, |_| true);
        let first = e.watch(b"k", 0);
        e.execute(&[b"SET", b"k", b"old"], 1, |_| false);
        assert!(!e.watch_dirty(b"k", &first, 1));
        e.execute(&[b"SET", b"k", b"old"], 1, |_| true);
        assert!(e.watch_dirty(b"k", &first, 1));
        let second = e.watch(b"k", 1);
        e.execute(&[b"SET", b"k", b"new"], 2, |_| true);
        e.execute(&[b"SET", b"k", b"old"], 2, |_| true);
        assert!(e.watch_dirty(b"k", &second, 2));
    }
    #[test]
    fn expiry_after_watch_aborts_but_already_expired_keys_and_reaping_do_not() {
        let mut e = Engine::new(1024 * 1024);
        e.execute(&[b"SET", b"k", b"v", b"PX", b"10"], 0, |_| true);
        let before = e.watch(b"k", 1);
        let after = e.watch(b"k", 11);
        e.execute(&[b"SORT", b"missing", b"STORE", b"k"], 11, |_| true);
        e.reap(11);
        assert!(e.watch_dirty(b"k", &before, 11));
        assert!(!e.watch_dirty(b"k", &after, 11));
        drop(before);
        drop(after);
        e.prune_watches();
        assert!(e.watches.is_empty());
    }
    #[test]
    fn speculative_versions_publish_only_after_transaction_admission() {
        let mut e = Engine::new(1024 * 1024);
        let token = e.watch(b"k", 0);
        for accepted in [false, true] {
            let result = e.transact(
                |staged, submit| vec![staged.execute(&[b"SET", b"k", b"v"], 1, submit)],
                |_| accepted,
            );
            assert_eq!(result.is_ok(), accepted);
            assert_eq!(e.watch_dirty(b"k", &token, 1), accepted);
        }
    }
}
