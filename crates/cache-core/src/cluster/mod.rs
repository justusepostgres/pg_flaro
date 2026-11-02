//! Redis Cluster client routing. PostgreSQL owns replication; no Redis cluster bus.
mod commands;
pub(crate) use commands::command;
mod migration;
mod topology;
use crate::{engine::Engine, protocol::Reply};
use std::{io, sync::mpsc, time::Duration};
pub use topology::{Config, Topology};

pub const SLOTS: usize = 16384;
const CRC16: [u16; 256] = {
    let mut table = [0; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};
pub fn keyslot(key: &[u8]) -> u16 {
    let tag = key
        .iter()
        .position(|b| *b == b'{')
        .and_then(|start| {
            key[start + 1..]
                .iter()
                .position(|b| *b == b'}')
                .filter(|end| *end > 0)
                .map(|end| &key[start + 1..start + 1 + end])
        })
        .unwrap_or(key);
    let mut crc = 0u16;
    for byte in tag {
        crc = (crc << 8) ^ CRC16[((crc >> 8) as u8 ^ *byte) as usize];
    }
    crc & 0x3fff
}

/// The PostgreSQL thread acknowledges only after a synchronous WAL commit.
/// Dropped/late acknowledgement is fatal to the serving thread, never a rollback.
pub struct Save {
    pub json: String,
    pub done: mpsc::SyncSender<()>,
}
pub struct Cluster {
    pub topology: Topology,
    save: mpsc::SyncSender<Save>,
}
impl Cluster {
    pub fn new(topology: Topology, save: mpsc::SyncSender<Save>) -> Self {
        Self { topology, save }
    }
    fn persist(&mut self, topology: Topology) -> io::Result<()> {
        let (done, ack) = mpsc::sync_channel(1);
        self.save
            .try_send(Save {
                json: topology.json(),
                done,
            })
            .map_err(|_| io::Error::other("cluster metadata writer unavailable"))?;
        ack.recv_timeout(Duration::from_secs(30))
            .map_err(|_| io::Error::other("cluster metadata commit uncertain; restart required"))?;
        self.topology = topology;
        Ok(())
    }
    pub(crate) fn route(
        &self,
        args: &[&[u8]],
        asking: bool,
        engine: &Engine,
        now: i64,
    ) -> Option<Reply> {
        let keys = commands::keys(args)?;
        let mut keys = keys.into_iter();
        let first = keys.next()?;
        let slot = keyslot(first);
        let moving = self.topology.migrating.contains_key(&slot);
        let mut present = usize::from(moving && engine.get(first, now).is_some());
        let mut count = 1;
        for key in keys {
            if keyslot(key) != slot {
                return Some(Reply::Error(
                    "CROSSSLOT Keys in request don't hash to the same slot",
                ));
            }
            count += 1;
            present += usize::from(moving && engine.get(key, now).is_some());
        }
        let t = &self.topology;
        if t.pending.as_ref().is_some_and(|p| p.slot == slot) {
            return Some(Reply::Error(
                "TRYAGAIN Slot has an interrupted transfer; resume MIGRATE",
            ));
        }
        let owner = t.owners[slot as usize];
        if owner != t.local {
            if asking && t.importing.get(&slot) == Some(&owner) {
                return None;
            }
            return Some(t.redirect("MOVED", slot, owner));
        }
        if let Some(target) = t.migrating.get(&slot) {
            if present == 0 {
                return Some(t.redirect("ASK", slot, *target));
            }
            if present != count {
                return Some(Reply::Error(
                    "TRYAGAIN Multiple keys request during rehashing of slot",
                ));
            }
        }
        None
    }
}

pub(crate) fn bulk(value: impl AsRef<[u8]>) -> Reply {
    Reply::Bulk(Some(value.as_ref().to_vec()))
}
pub(crate) fn number(value: &[u8]) -> Option<u16> {
    std::str::from_utf8(value).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn redis_hash_tags_and_crc_vectors() {
        assert_eq!(keyslot(b"123456789"), 0x31c3);
        assert_eq!(keyslot(b"foo"), 12182);
        assert_eq!(keyslot(b"bar"), 5061);
        assert_eq!(keyslot(b"a{user}x"), keyslot(b"b{user}y"));
        assert_eq!(keyslot(b"foo{{bar}}zap"), keyslot(b"{bar"));
        assert_ne!(keyslot(b"foo{}{bar}"), keyslot(b"bar"));
        assert_eq!(keyslot(b"\0{\xff}\r\n"), keyslot(b"\xff"));
    }
}

#[cfg(test)]
mod routing_tests {
    use super::topology::Node;
    use super::*;
    fn topology() -> Topology {
        Config {
            local_id: "1".repeat(40),
            nodes: vec![
                Node {
                    id: "1".repeat(40),
                    host: "127.0.0.1".into(),
                    port: 6380,
                    slots: vec![[0, 8191]],
                },
                Node {
                    id: "2".repeat(40),
                    host: "127.0.0.1".into(),
                    port: 6381,
                    slots: vec![[8192, 16383]],
                },
            ],
        }
        .bootstrap()
        .unwrap()
    }
    #[test]
    fn validate_bootstrap_and_recovered_state() {
        let t = topology();
        assert!(Topology::restore(t.config.clone(), &t.json()).is_ok());
        let mut c = t.config.clone();
        c.nodes[0].slots = vec![[0, 8192]];
        assert!(c.bootstrap().is_err());
        c.nodes[0].slots = vec![[0, 8190]];
        assert!(c.bootstrap().is_err());
        c = t.config.clone();
        c.nodes[0].host = "host\r\n+OK".into();
        assert!(c.bootstrap().is_err());
        c = t.config.clone();
        c.nodes[1].id = c.nodes[0].id.clone();
        assert!(c.bootstrap().is_err());
        let mut corrupt = t.clone();
        corrupt.owners.pop();
        assert!(Topology::restore(t.config.clone(), &corrupt.json()).is_err());
        corrupt = t.clone();
        corrupt.importing.insert(5061, 1);
        assert!(Topology::restore(t.config.clone(), &corrupt.json()).is_err());
        let mut migrated = t.clone();
        migrated.owners[5061] = 1;
        assert_eq!(
            Topology::restore(t.config.clone(), &migrated.json())
                .unwrap()
                .owners[5061],
            1
        );
        c = t.config.clone();
        c.nodes[0].port += 1;
        assert!(Topology::restore(c, &t.json()).is_err());
    }
    #[test]
    fn migration_routes_multikey_and_fences_uncertain_transfers() {
        let (tx, _) = mpsc::sync_channel(1);
        let mut c = Cluster::new(topology(), tx);
        let mut e = Engine::new(4096);
        e.execute(&[b"SET", b"{bar}one", b"value"], 0, |_| true);
        assert!(c.route(&[b"GET", b"{bar}one"], false, &e, 0).is_none());
        assert_eq!(
            c.route(&[b"GET", b"foo"], true, &e, 0),
            Some(Reply::OwnedError("MOVED 12182 127.0.0.1:6381".into()))
        );
        assert!(
            matches!(c.route(&[b"MSET", b"bar", b"v", b"foo", b"v"], false, &e, 0), Some(Reply::Error(s)) if s.starts_with("CROSSSLOT"))
        );
        c.topology.migrating.insert(5061, 1);
        assert!(c.route(&[b"GET", b"{bar}one"], false, &e, 0).is_none());
        assert!(
            matches!(c.route(&[b"MGET", b"{bar}one", b"{bar}two"], false, &e, 0), Some(Reply::Error(s)) if s.starts_with("TRYAGAIN"))
        );
        assert_eq!(
            c.route(&[b"SET", b"{bar}two", b"v"], false, &e, 0),
            Some(Reply::OwnedError("ASK 5061 127.0.0.1:6381".into()))
        );
        c.topology.epoch = 1;
        c.topology.pending = Some(super::topology::Transfer {
            token: 1,
            slot: 5061,
            target: 1,
            key: b"{bar}one".to_vec(),
        });
        assert!(Topology::restore(c.topology.config.clone(), &c.topology.json()).is_ok());
        assert!(
            matches!(c.route(&[b"GET", b"{bar}one"], false, &e, 0), Some(Reply::Error(s)) if s.starts_with("TRYAGAIN"))
        );
        assert!(c.route(&[b"GET", b"another-slot"], false, &e, 0).is_none());
    }
}
