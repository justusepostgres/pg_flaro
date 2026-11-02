use super::{bulk, SLOTS};
use crate::protocol::Reply;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    pub id: String,
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub slots: Vec<[u16; 2]>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub local_id: String,
    pub nodes: Vec<Node>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topology {
    pub pending: Option<Transfer>,
    pub imported: BTreeMap<u16, u64>,
    pub config: Config,
    pub local: u16,
    pub owners: Vec<u16>,
    pub migrating: BTreeMap<u16, u16>,
    pub importing: BTreeMap<u16, u16>,
    pub epoch: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transfer {
    pub token: u64,
    pub slot: u16,
    pub target: u16,
    pub key: Vec<u8>,
}
impl Config {
    pub fn parse(json: &str) -> Result<Self, String> {
        if json.len() > 1024 * 1024 {
            return Err("cluster configuration too large".into());
        }
        let config: Self = serde_json::from_str(json).map_err(|e| e.to_string())?;
        config.bootstrap()?;
        Ok(config)
    }
    pub fn bootstrap(&self) -> Result<Topology, String> {
        if self.nodes.is_empty() || self.nodes.len() > 64 {
            return Err("expected 1..64 shards".into());
        }
        let mut ids = HashSet::new();
        let mut endpoints = HashSet::new();
        let mut owners = vec![u16::MAX; SLOTS];
        for (index, node) in self.nodes.iter().enumerate() {
            if node.id.len() != 40
                || !node
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                || !ids.insert(&node.id)
            {
                return Err("node IDs must be unique 40-character lowercase hex strings".into());
            }
            // Accept DNS/IPv4 and unbracketed IPv6; prohibit RESP/text injection.
            if node.host.is_empty()
                || node.host.len() > 253
                || !node
                    .host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".-_:".contains(&b))
                || node.port == 0
                || !endpoints.insert((&node.host, node.port))
            {
                return Err("invalid or duplicate advertised endpoint".into());
            }
            for [start, end] in &node.slots {
                if start > end || *end as usize >= SLOTS {
                    return Err("invalid slot range".into());
                }
                for slot in *start..=*end {
                    if owners[slot as usize] != u16::MAX {
                        return Err("overlapping slot ranges".into());
                    }
                    owners[slot as usize] = index as u16;
                }
            }
        }
        if owners.contains(&u16::MAX) {
            return Err("all 16384 slots must be assigned".into());
        }
        let local = self
            .nodes
            .iter()
            .position(|n| n.id == self.local_id)
            .ok_or("local_id not in nodes")? as u16;
        Ok(Topology {
            pending: None,
            imported: BTreeMap::new(),
            config: self.clone(),
            local,
            owners,
            migrating: BTreeMap::new(),
            importing: BTreeMap::new(),
            epoch: 0,
        })
    }
}
impl Topology {
    pub fn restore(config: Config, json: &str) -> Result<Self, String> {
        config.bootstrap()?;
        let t: Self = serde_json::from_str(json).map_err(|e| e.to_string())?;
        if t.config != config
            || t.local as usize >= config.nodes.len()
            || config.nodes[t.local as usize].id != config.local_id
        {
            return Err("persisted topology differs from bootstrap configuration; membership changes require an explicit upgrade".into());
        }
        if t.owners.len() != SLOTS || t.owners.iter().any(|n| *n as usize >= config.nodes.len()) {
            return Err("invalid persisted slot owners".into());
        }
        for (states, importing) in [(&t.migrating, false), (&t.importing, true)] {
            for (&slot, &peer) in states {
                if slot as usize >= SLOTS
                    || peer as usize >= config.nodes.len()
                    || peer == t.local
                    || if importing {
                        t.owners[slot as usize] != peer || t.migrating.contains_key(&slot)
                    } else {
                        t.owners[slot as usize] != t.local
                    }
                {
                    return Err("invalid persisted migration state".into());
                }
            }
        }
        if t.imported.iter().any(|(source, token)| {
            *source as usize >= config.nodes.len() || *source == t.local || *token == 0
        }) {
            return Err("invalid import replay watermark".into());
        }
        if let Some(p) = &t.pending {
            if p.token == 0
                || p.token > t.epoch
                || p.key.len() > crate::protocol::MAX_FRAME
                || super::keyslot(&p.key) != p.slot
                || t.migrating.get(&p.slot) != Some(&p.target)
            {
                return Err("invalid persisted transfer intent".into());
            }
        }
        Ok(t)
    }
    pub fn json(&self) -> String {
        serde_json::to_string(self).expect("topology serialization")
    }
    pub(crate) fn node_index(&self, id: &[u8]) -> Option<u16> {
        self.config
            .nodes
            .iter()
            .position(|n| n.id.as_bytes() == id)
            .map(|i| i as u16)
    }
    pub(crate) fn endpoint(&self, node: u16) -> String {
        let n = &self.config.nodes[node as usize];
        if n.host.contains(':') {
            format!("[{}]:{}", n.host, n.port)
        } else {
            format!("{}:{}", n.host, n.port)
        }
    }
    pub(crate) fn redirect(&self, kind: &str, slot: u16, node: u16) -> Reply {
        Reply::OwnedError(format!("{kind} {slot} {}", self.endpoint(node)))
    }
    pub(crate) fn ranges(&self, node: u16) -> Vec<(u16, u16)> {
        let mut ranges = Vec::new();
        let mut slot = 0;
        while slot < SLOTS {
            if self.owners[slot] != node {
                slot += 1;
                continue;
            }
            let start = slot;
            while slot + 1 < SLOTS && self.owners[slot + 1] == node {
                slot += 1;
            }
            ranges.push((start as u16, slot as u16));
            slot += 1;
        }
        ranges
    }
    pub(crate) fn slots(&self) -> Reply {
        Reply::Array(
            self.config
                .nodes
                .iter()
                .enumerate()
                .flat_map(|(i, n)| {
                    self.ranges(i as u16).into_iter().map(move |(start, end)| {
                        Reply::Array(vec![
                            Reply::Integer(start as i64),
                            Reply::Integer(end as i64),
                            Reply::Array(vec![
                                bulk(&n.host),
                                Reply::Integer(n.port as i64),
                                bulk(&n.id),
                            ]),
                        ])
                    })
                })
                .collect(),
        )
    }
    pub(crate) fn shards(&self) -> Reply {
        Reply::Array(
            self.config
                .nodes
                .iter()
                .enumerate()
                .map(|(index, n)| {
                    let slots = self
                        .ranges(index as u16)
                        .into_iter()
                        .flat_map(|(a, b)| [Reply::Integer(a as i64), Reply::Integer(b as i64)])
                        .collect();
                    Reply::Array(vec![
                        bulk("slots"),
                        Reply::Array(slots),
                        bulk("nodes"),
                        Reply::Array(vec![Reply::Array(vec![
                            bulk("id"),
                            bulk(&n.id),
                            bulk("endpoint"),
                            bulk(&n.host),
                            bulk("ip"),
                            bulk(&n.host),
                            bulk("port"),
                            Reply::Integer(n.port as i64),
                            bulk("role"),
                            bulk("master"),
                            bulk("replication-offset"),
                            Reply::Integer(0),
                            bulk("health"),
                            bulk("online"),
                        ])]),
                    ])
                })
                .collect(),
        )
    }
    pub(crate) fn nodes(&self) -> Reply {
        let mut lines = String::new();
        for (i, node) in self.config.nodes.iter().enumerate() {
            let mut line = format!(
                "{} {}@0 {}master - 0 0 {} connected",
                node.id,
                self.endpoint(i as u16),
                if i as u16 == self.local {
                    "myself,"
                } else {
                    ""
                },
                self.epoch
            );
            for (start, end) in self.ranges(i as u16) {
                line.push_str(&if start == end {
                    format!(" {start}")
                } else {
                    format!(" {start}-{end}")
                });
            }
            if i as u16 == self.local {
                for (slot, peer) in &self.migrating {
                    line.push_str(&format!(
                        " [{slot}->-{}]",
                        self.config.nodes[*peer as usize].id
                    ));
                }
                for (slot, peer) in &self.importing {
                    line.push_str(&format!(
                        " [{slot}-<-{}]",
                        self.config.nodes[*peer as usize].id
                    ));
                }
            }
            lines.push_str(&line);
            lines.push('\n');
        }
        bulk(lines)
    }
}
