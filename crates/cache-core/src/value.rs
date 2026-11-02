//! Typed immutable values with a versioned binary representation for persistence.
use crate::engine::Bytes;
use im::{OrdSet, Vector};
use std::collections::{HashMap, HashSet};
// Reserve room for sparsely populated persistent-tree roots and vector chunks.
pub(crate) const COLLECTION_BASE_MEMORY: usize = 8192;
use std::{
    cmp::Ordering,
    ops::Deref,
    sync::{Arc, OnceLock},
};
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Score(pub f64);
impl Eq for Score {}
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SortedSet {
    pub scores: HashMap<Bytes, Score>,
    pub order: OrdSet<(Score, Bytes)>,
}
impl SortedSet {
    pub fn insert(&mut self, member: Bytes, score: f64) {
        let score = Score(if score == 0.0 { 0.0 } else { score });
        if let Some(old) = self.scores.insert(member.clone(), score) {
            self.order.remove(&(old, member.clone()));
        }
        self.order.insert((score, member));
    }
    pub fn remove(&mut self, member: &[u8]) -> bool {
        if let Some((member, score)) = self.scores.remove_entry(member) {
            self.order.remove(&(score, member));
            true
        } else {
            false
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Collection {
    Hash(HashMap<Bytes, Bytes>),
    List(Vector<Bytes>),
    Set(HashSet<Bytes>),
    ZSet(SortedSet),
}
impl Collection {
    pub fn empty(kind: u8) -> Self {
        match kind {
            1 => Self::Hash(HashMap::new()),
            2 => Self::List(Vector::new()),
            3 => Self::Set(HashSet::new()),
            4 => Self::ZSet(SortedSet::default()),
            _ => unreachable!(),
        }
    }
    pub fn kind(&self) -> u8 {
        match self {
            Self::Hash(_) => 1,
            Self::List(_) => 2,
            Self::Set(_) => 3,
            Self::ZSet(_) => 4,
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Self::Hash(v) => v.len(),
            Self::List(v) => v.len(),
            Self::Set(v) => v.len(),
            Self::ZSet(v) => v.scores.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
#[derive(Clone, Debug)]
pub enum Value {
    String(Bytes),
    Collection {
        data: Arc<Collection>,
        encoded: Arc<OnceLock<Bytes>>,
        encoded_len: usize,
        memory: usize,
    },
}
impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Self::String(v.into())
    }
}
impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Self {
        Self::String(v.into())
    }
}
impl From<Bytes> for Value {
    fn from(v: Bytes) -> Self {
        Self::String(v)
    }
}
impl Deref for Value {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::String(v) => v,
            Self::Collection { data, encoded, .. } => encoded.get_or_init(|| encode(data)),
        }
    }
}
impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self
    }
}
impl Value {
    pub fn kind(&self) -> u8 {
        match self {
            Self::String(_) => 0,
            Self::Collection { data, .. } => data.kind(),
        }
    }
    pub fn type_name(&self) -> &'static str {
        ["string", "hash", "list", "set", "zset"][self.kind() as usize]
    }
    pub fn collection(&self) -> Option<&Collection> {
        match self {
            Self::Collection { data, .. } => Some(data),
            _ => None,
        }
    }
    pub fn memory_bytes(&self) -> usize {
        match self {
            Self::String(v) => v.len(),
            Self::Collection { memory, .. } => *memory,
        }
    }
    pub fn from_collection(data: Collection) -> Result<Self, &'static str> {
        let bytes = match &data {
            Collection::Hash(v) => v.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>(),
            Collection::List(v) => v.iter().map(|v| v.len()).sum(),
            Collection::Set(v) => v.iter().map(|v| v.len()).sum(),
            Collection::ZSet(v) => v.scores.keys().map(|v| v.len() + 8).sum(),
        };
        Self::with_payload_size(data, bytes)
    }
    /// Reuse snapshot size metadata by inspecting only fields touched by a command.
    pub(crate) fn from_updated(
        data: Collection,
        old: Option<&Value>,
        args: &[&[u8]],
    ) -> Result<Self, &'static str> {
        let Some(old) = old else {
            return Self::from_collection(data);
        };
        let Some(previous) = old.collection() else {
            return Self::from_collection(data);
        };
        let fields = if matches!(previous, Collection::Hash(_) | Collection::ZSet(_)) {
            2
        } else {
            1
        };
        let mut bytes = (old.len() - 8 - previous.len() * fields * 4) as isize;
        match (previous, &data) {
            (Collection::Hash(a), Collection::Hash(b)) => {
                let step = if args[0].eq_ignore_ascii_case(b"HSET")
                    || args[0].eq_ignore_ascii_case(b"HMSET")
                {
                    2
                } else if args[0].eq_ignore_ascii_case(b"HDEL") {
                    1
                } else {
                    args.len()
                };
                let mut seen = std::collections::HashSet::new();
                for key in args[2..].iter().step_by(step) {
                    if !seen.insert(*key) {
                        continue;
                    }
                    bytes -= a.get(*key).map_or(0, |v| key.len() + v.len()) as isize;
                    bytes += b.get(*key).map_or(0, |v| key.len() + v.len()) as isize;
                }
            }
            (Collection::Set(a), Collection::Set(b)) => {
                let mut seen = std::collections::HashSet::new();
                for key in &args[2..] {
                    if !seen.insert(*key) {
                        continue;
                    }
                    bytes -= if a.contains(*key) {
                        key.len() as isize
                    } else {
                        0
                    };
                    bytes += if b.contains(*key) {
                        key.len() as isize
                    } else {
                        0
                    };
                }
            }
            (Collection::ZSet(a), Collection::ZSet(_))
                if args[0].eq_ignore_ascii_case(b"ZINCRBY") =>
            {
                if !a.scores.contains_key(args[3]) {
                    bytes += (args[3].len() + 8) as isize;
                }
            }
            (Collection::List(a), Collection::List(b)) if args[0].eq_ignore_ascii_case(b"LSET") => {
                let index = std::str::from_utf8(args[2])
                    .unwrap()
                    .parse::<i64>()
                    .unwrap();
                let index = if index < 0 {
                    a.len() as i64 + index
                } else {
                    index
                } as usize;
                bytes += b[index].len() as isize - a[index].len() as isize;
            }
            _ => return Self::from_collection(data),
        }
        Self::with_payload_size(
            data,
            usize::try_from(bytes).expect("collection size underflow"),
        )
    }
    fn with_payload_size(data: Collection, bytes: usize) -> Result<Self, &'static str> {
        if data.is_empty() {
            return Err("empty collections must delete their key");
        }
        let fields = if matches!(data, Collection::Hash(_) | Collection::ZSet(_)) {
            2
        } else {
            1
        };
        let encoded_len = 8 + bytes + data.len() * fields * 4;
        if encoded_len > crate::protocol::MAX_FRAME {
            return Err("collection exceeds maximum encoded size");
        }
        let memory = encoded_len + bytes + data.len() * 160 + COLLECTION_BASE_MEMORY;
        Ok(Self::Collection {
            memory,
            encoded_len,
            data: Arc::new(data),
            encoded: Arc::new(OnceLock::new()),
        })
    }
    /// Encoded size without materializing a persistence snapshot on the network thread.
    pub fn len(&self) -> usize {
        match self {
            Self::String(v) => v.len(),
            Self::Collection { encoded_len, .. } => *encoded_len,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn from_stored(kind: u8, bytes: &[u8]) -> Result<Self, &'static str> {
        if kind == 0 {
            return Ok(bytes.into());
        }
        if !(1..=4).contains(&kind)
            || bytes.len() > crate::protocol::MAX_FRAME
            || bytes.get(..4) != Some(b"COL1")
        {
            return Err("invalid collection header");
        }
        let count = u32::from_le_bytes(
            bytes
                .get(4..8)
                .ok_or("truncated collection")?
                .try_into()
                .unwrap(),
        ) as usize;
        if count == 0 || count > bytes.len() / 4 {
            return Err("invalid collection count");
        }
        let mut pos = 8;
        let mut field = || -> Result<Bytes, &'static str> {
            let n = u32::from_le_bytes(
                bytes
                    .get(pos..pos + 4)
                    .ok_or("truncated field length")?
                    .try_into()
                    .unwrap(),
            ) as usize;
            pos += 4;
            let end = pos.checked_add(n).ok_or("field overflow")?;
            let result = bytes.get(pos..end).ok_or("truncated field")?.into();
            pos = end;
            Ok(result)
        };
        let mut data = Collection::empty(kind);
        for _ in 0..count {
            match &mut data {
                Collection::Hash(v) => {
                    let k = field()?;
                    let value = field()?;
                    if v.insert(k, value).is_some() {
                        return Err("duplicate hash field");
                    }
                }
                Collection::List(v) => v.push_back(field()?),
                Collection::Set(v) => {
                    if !v.insert(field()?) {
                        return Err("duplicate set member");
                    }
                }
                Collection::ZSet(v) => {
                    let member = field()?;
                    let raw = field()?;
                    let score = f64::from_le_bytes(
                        raw.as_ref()
                            .try_into()
                            .map_err(|_| "invalid score encoding")?,
                    );
                    if score.is_nan() || v.scores.contains_key(&member) {
                        return Err("invalid sorted set member");
                    }
                    v.insert(member, score);
                }
            }
        }
        if pos != bytes.len() {
            return Err("trailing collection data");
        }
        Self::from_collection(data)
    }
}

fn encode(data: &Collection) -> Bytes {
    let mut encoded = b"COL1".to_vec();
    encoded.extend_from_slice(&(data.len() as u32).to_le_bytes());
    let mut field = |v: &[u8]| {
        encoded.extend_from_slice(&(v.len() as u32).to_le_bytes());
        encoded.extend_from_slice(v);
    };
    match data {
        Collection::Hash(v) => {
            for (k, v) in v {
                field(k);
                field(v);
            }
        }
        Collection::List(v) => {
            for v in v {
                field(v);
            }
        }
        Collection::Set(v) => {
            for v in v {
                field(v);
            }
        }
        Collection::ZSet(v) => {
            for (m, s) in &v.scores {
                field(m);
                field(&s.0.to_le_bytes());
            }
        }
    }
    encoded.into()
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Collection { data: a, .. }, Self::Collection { data: b, .. }) => a == b,
            _ => false,
        }
    }
}
impl Eq for Value {}
