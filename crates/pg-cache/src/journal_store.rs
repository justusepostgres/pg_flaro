//! Experimental transaction adapter. SQL wrappers exist only with journal-prototype.
//! No function commits: the caller owns its transaction. Experimental RESP mode
//! uses this adapter through journal_worker. All journal tables are ordinary WAL-logged relations.
use pg_cache_core::{
    engine::{Bytes, Entry, Mutation},
    journal::{
        decode,
        replay::Replay,
        rotation::{Segment, State},
    },
    now_ms,
};
use pgrx::prelude::*;
use std::collections::HashMap;

type Changes = HashMap<Bytes, Option<Entry>>;
const TABLES: [&str; 2] = ["cache_journal.segment0", "cache_journal.segment1"];

fn state(lock: &str) -> (State, u64) {
    assert!(matches!(lock, "" | "FOR UPDATE" | "FOR SHARE"));
    let query = format!("SELECT ARRAY[materialized,committed,active,previous0,sequence0,bytes0,previous1,sequence1,bytes1,segment_limit] FROM cache_journal.state WHERE singleton {lock}");
    let values = Spi::connect_mut(|client| {
        client
            .update(&query, None, &[])
            .expect("read journal state")
            .first()
            .get_one::<Vec<i64>>()
            .expect("state columns")
            .expect("journal state row")
    });
    assert_eq!(values.len(), 10);
    assert!(values.iter().all(|n| *n >= 0));
    let value = |i| values[i] as u64;
    let state = State {
        materialized: value(0),
        committed: value(1),
        active: value(2) as usize,
        segments: [
            Segment {
                previous: value(3),
                sequence: value(4),
                bytes: value(5),
            },
            Segment {
                previous: value(6),
                sequence: value(7),
                bytes: value(8),
            },
        ],
    };
    state.validate(value(9)).expect("valid journal metadata");
    (state, value(9))
}

fn save(state: State) {
    let s = state.segments;
    let values = vec![
        state.materialized as i64,
        state.committed as i64,
        state.active as i64,
        s[0].previous as i64,
        s[0].sequence as i64,
        s[0].bytes as i64,
        s[1].previous as i64,
        s[1].sequence as i64,
        s[1].bytes as i64,
    ];
    Spi::run_with_args("UPDATE cache_journal.state SET materialized=($1::bigint[])[1],committed=($1::bigint[])[2],active=($1::bigint[])[3],previous0=($1::bigint[])[4],sequence0=($1::bigint[])[5],bytes0=($1::bigint[])[6],previous1=($1::bigint[])[7],sequence1=($1::bigint[])[8],bytes1=($1::bigint[])[9] WHERE singleton", &[values.into()]).expect("save journal metadata");
}

fn durable() {
    Spi::run("SET LOCAL synchronous_commit=on").expect("journal durability");
}

/// Prepared once before retries. SQL input is decoded and validated; the worker
/// constructs this directly from the same mutations validated by the encoder.
pub(crate) struct AppendBatch {
    payload: Vec<u8>,
    bytes: u64,
}
impl AppendBatch {
    pub(crate) fn new(mutations: &[Mutation]) -> Self {
        assert!(!mutations.is_empty());
        Self {
            payload: pg_cache_core::journal::encode(mutations).expect("encode journal batch"),
            bytes: mutations.iter().map(Mutation::journal_bytes).sum::<usize>() as u64,
        }
    }
}
#[derive(Default)]
pub(crate) struct AppendTiming {
    pub lock_us: u64,
    pub insert_us: u64,
    pub save_us: u64,
}

#[pg_extern]
pub(crate) fn journal_proto_append(sequence: i64, payload: Vec<u8>) -> bool {
    let mutations = decode(&payload).expect("valid journal batch");
    assert!(!mutations.is_empty());
    let bytes = mutations.iter().map(Mutation::journal_bytes).sum::<usize>() as u64;
    append(
        sequence,
        &[AppendBatch { payload, bytes }],
        &mut AppendTiming::default(),
    )
}

#[pg_extern]
fn journal_proto_append_records(sequence: i64, first: Vec<u8>, second: Vec<u8>) -> bool {
    let payloads = [first, second];
    let batches: Vec<_> = payloads
        .into_iter()
        .map(|payload| {
            let mutations = decode(&payload).expect("valid journal record");
            assert!(!mutations.is_empty());
            let bytes = mutations.iter().map(Mutation::journal_bytes).sum::<usize>() as u64;
            AppendBatch { payload, bytes }
        })
        .collect();
    append(sequence, &batches, &mut AppendTiming::default())
}

pub(crate) fn append(sequence: i64, batches: &[AppendBatch], timing: &mut AppendTiming) -> bool {
    let started = std::time::Instant::now();
    durable();
    let (current, limit) = state("FOR UPDATE");
    timing.lock_us = started.elapsed().as_micros() as u64;
    assert!(
        sequence > current.committed as i64,
        "journal sequence regression"
    );
    assert!(!batches.is_empty());
    assert!(
        batches.iter().map(|batch| batch.bytes).sum::<u64>() <= limit,
        "snapshot exceeds segment capacity"
    );
    assert!(
        sequence as u64 - current.committed >= batches.len() as u64,
        "insufficient snapshot sequence range"
    );
    let first_sequence = sequence as u64 - batches.len() as u64 + 1;
    let mut next = current;
    let mut plans = Vec::with_capacity(batches.len());
    // Plan every record before writing any: full capacity is a clean retry.
    for (index, batch) in batches.iter().enumerate() {
        let previous = next.committed;
        let Ok(planned) = next.append(first_sequence + index as u64, batch.bytes, limit) else {
            return false;
        };
        plans.push((planned.active, planned.committed, previous));
        next = planned;
    }
    let started = std::time::Instant::now();
    for (batch, (slot, sequence, previous)) in batches.iter().zip(plans) {
        crate::journal_copy::insert(slot, sequence as i64, previous as i64, &batch.payload);
    }
    timing.insert_us = started.elapsed().as_micros() as u64;
    let started = std::time::Instant::now();
    save(next);
    timing.save_us = started.elapsed().as_micros() as u64;
    true
}

#[pg_extern]
pub(crate) fn journal_proto_rotate() -> bool {
    durable();
    let (current, limit) = state("FOR UPDATE");
    let Ok(next) = current.rotate(limit) else {
        return false;
    };
    save(next);
    true
}

fn read_changes(state: State, limit: u64) -> Changes {
    let mut replay = Replay::new(state, limit).expect("journal replay state");
    let mut changes = Changes::new();
    // Read only nonempty segments. A compactor must not acquire a table lock on
    // the concurrently active segment while scanning its immutable sealed slot.
    let slots = [1 - state.active, state.active];
    for slot in slots {
        if state.segments[slot].bytes == 0 {
            continue;
        }
        let query = format!(
            "SELECT previous,sequence,part,parts,payload FROM {} ORDER BY sequence,part",
            TABLES[slot]
        );
        // pgrx retains tuple tables until SPI disconnect. Detach the portal
        // between fetches so each connection releases its consumed row storage.
        let cursor_name = Spi::connect(|client| client.open_cursor(&query, &[]).detach_into_name());
        let mut chunks = pg_cache_core::journal::chunks::Chunks::default();
        loop {
            let more = Spi::connect(|client| {
                let mut cursor = client.find_cursor(&cursor_name).expect("journal portal");
                let rows = cursor.fetch(256).expect("journal cursor");
                if rows.is_empty() {
                    return false; // Dropping the cursor closes the portal.
                }
                for row in rows {
                    let previous = row.get::<i64>(1).unwrap().unwrap();
                    let sequence = row.get::<i64>(2).unwrap().unwrap();
                    assert!(previous >= 0 && sequence > previous);
                    let part = row.get::<i32>(3).unwrap().unwrap();
                    let parts = row.get::<i32>(4).unwrap().unwrap();
                    let bytes = row.get::<Vec<u8>>(5).unwrap().unwrap();
                    let Some((previous, sequence, payload)) = chunks
                        .push(previous as u64, sequence as u64, part, parts, &bytes)
                        .expect("complete journal chunks")
                    else {
                        continue;
                    };
                    for mutation in replay
                        .push(slot, previous, sequence, &payload)
                        .expect("complete journal row")
                    {
                        changes.insert(mutation.key, mutation.entry);
                    }
                }
                cursor.detach_into_name();
                true
            });
            if !more {
                break;
            }
        }
        chunks.finish().expect("complete final journal block");
    }
    replay.finish().expect("complete journal prefix");
    changes
}

fn apply(changes: &Changes) {
    // Visit rows in key order instead of hash-table order for heap locality. The
    // segment already bounds changes; this adds only a vector of borrowed pairs,
    // without duplicating key/value payloads or changing the transaction boundary.
    let mut ordered: Vec<_> = changes.iter().collect();
    ordered.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    // Bound executor tuplestores and parameter allocations. All statements stay
    // in the caller's single compaction transaction; no watermark moves early.
    let mut batch = Vec::new();
    let mut bytes = 0;
    for (key, entry) in ordered {
        let cost = key.len() + entry.as_ref().map_or(0, |entry| entry.value.len()) + 128;
        if !batch.is_empty() && (batch.len() >= 512 || bytes + cost > 1024 * 1024) {
            apply_batch(&batch);
            batch.clear();
            bytes = 0;
        }
        batch.push((key, entry));
        bytes += cost;
    }
    if !batch.is_empty() {
        apply_batch(&batch);
    }
}

fn apply_batch(batch: &[(&Bytes, &Option<Entry>)]) {
    // Construct PostgreSQL parameter datums inside this SPI context so they are
    // released after each bounded statement group, rather than at transaction end.
    Spi::connect_mut(|client| {
        let (mut keys, mut values, mut expirations, mut deletes, mut types) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (key, entry) in batch {
            if let Some(entry) = entry {
                keys.push(key.as_ref());
                values.push(entry.value.as_ref());
                types.push(entry.value.kind() as i16);
                expirations.push(entry.expires_at_ms);
            } else {
                deletes.push(key.as_ref());
            }
        }
        if !keys.is_empty() {
            client.update("MERGE INTO cache_journal.entries AS target USING (SELECT * FROM unnest($1::bytea[],$2::bytea[],$3::bigint[],$4::smallint[])) AS source(key,value,expires_at_ms,data_type) ON target.key=source.key WHEN MATCHED THEN UPDATE SET value=source.value,expires_at_ms=source.expires_at_ms,data_type=source.data_type WHEN NOT MATCHED THEN INSERT(key,value,expires_at_ms,data_type) VALUES(source.key,source.value,source.expires_at_ms,source.data_type)", None, &[keys.into(),values.into(),expirations.into(),types.into()]).expect("materialize journal values");
        }
        if !deletes.is_empty() {
            client
                .update(
                    "DELETE FROM cache_journal.entries WHERE key=ANY($1::bytea[])",
                    None,
                    &[deletes.into()],
                )
                .expect("materialize journal deletions");
        }
    });
}

#[pg_extern]
pub(crate) fn journal_proto_compact() -> bool {
    durable();
    // Only compactors take this transaction-scoped lock. The writer does not.
    if !Spi::get_one::<bool>("SELECT pg_try_advisory_xact_lock(62400001)")
        .unwrap()
        .unwrap()
    {
        return false;
    }
    let (before, limit) = state("");
    let Some(ticket) = before.sealed() else {
        return false;
    };
    let mut prefix = State {
        materialized: before.materialized,
        committed: ticket.segment.sequence,
        active: ticket.slot,
        segments: [Segment {
            previous: before.materialized,
            sequence: before.materialized,
            bytes: 0,
        }; 2],
    };
    prefix.segments[ticket.slot] = ticket.segment;
    let changes = read_changes(prefix, limit);
    // Hash indexes recheck full keys but do not enforce uniqueness. Our advisory
    // lock serializes the entire MERGE read/modify/insert decision across all
    // compactors. Direct SQL writes to these private tables are unsupported.
    super::point_lookup::run(|| apply(&changes));
    // Appends may have committed during apply(). Reload and preserve their state.
    // The writer's metadata lock is held only during this short finalization.
    let (current, current_limit) = state("FOR UPDATE");
    assert_eq!(current_limit, limit);
    let next = current
        .finish(ticket, limit)
        .expect("current compaction ticket");
    Spi::run(&format!("TRUNCATE {}", TABLES[ticket.slot])).expect("retire sealed segment");
    save(next);
    true
}

#[pg_extern]
fn journal_proto_get(key: Vec<u8>) -> Option<Vec<u8>> {
    // Protect the recovery view from compactor finalization/TRUNCATE. This is a
    // validation interface, not the future in-memory GET path.
    let (current, limit) = state("FOR SHARE");
    let changes = read_changes(current, limit);
    let entry = if let Some(entry) = changes.get(key.as_slice()) {
        entry.clone()
    } else {
        Spi::connect(|client| {
            let rows = client
                .select(
                    "SELECT value,expires_at_ms,data_type FROM cache_journal.entries WHERE key=$1",
                    None,
                    &[key.as_slice().into()],
                )
                .unwrap();
            rows.into_iter().next().map(|row| Entry {
                value: pg_cache_core::value::Value::from_stored(
                    row.get::<i16>(3)
                        .unwrap()
                        .unwrap()
                        .try_into()
                        .expect("value type"),
                    &row.get::<Vec<u8>>(1).unwrap().unwrap(),
                )
                .expect("decode stored value"),
                expires_at_ms: row.get::<i64>(2).unwrap(),
            })
        })
    };
    entry
        .filter(|entry| entry.expires_at_ms.is_none_or(|expiry| expiry > now_ms()))
        .map(|entry| entry.value.to_vec())
}

/// Restore the final overlay, skipping superseded base rows before accounting
/// memory. The shared metadata lock excludes journal retirement during the scan.
pub(crate) fn restore(
    memory_limit: usize,
    pending_limit: usize,
) -> (pg_cache_core::engine::Engine, u64) {
    let (current, limit) = state("FOR SHARE");
    assert!(
        pending_limit as u64 <= limit,
        "pending limit must fit a journal segment"
    );
    let changes = read_changes(current, limit);
    let mut engine = pg_cache_core::engine::Engine::new(memory_limit);
    let now = now_ms();
    Spi::connect(|client| {
        let mut cursor = client.open_cursor(
            "SELECT key,value,expires_at_ms,data_type FROM cache_journal.entries",
            &[],
        );
        loop {
            let rows = cursor.fetch(256).expect("restore base cursor");
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let key = row.get::<Vec<u8>>(1).unwrap().unwrap();
                if changes.contains_key(key.as_slice()) {
                    continue;
                }
                engine
                    .restore(
                        key,
                        Entry {
                            value: pg_cache_core::value::Value::from_stored(
                                row.get::<i16>(4)
                                    .unwrap()
                                    .unwrap()
                                    .try_into()
                                    .expect("value type"),
                                &row.get::<Vec<u8>>(2).unwrap().unwrap(),
                            )
                            .expect("decode stored value"),
                            expires_at_ms: row.get::<i64>(3).unwrap(),
                        },
                        now,
                    )
                    .expect("restore materialized cache");
            }
        }
    });
    for (key, entry) in changes {
        if let Some(entry) = entry {
            engine
                .restore(key.to_vec(), entry, now)
                .expect("restore journal overlay");
        }
    }
    (engine, current.committed)
}
