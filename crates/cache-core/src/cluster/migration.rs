//! Bounded, synchronous single-key transfer. Like Redis MIGRATE, it pauses the
//! source event loop. Never delete the source on an ambiguous destination reply.
use super::{keyslot, number, Cluster};
use crate::{
    engine::Engine,
    now_ms,
    protocol::{self, Reply},
    server::Sink,
};
use std::{
    io::{self, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    time::{Duration, Instant},
};

impl Cluster {
    pub(super) fn migrate(
        &mut self,
        args: &[&[u8]],
        engine: &mut Engine,
        sink: &mut Sink,
    ) -> io::Result<Reply> {
        if !(args.len() == 6 || args.len() == 7 && args[6].eq_ignore_ascii_case(b"REPLACE"))
            || args[4] != b"0"
        {
            return Ok(Reply::Error(
                "ERR supported syntax: MIGRATE host port key 0 timeout [REPLACE]",
            ));
        }
        let slot = keyslot(args[3]);
        let Some(&peer) = self.topology.migrating.get(&slot) else {
            return Ok(Reply::Error(
                "ERR slot must be MIGRATING before key transfer",
            ));
        };
        if self
            .topology
            .pending
            .as_ref()
            .is_some_and(|p| p.key != args[3])
        {
            return Ok(Reply::Error("TRYAGAIN Resume the pending transfer first"));
        }
        let target = self.topology.config.nodes[peer as usize].clone();
        if args[1] != target.host.as_bytes() || number(args[2]) != Some(target.port) {
            return Ok(Reply::Error(
                "ERR destination must match the configured migration target",
            ));
        }
        let Some(timeout) = std::str::from_utf8(args[5])
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0 && *n <= 30000)
        else {
            return Ok(Reply::Error(
                "ERR migration timeout must be 1..30000 milliseconds",
            ));
        };
        let Some(entry) = engine.get(args[3], now_ms()).cloned() else {
            if let Some(pending) = &self.topology.pending {
                // Expiry can remove the source before an uncertain import is
                // processed. Fence that token at the destination before allowing
                // new ASK writes; a delayed expired copy must not erase them.
                let token = pending.token.to_string();
                let slot = slot.to_string();
                let frame = request(&[
                    b"PGCACHE.FINISH",
                    self.topology.config.local_id.as_bytes(),
                    token.as_bytes(),
                    slot.as_bytes(),
                ]);
                if transfer(
                    &target.host,
                    target.port,
                    &frame,
                    Duration::from_millis(timeout),
                )
                .is_err()
                {
                    return Ok(Reply::Error(
                        "IOERR destination completion uncertain; resume MIGRATE",
                    ));
                }
                let mut next = self.topology.clone();
                next.pending = None;
                self.persist(next)?;
            }
            return Ok(Reply::Simple("NOKEY"));
        };
        // Commit prior source writes first: a failed transfer must not weaken their
        // durability. The network loop is paused until the source deletion commits.
        sink.barrier(engine)?;
        let token = if let Some(p) = &self.topology.pending {
            p.token
        } else {
            self.topology
                .epoch
                .checked_add(1)
                .ok_or_else(|| io::Error::other("transfer sequence exhausted"))?
        };
        let expiry = entry.expires_at_ms.unwrap_or(-1).to_string();
        let replace: &[u8] = if args.len() == 7 { b"1" } else { b"0" };
        let token_text = token.to_string();
        let kind_text = entry.value.kind().to_string();
        let frame = request(&[
            b"PGCACHE.IMPORT",
            args[3],
            &entry.value,
            expiry.as_bytes(),
            self.topology.config.local_id.as_bytes(),
            replace,
            token_text.as_bytes(),
            kind_text.as_bytes(),
        ]);
        if frame.len() > protocol::MAX_TRANSFER_FRAME {
            return Ok(Reply::Error("ERR transfer frame exceeds maximum size"));
        }
        if self.topology.pending.is_none() {
            let mut next = self.topology.clone();
            next.epoch = token;
            next.pending = Some(super::topology::Transfer {
                token,
                slot,
                target: peer,
                key: args[3].to_vec(),
            });
            self.persist(next)?;
        }
        match transfer(&target.host, target.port, &frame, Duration::from_millis(timeout)) {
            Ok(()) => {},
            Err(_) => return Ok(Reply::Error("IOERR destination transfer failed or response uncertain; source retained, retry with REPLACE")),
        }
        let result = engine.execute(&[b"DEL", args[3]], now_ms(), |m| sink.submit(m));
        if matches!(result, Reply::Error(_) | Reply::OwnedError(_)) {
            return Ok(result);
        }
        // A timeout here is fatal. Serving the in-memory deletion before it is
        // durable could resurrect an old value after the target accepts updates.
        sink.barrier(engine)?;
        let mut next = self.topology.clone();
        next.pending = None;
        self.persist(next)?;
        Ok(Reply::Simple("OK"))
    }
    pub(super) fn finish(
        &mut self,
        args: &[&[u8]],
        engine: &Engine,
        sink: &mut Sink,
    ) -> io::Result<Reply> {
        if args.len() != 4 {
            return Ok(Reply::Error("ERR wrong number of arguments"));
        }
        let Some(source) = self.topology.node_index(args[1]) else {
            return Ok(Reply::Error("ERR unknown import source"));
        };
        let Some(token) = std::str::from_utf8(args[2])
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
        else {
            return Ok(Reply::Error("ERR invalid transfer sequence"));
        };
        let Some(slot) = number(args[3]).filter(|s| (*s as usize) < super::SLOTS) else {
            return Ok(Reply::Error("ERR invalid slot"));
        };
        if self
            .topology
            .imported
            .get(&source)
            .is_some_and(|seen| token <= *seen)
        {
            return Ok(Reply::Simple("OK"));
        }
        if self.topology.importing.get(&slot) != Some(&source) {
            return Ok(Reply::Error("ERR slot is not importing from this source"));
        }
        sink.barrier(engine)?;
        let mut next = self.topology.clone();
        next.imported.insert(source, token);
        self.persist(next)?;
        Ok(Reply::Simple("OK"))
    }
    pub(super) fn import(
        &mut self,
        args: &[&[u8]],
        engine: &mut Engine,
        sink: &mut Sink,
    ) -> io::Result<Reply> {
        if !matches!(args.len(), 7 | 8) {
            return Ok(Reply::Error("ERR wrong number of arguments"));
        }
        let slot = keyslot(args[1]);
        let Some(source) = self.topology.node_index(args[4]) else {
            return Ok(Reply::Error("ERR unknown import source"));
        };
        let Some(token) = std::str::from_utf8(args[6])
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
        else {
            return Ok(Reply::Error("ERR invalid transfer sequence"));
        };
        // A previous +OK covers both the value and this watermark. Delayed or
        // retried imports must never overwrite subsequent destination writes,
        // even after the slot has been finalized or moved again.
        if self
            .topology
            .imported
            .get(&source)
            .is_some_and(|seen| token <= *seen)
        {
            return Ok(Reply::Simple("OK"));
        }
        if self.topology.importing.get(&slot) != Some(&source) {
            return Ok(Reply::Error("ERR slot is not importing from this source"));
        }
        let Some(expiry) = std::str::from_utf8(args[3])
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|t| *t >= -1)
        else {
            return Ok(Reply::Error("ERR invalid absolute expiry"));
        };
        if args[5] != b"0" && args[5] != b"1" {
            return Ok(Reply::Error("ERR invalid replace flag"));
        }
        if args[5] == b"0" && engine.get(args[1], now_ms()).is_some() {
            return Ok(Reply::Error("BUSYKEY Target key name already exists"));
        }
        let kind = if args.len() == 8 {
            match number(args[7]).and_then(|n| u8::try_from(n).ok()) {
                Some(kind) => kind,
                None => return Ok(Reply::Error("ERR invalid value type")),
            }
        } else {
            0
        };
        let value = match crate::value::Value::from_stored(kind, args[2]) {
            Ok(value) => value,
            Err(_) => return Ok(Reply::Error("ERR invalid typed value")),
        };
        let result = engine.apply_typed(
            vec![(
                args[1].to_vec(),
                Some(crate::engine::Entry {
                    value,
                    expires_at_ms: (expiry >= 0).then_some(expiry),
                }),
            )],
            Reply::Simple("OK"),
            &mut |m| sink.submit(m),
        );
        if result == Reply::Simple("OK") {
            sink.barrier(engine)?;
            let mut next = self.topology.clone();
            next.imported.insert(source, token);
            self.persist(next)?;
        }
        Ok(result)
    }
}
fn request(args: &[&[u8]]) -> Vec<u8> {
    let mut result = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        protocol::bulk(Some(arg), &mut result);
    }
    result
}
fn transfer(host: &str, port: u16, frame: &[u8], timeout: Duration) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut socket = None;
    for address in (host, port).to_socket_addrs()?.take(8) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        if let Ok(stream) = TcpStream::connect_timeout(&address, remaining) {
            socket = Some(stream);
            break;
        }
    }
    let mut socket = socket.ok_or_else(|| io::Error::other("migration connect failed"))?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(io::ErrorKind::TimedOut.into());
    }
    socket.set_write_timeout(Some(remaining))?;
    socket.write_all(frame)?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(io::ErrorKind::TimedOut.into());
    }
    socket.set_read_timeout(Some(remaining))?;
    let mut reply = [0; 5];
    socket.read_exact(&mut reply)?;
    if &reply != b"+OK\r\n" {
        return Err(io::Error::other("destination rejected import"));
    }
    Ok(())
}
