//! Symmetric nonblocking load generator for bounded, mutation-producing workloads.
//! Each connection owns one key, preventing set remove/add pairs from racing.
use polling::{Event, Events, PollMode, Poller};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};
fn frame(args: &[&[u8]], out: &mut Vec<u8>) {
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        pg_cache_core::protocol::bulk(Some(a), out);
    }
}
fn value(n: u64) -> Vec<u8> {
    format!("{n:016x}{}", "x".repeat(48)).into_bytes()
}
fn request(op: &str, id: usize, n: u64, card: usize, out: &mut Vec<u8>) {
    let key = format!("bench:{id}");
    let member = format!("f{}", n as usize % card);
    let index = (n as usize % card).to_string();
    let v = value(n);
    match op {
        "hset" => frame(&[b"HSET", key.as_bytes(), member.as_bytes(), &v], out),
        "hget" => frame(&[b"HGET", key.as_bytes(), member.as_bytes()], out),
        "lset" => frame(&[b"LSET", key.as_bytes(), index.as_bytes(), &v], out),
        "lindex" => frame(&[b"LINDEX", key.as_bytes(), index.as_bytes()], out),
        "set-churn" => {
            frame(&[b"SREM", key.as_bytes(), member.as_bytes()], out);
            frame(&[b"SADD", key.as_bytes(), member.as_bytes()], out);
        }
        "sismember" => frame(&[b"SISMEMBER", key.as_bytes(), member.as_bytes()], out),
        "zincrby" => frame(&[b"ZINCRBY", key.as_bytes(), b"1", member.as_bytes()], out),
        "zscore" => frame(&[b"ZSCORE", key.as_bytes(), member.as_bytes()], out),
        _ => panic!("unknown operation"),
    }
}
fn reply(input: &[u8], op: &str) -> Result<Option<usize>, String> {
    let Some(end) = input.windows(2).position(|v| v == b"\r\n") else {
        return Ok(None);
    };
    let line = &input[1..end];
    match input[0] {
        b'-' => Err(String::from_utf8_lossy(line).into()),
        b'+' => {
            if op == "lset" && line == b"OK" {
                Ok(Some(end + 2))
            } else {
                Err(format!("unexpected simple response {line:?}"))
            }
        }
        b':' => {
            let expected = if op == "hset" { b"0".as_slice() } else { b"1" };
            if line != expected {
                return Err(format!("unexpected integer {line:?} for {op}"));
            }
            Ok(Some(end + 2))
        }
        b'$' => {
            let len: usize = std::str::from_utf8(line)
                .map_err(|e| e.to_string())?
                .parse()
                .map_err(|_| "null or invalid bulk")?;
            let total = end + 2 + len + 2;
            if input.len() < total {
                return Ok(None);
            };
            if &input[total - 2..total] != b"\r\n" {
                return Err("invalid bulk terminator".into());
            }
            let v = &input[end + 2..total - 2];
            match op {
                "hget" | "lindex" => {
                    if v != b"seedxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" {
                        return Err("read value mismatch".into());
                    }
                }
                "zscore" => {
                    if v != b"0" {
                        return Err("score mismatch".into());
                    }
                }
                "zincrby" => {
                    if std::str::from_utf8(v)
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                        .is_none_or(|v| !v.is_finite() || v < 1.0)
                    {
                        return Err("invalid increment score".into());
                    }
                }
                _ => return Err("unexpected bulk response".into()),
            }
            Ok(Some(total))
        }
        _ => Err("unexpected response kind".into()),
    }
}
struct Client {
    socket: TcpStream,
    id: usize,
    out: Vec<u8>,
    written: usize,
    input: Vec<u8>,
    pending: usize,
    iterations: u64,
    sent: u64,
    start: Instant,
    active: bool,
}
impl Client {
    fn batch(&mut self, op: &str, card: usize, pipeline: usize) {
        self.out.clear();
        self.written = 0;
        for _ in 0..pipeline {
            request(op, self.id, self.sent, card, &mut self.out);
            self.sent += 1;
        }
        self.pending = pipeline * if op == "set-churn" { 2 } else { 1 };
        self.start = Instant::now();
        self.active = true;
    }
}
fn worker(
    address: SocketAddr,
    op: &str,
    card: usize,
    ids: Vec<usize>,
    pipeline: usize,
    duration: Duration,
    barrier: Arc<Barrier>,
) -> (Vec<(usize, u64)>, Vec<u64>, f64) {
    let poll = Poller::new().unwrap();
    let mut clients = Vec::new();
    for id in ids {
        let socket = TcpStream::connect_timeout(&address, Duration::from_secs(10)).unwrap();
        socket.set_nodelay(true).unwrap();
        socket.set_nonblocking(true).unwrap();
        clients.push(Client {
            socket,
            id,
            out: Vec::new(),
            written: 0,
            input: Vec::new(),
            pending: 0,
            iterations: 0,
            sent: 0,
            start: Instant::now(),
            active: true,
        });
    }
    for (i, c) in clients.iter().enumerate() {
        unsafe {
            poll.add_with_mode(&c.socket, Event::all(i), PollMode::Level)
                .unwrap();
        }
    }
    barrier.wait();
    let begin = Instant::now();
    let end = begin + duration;
    let mut latency = Vec::new();
    let mut events = Events::new();
    let mut remaining = clients.len();
    for c in &mut clients {
        c.batch(op, card, pipeline);
    }
    while remaining > 0 {
        assert!(
            Instant::now() < end + Duration::from_secs(60),
            "benchmark drain timed out"
        );
        events.clear();
        poll.wait(&mut events, Some(Duration::from_millis(100)))
            .unwrap();
        for event in events.iter() {
            let c = &mut clients[event.key];
            if !c.active {
                continue;
            }
            if event.readable {
                let mut buffer = [0; 16384];
                loop {
                    match c.socket.read(&mut buffer) {
                        Ok(0) => panic!("server disconnected"),
                        Ok(n) => c.input.extend_from_slice(&buffer[..n]),
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(e) => panic!("read: {e}"),
                    }
                }
                let mut used = 0;
                while c.pending > 0 {
                    match reply(&c.input[used..], op).unwrap() {
                        Some(n) => {
                            used += n;
                            c.pending -= 1;
                        }
                        None => break,
                    }
                }
                c.input.drain(..used);
                if c.pending == 0 {
                    assert!(c.input.is_empty());
                    latency.push(c.start.elapsed().as_micros() as u64);
                    c.iterations += pipeline as u64;
                    if Instant::now() >= end {
                        c.active = false;
                        remaining -= 1;
                        poll.modify(&c.socket, Event::none(event.key)).unwrap();
                        continue;
                    }
                    c.batch(op, card, pipeline);
                }
            }
            while c.written < c.out.len() {
                match c.socket.write(&c.out[c.written..]) {
                    Ok(0) => panic!("zero write"),
                    Ok(n) => c.written += n,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => panic!("write: {e}"),
                }
            }
            poll.modify(
                &c.socket,
                if c.written < c.out.len() {
                    Event::all(event.key)
                } else {
                    Event::readable(event.key)
                },
            )
            .unwrap();
        }
    }
    let elapsed = begin.elapsed().as_secs_f64();
    for c in &clients {
        poll.delete(&c.socket).unwrap();
    }
    (
        clients.iter().map(|c| (c.id, c.iterations)).collect(),
        latency,
        elapsed,
    )
}
fn main() {
    let a: Vec<String> = std::env::args().collect();
    assert_eq!(
        a.len(),
        8,
        "address operation cardinality clients pipeline seconds threads"
    );
    let address: SocketAddr = a[1].parse().unwrap();
    let op = &a[2];
    let card: usize = a[3].parse().unwrap();
    let count: usize = a[4].parse().unwrap();
    let pipeline: usize = a[5].parse().unwrap();
    let seconds: f64 = a[6].parse().unwrap();
    let threads: usize = a[7].parse().unwrap();
    assert!(
        card > 0
            && count > 0
            && count <= 100
            && threads > 0
            && threads <= count
            && pipeline > 0
            && pipeline <= 64
            && seconds > 0.0
    );
    let barrier = Arc::new(Barrier::new(threads));
    let mut handles = Vec::new();
    for t in 0..threads {
        let ids = (0..count).filter(|id| id % threads == t).collect();
        let b = barrier.clone();
        let op = op.clone();
        handles.push(std::thread::spawn(move || {
            worker(
                address,
                &op,
                card,
                ids,
                pipeline,
                Duration::from_secs_f64(seconds),
                b,
            )
        }));
    }
    let mut counts = Vec::new();
    let mut latency = Vec::new();
    let mut elapsed = 0.0f64;
    for handle in handles {
        let (c, l, e) = handle.join().unwrap();
        counts.extend(c);
        latency.extend(l);
        elapsed = elapsed.max(e);
    }
    counts.sort_unstable();
    latency.sort_unstable();
    let total: u64 = counts.iter().map(|(_, n)| n).sum();
    let commands = total * if op == "set-churn" { 2 } else { 1 };
    let percentile = |p: f64| latency[((latency.len() - 1) as f64 * p) as usize] as f64 / 1000.0;
    println!(
        "{}",
        serde_json::json!({"operation":op,"cardinality":card,"clients":count,"pipeline":pipeline,"threads":threads,"elapsed_seconds":elapsed,"commands":commands,"iterations":total,"ops_per_second":commands as f64/elapsed,"p50_ms":percentile(0.5),"p99_ms":percentile(0.99),"p999_ms":percentile(0.999),"latency_basis":"pipeline batch completion","latency_samples":latency.len(),"per_client_iterations":counts})
    );
}
