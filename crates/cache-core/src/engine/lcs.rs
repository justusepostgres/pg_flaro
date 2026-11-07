//! Binary longest common subsequence with Redis's backtracking order.
use super::{eq, integer, Engine, ARITY, SYNTAX};
use crate::protocol::{Reply, MAX_FRAME};

pub(super) fn execute(engine: &Engine, args: &[&[u8]], now: i64) -> Reply {
    if args.len() < 3 {
        return ARITY;
    }
    let left = engine.get(args[1], now);
    let right = engine.get(args[2], now);
    // Unlike most string commands, Redis returns ERR rather than WRONGTYPE.
    if [left, right].iter().flatten().any(|e| e.value.kind() != 0) {
        return Reply::Error("ERR The specified keys must contain string values");
    }
    let a = left.map_or(&[][..], |e| e.value.as_ref());
    let b = right.map_or(&[][..], |e| e.value.as_ref());
    let (mut len_only, mut indexes, mut with_len) = (false, false, false);
    let mut minimum = 0;
    let mut pos = 3;
    while pos < args.len() {
        if eq(args[pos], b"LEN") {
            len_only = true;
        } else if eq(args[pos], b"IDX") {
            indexes = true;
        } else if eq(args[pos], b"WITHMATCHLEN") {
            with_len = true;
        } else if eq(args[pos], b"MINMATCHLEN") && pos + 1 < args.len() {
            pos += 1;
            let Some(value) = integer(args[pos]) else {
                return Reply::Error("ERR value is not an integer or out of range");
            };
            minimum = value.max(0) as usize;
        } else {
            return SYNTAX;
        }
        pos += 1;
    }
    if len_only && indexes {
        return Reply::Error("ERR If you want both the length and indexes, please just use IDX.");
    }
    // Bound quadratic work and allocation before constructing the table. The
    // ceiling follows Flaro's frame limit, smaller than Redis's default ceiling.
    let cells = a.len().checked_add(1).and_then(|rows| {
        b.len()
            .checked_add(1)
            .and_then(|cols| rows.checked_mul(cols))
    });
    let Some(cells) = cells.filter(|n| *n <= MAX_FRAME / size_of::<u32>()) else {
        return Reply::Error(
            "ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len",
        );
    };
    let mut table = Vec::<u32>::new();
    if table.try_reserve_exact(cells).is_err() {
        return Reply::Error("ERR Insufficient memory, failed allocating transient memory for LCS");
    }
    table.resize(cells, 0);
    let width = b.len() + 1;
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            table[i * width + j] = if a[i - 1] == b[j - 1] {
                table[(i - 1) * width + j - 1] + 1
            } else {
                table[(i - 1) * width + j].max(table[i * width + j - 1])
            };
        }
    }
    let length = table[a.len() * width + b.len()];
    if len_only {
        return Reply::Integer(length as i64);
    }
    let mut bytes = Vec::with_capacity(if indexes { 0 } else { length as usize });
    let mut matches = Vec::new();
    let mut range: Option<(usize, usize, usize, usize)> = None;
    let (mut i, mut j) = (a.len(), b.len());
    while i > 0 && j > 0 {
        if a[i - 1] == b[j - 1] {
            if indexes {
                range = Some(match range {
                    Some((_, end_a, _, end_b)) => (i - 1, end_a, j - 1, end_b),
                    None => (i - 1, i - 1, j - 1, j - 1),
                });
            } else {
                bytes.push(a[i - 1]);
            }
            i -= 1;
            j -= 1;
        } else {
            emit(&mut matches, range.take(), minimum, with_len);
            // Redis moves left on ties; changing this changes both replies.
            if table[(i - 1) * width + j] > table[i * width + j - 1] {
                i -= 1;
            } else {
                j -= 1;
            }
        }
    }
    emit(&mut matches, range, minimum, with_len);
    if indexes {
        Reply::Array(vec![
            Reply::Bulk(Some(b"matches".to_vec())),
            Reply::Array(matches),
            Reply::Bulk(Some(b"len".to_vec())),
            Reply::Integer(length as i64),
        ])
    } else {
        bytes.reverse();
        Reply::Bulk(Some(bytes))
    }
}

fn emit(
    matches: &mut Vec<Reply>,
    range: Option<(usize, usize, usize, usize)>,
    minimum: usize,
    with_len: bool,
) {
    let Some((start_a, end_a, start_b, end_b)) = range else {
        return;
    };
    let len = end_a - start_a + 1;
    if len < minimum {
        return;
    }
    let pair = |start, end| Reply::Array(vec![Reply::Integer(start), Reply::Integer(end)]);
    let mut parts = vec![
        pair(start_a as i64, end_a as i64),
        pair(start_b as i64, end_b as i64),
    ];
    if with_len {
        parts.push(Reply::Integer(len as i64));
    }
    matches.push(Reply::Array(parts));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lcs_is_read_only_checks_expiry_and_bounds_quadratic_work() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SET", b"a", b"ab", b"PX", b"100"], 0, |_| true);
        engine.execute(&[b"SET", b"b", b"ba"], 0, |_| true);
        let original = engine.snapshot(b"a");
        assert_eq!(
            engine.execute(&[b"LCS", b"a", b"b"], 1, |_| panic!("read only")),
            Reply::Bulk(Some(b"b".to_vec()))
        );
        assert_eq!(engine.snapshot(b"a"), original);
        assert_eq!(
            execute(&engine, &[b"LCS", b"a", b"b", b"LEN"], 100),
            Reply::Integer(0)
        );
        let large = vec![b'x'; 1024];
        engine.execute(&[b"SET", b"a", &large], 0, |_| true);
        engine.execute(&[b"SET", b"b", &large], 0, |_| true);
        assert!(matches!(
            execute(&engine, &[b"LCS", b"a", b"b"], 0),
            Reply::Error(_)
        ));
    }
}
