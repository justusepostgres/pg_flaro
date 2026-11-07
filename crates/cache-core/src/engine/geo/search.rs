//! Radius/box selection and atomic sorted-set result replacement.
use super::*;
use crate::{
    engine::Entry,
    value::{Score, SortedSet, Value},
};

#[derive(Clone, Copy)]
enum Shape {
    Radius(f64),
    Box(f64, f64),
}
#[derive(Default)]
struct Options {
    origin: Option<(f64, f64)>,
    member: bool,
    shape: Option<Shape>,
    unit: f64,
    count: Option<usize>,
    any: bool,
    sort: i8,
    withdist: bool,
    withhash: bool,
    withcoord: bool,
    storedist: bool,
    destination: Option<usize>,
}

pub(super) fn run(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply, Reply> {
    let store = eq(a[0], b"GEOSEARCHSTORE");
    let index = if store { 2 } else { 1 };
    let source = match engine.get(a[index], now) {
        None => None,
        Some(entry) => match entry.value.collection() {
            Some(Collection::ZSet(zset)) => Some(zset),
            _ => return Err(collections::WRONGTYPE),
        },
    };
    let options = parse(a, index + 1, source, store)?;
    let mut results = Vec::new();
    if let (Some(source), Some(origin), Some(shape)) = (source, options.origin, options.shape) {
        let (radius, width, height) = match shape {
            Shape::Radius(radius) => (radius, radius, radius),
            Shape::Box(width, height) => {
                let (width, height) = (width / 2.0, height / 2.0);
                ((width * width + height * height).sqrt(), width, height)
            }
        };
        let ranges = super::cover::ranges(
            origin,
            radius * options.unit,
            width * options.unit,
            height * options.unit,
        );
        for (score, member) in candidates(source, ranges) {
            let position = decode(score.0);
            let meters = distance(origin, position);
            let matched = match shape {
                Shape::Radius(radius) => meters <= radius * options.unit,
                Shape::Box(width, height) => {
                    let vertical =
                        6372797.560856 * (position.1.to_radians() - origin.1.to_radians()).abs();
                    let horizontal = distance(position, (origin.0, position.1));
                    vertical <= height * options.unit / 2.0
                        && horizontal <= width * options.unit / 2.0
                }
            };
            if matched {
                results.push((member.clone(), score.0, position, meters / options.unit));
                if options.any && options.count == Some(results.len()) {
                    break;
                }
            }
        }
    }
    let sort = if options.sort == 0 && options.count.is_some() && !options.any {
        1
    } else {
        options.sort
    };
    if sort != 0 {
        results.sort_by(|a, b| {
            if sort > 0 {
                a.3.total_cmp(&b.3)
            } else {
                b.3.total_cmp(&a.3)
            }
        });
    }
    if let Some(count) = options.count {
        results.truncate(count);
    }
    if let Some(destination) = options.destination {
        let count = results.len();
        let entry = if count == 0 {
            None
        } else {
            let mut zset = SortedSet::default();
            for (member, hash, _, distance) in results {
                zset.insert(member, if options.storedist { distance } else { hash });
            }
            Some(Entry {
                value: Value::from_collection(Collection::ZSet(zset))
                    .map_err(|_| Reply::Error("ERR collection exceeds maximum size"))?,
                expires_at_ms: None,
            })
        };
        if entry.is_none() && engine.get(a[destination], now).is_none() {
            return Ok(Reply::Integer(0));
        }
        return Ok(engine.apply_typed(
            vec![(a[destination].to_vec(), entry)],
            Reply::Integer(count as i64),
            submit,
        ));
    }
    let mut bytes = 32usize;
    let mut reply = Vec::new();
    for (member, hash, (longitude, latitude), distance) in results {
        bytes += member.len() + 160;
        if bytes > 2 * crate::protocol::MAX_FRAME {
            return Err(Reply::Error("ERR reply exceeds output limit"));
        }
        let member = Reply::Bulk(Some(member.to_vec()));
        if !options.withdist && !options.withhash && !options.withcoord {
            reply.push(member);
            continue;
        }
        let mut row = vec![member];
        if options.withdist {
            row.push(bulk(format!("{distance:.4}")));
        }
        if options.withhash {
            row.push(Reply::Integer(hash as i64));
        }
        if options.withcoord {
            row.push(Reply::Array(vec![
                bulk(format_coordinate(longitude)),
                bulk(format_coordinate(latitude)),
            ]));
        }
        reply.push(Reply::Array(row));
    }
    Ok(Reply::Array(reply))
}
fn candidates(
    source: &SortedSet,
    ranges: Vec<(f64, f64)>,
) -> impl Iterator<Item = &(Score, crate::engine::Bytes)> {
    ranges.into_iter().flat_map(move |(min, max)| {
        let empty = crate::engine::Bytes::from(&b""[..]);
        source
            .order
            .range((Score(min), empty.clone())..(Score(max), empty))
    })
}

fn parse(
    a: &[&[u8]],
    mut i: usize,
    source: Option<&SortedSet>,
    store: bool,
) -> Result<Options, Reply> {
    let radius_command = a[0].to_ascii_lowercase().starts_with(b"georadius");
    let member_command = a[0].to_ascii_lowercase().starts_with(b"georadiusbymember");
    let readonly = a[0].to_ascii_lowercase().ends_with(b"_ro");
    let mut options = Options {
        destination: store.then_some(1),
        ..Options::default()
    };
    if radius_command {
        i = if member_command { 5 } else { 6 };
        if member_command {
            if let Some(source) = source {
                let score = source
                    .scores
                    .get(a[2])
                    .ok_or(Reply::Error("ERR could not decode requested zset member"))?;
                options.origin = Some(decode(score.0));
            }
            options.member = true;
        } else {
            options.origin = Some(coordinates(a[2], a[3])?);
        }
        // Redis skips the radius/unit when the member-origin source is absent.
        if !member_command || source.is_some() {
            options.shape = Some(Shape::Radius(radius(a[i - 2])?));
            options.unit = unit(a[i - 1])?;
        }
    }
    while i < a.len() {
        let arg = cstring(a[i]);
        let remaining = a.len() - i;
        if eq(arg, b"WITHDIST") {
            options.withdist = true;
        } else if eq(arg, b"WITHHASH") {
            options.withhash = true;
        } else if eq(arg, b"WITHCOORD") {
            options.withcoord = true;
        } else if eq(arg, b"ANY") {
            options.any = true;
        } else if eq(arg, b"ASC") {
            options.sort = 1;
        } else if eq(arg, b"DESC") {
            options.sort = -1;
        } else if radius_command
            && !readonly
            && remaining > 1
            && (eq(arg, b"STORE") || eq(arg, b"STOREDIST"))
        {
            options.destination = Some(i + 1);
            options.storedist = eq(arg, b"STOREDIST");
            i += 1;
        } else if eq(arg, b"STOREDIST") && store {
            options.storedist = true;
        } else if eq(arg, b"COUNT") && remaining > 1 {
            let count = collections::integer(a[i + 1])?;
            if count <= 0 {
                return Err(Reply::Error("ERR COUNT must be > 0"));
            }
            options.count = Some(count as usize);
            i += 1;
        } else if !radius_command
            && eq(arg, b"FROMMEMBER")
            && remaining > 1
            && (options.origin.is_none() || options.member)
        {
            if let Some(source) = source {
                let score = source
                    .scores
                    .get(a[i + 1])
                    .ok_or(Reply::Error("ERR could not decode requested zset member"))?;
                options.origin = Some(decode(score.0));
            }
            options.member = true;
            i += 1;
        } else if !radius_command && eq(arg, b"FROMLONLAT") && remaining > 2 && !options.member {
            options.origin = Some(coordinates(a[i + 1], a[i + 2])?);
            i += 2;
        } else if !radius_command
            && eq(arg, b"BYRADIUS")
            && remaining > 2
            && !matches!(options.shape, Some(Shape::Box(..)))
        {
            let radius = radius(a[i + 1])?;
            options.unit = unit(a[i + 2])?;
            options.shape = Some(Shape::Radius(radius));
            i += 2;
        } else if !radius_command
            && eq(arg, b"BYBOX")
            && remaining > 3
            && !matches!(options.shape, Some(Shape::Radius(..)))
        {
            let width = collections::score::parse(a[i + 1])
                .map_err(|_| Reply::Error("ERR need numeric width"))?;
            let height = collections::score::parse(a[i + 2])
                .map_err(|_| Reply::Error("ERR need numeric height"))?;
            if width < 0.0 || height < 0.0 {
                return Err(Reply::Error("ERR height or width cannot be negative"));
            }
            options.unit = unit(a[i + 3])?;
            options.shape = Some(Shape::Box(width, height));
            i += 3;
        } else {
            return Err(collections::SYNTAX);
        }
        i += 1;
    }
    if options.destination.is_some() && (options.withcoord || options.withdist || options.withhash)
    {
        return Err(Reply::Error(if store {
            "ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
        } else {
            "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
        }));
    }
    if !radius_command && options.origin.is_none() && !options.member {
        return Err(Reply::OwnedError(format!(
            "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for {}",
            String::from_utf8_lossy(a[0])
        )));
    }
    if !radius_command && options.shape.is_none() {
        return Err(Reply::OwnedError(format!(
            "ERR exactly one of BYRADIUS and BYBOX can be specified for {}",
            String::from_utf8_lossy(a[0])
        )));
    }
    if options.any && options.count.is_none() {
        return Err(Reply::Error("ERR the ANY argument requires COUNT argument"));
    }
    Ok(options)
}

fn radius(value: &[u8]) -> Result<f64, Reply> {
    let value =
        collections::score::parse(value).map_err(|_| Reply::Error("ERR need numeric radius"))?;
    if value < 0.0 {
        Err(Reply::Error("ERR radius cannot be negative"))
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn local_search_seeks_past_unrelated_index_entries() {
        let mut source = SortedSet::default();
        for index in 0..10_000 {
            source.insert(
                format!("far:{index}").into_bytes().into(),
                encode(100.0 + index as f64 / 10_000.0, 45.0, LAT) as f64,
            );
        }
        for index in 0..3 {
            source.insert(
                format!("near:{index}").into_bytes().into(),
                encode(1.0 + index as f64 / 100_000.0, 1.0, LAT) as f64,
            );
        }
        let ranges = super::super::cover::ranges((1.0, 1.0), 100.0, 100.0, 100.0);
        let visited: Vec<_> = candidates(&source, ranges).collect();
        assert_eq!(source.scores.len(), 10_003);
        assert_eq!(visited.len(), 3);
        assert!(visited
            .iter()
            .all(|(_, member)| member.starts_with(b"near:")));
    }

    #[test]
    fn stored_searches_admit_atomically_and_support_aliases() {
        let mut engine = Engine::new(1 << 20);
        engine.execute(&[b"GEOADD", b"source", b"0", b"0", b"member"], 0, |_| true);
        engine.execute(&[b"SET", b"target", b"original"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"target", b"5000"], 0, |_| true);
        let before = engine.snapshot(b"target");
        let watch = engine.watch(b"target", 0);
        let command = [
            b"GEOSEARCHSTORE".as_slice(),
            b"target",
            b"source",
            b"FROMMEMBER",
            b"member",
            b"BYRADIUS",
            b"10",
            b"km",
        ];
        assert_eq!(
            engine.execute(&command, 1, |_| false),
            Reply::Error("MISCONF persistence backlog full or unavailable")
        );
        for radius_command in [
            vec![
                b"GEORADIUS".as_slice(),
                b"source",
                b"0",
                b"0",
                b"10",
                b"km",
                b"STORE",
                b"target",
            ],
            vec![
                b"GEORADIUSBYMEMBER".as_slice(),
                b"source",
                b"member",
                b"10",
                b"km",
                b"STOREDIST",
                b"target",
            ],
        ] {
            assert_eq!(
                engine.execute(&radius_command, 1, |_| false),
                Reply::Error("MISCONF persistence backlog full or unavailable")
            );
            assert_eq!(engine.snapshot(b"target"), before);
        }
        assert_eq!(engine.snapshot(b"target"), before);
        assert!(!engine.watch_dirty(b"target", &watch, 1));
        assert!(engine
            .transact(
                |engine, submit| vec![engine.execute(&command, 1, submit)],
                |_| false
            )
            .is_err());
        assert_eq!(engine.snapshot(b"target"), before);
        assert!(!engine.watch_dirty(b"target", &watch, 1));
        assert_eq!(engine.execute(&command, 1, |_| true), Reply::Integer(1));
        assert_eq!(engine.get(b"target", 1).unwrap().expires_at_ms, None);
        assert!(engine.watch_dirty(b"target", &watch, 1));
        let mut alias = command;
        alias[1] = b"source";
        assert_eq!(engine.execute(&alias, 1, |_| true), Reply::Integer(1));
        assert_eq!(
            engine.execute(&[b"GEODIST", b"source", b"member", b"member"], 1, |_| true),
            bulk("0.0000".to_owned())
        );
    }
}
