//! Redis geospatial values share sorted-set storage and 52-bit Morton scores.
mod cover;
mod search;
use super::{eq, Engine, Mutations};
use crate::{collections, protocol::Reply, value::Collection};
const LAT: f64 = 85.05112878;
const SCALE: f64 = (1u64 << 26) as f64;

pub(super) fn execute(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    if ![
        b"GEOADD".as_slice(),
        b"GEOPOS",
        b"GEOHASH",
        b"GEODIST",
        b"GEOSEARCH",
        b"GEOSEARCHSTORE",
        b"GEORADIUS",
        b"GEORADIUS_RO",
        b"GEORADIUSBYMEMBER",
        b"GEORADIUSBYMEMBER_RO",
    ]
    .iter()
    .any(|name| eq(args[0], name))
    {
        return None;
    }
    Some(run(engine, args, now, submit).unwrap_or_else(|error| error))
}
fn run(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply, Reply> {
    crate::cluster::validate_command(a)?;
    if eq(a[0], b"GEOSEARCH")
        || eq(a[0], b"GEOSEARCHSTORE")
        || a[0].to_ascii_lowercase().starts_with(b"georadius")
    {
        return search::run(engine, a, now, submit);
    }
    if eq(a[0], b"GEOADD") {
        let mut start = 2;
        let (mut nx, mut xx) = (false, false);
        while start < a.len() {
            let option = cstring(a[start]);
            if eq(option, b"NX") {
                nx = true;
            } else if eq(option, b"XX") {
                xx = true;
            } else if !eq(option, b"CH") {
                break;
            }
            start += 1;
        }
        if start == a.len() || !(a.len() - start).is_multiple_of(3) || nx && xx {
            return Err(collections::SYNTAX);
        }
        let mut converted = vec![b"ZADD".to_vec(), a[1].to_vec()];
        converted.extend(a[2..start].iter().map(|s| cstring(s).to_vec()));
        for triple in a[start..].chunks_exact(3) {
            let (longitude, latitude) = coordinates(triple[0], triple[1])?;
            converted.push(encode(longitude, latitude, LAT).to_string().into_bytes());
            converted.push(triple[2].to_vec());
        }
        let borrowed: Vec<_> = converted.iter().map(Vec::as_slice).collect();
        return Ok(collections::execute(engine, &borrowed, now, submit).unwrap());
    }
    let mut unit = 1.0;
    if eq(a[0], b"GEODIST") {
        if a.len() > 5 {
            return Err(collections::SYNTAX);
        }
        if a.len() == 5 {
            unit = self::unit(a[4])?;
        }
    }
    let zset = match engine.get(a[1], now) {
        None => None,
        Some(entry) => match entry.value.collection() {
            Some(Collection::ZSet(zset)) => Some(zset),
            _ => return Err(collections::WRONGTYPE),
        },
    };
    let position = |member: &[u8]| zset.and_then(|z| z.scores.get(member)).map(|s| decode(s.0));
    if eq(a[0], b"GEODIST") {
        return Ok(match (position(a[2]), position(a[3])) {
            (Some(first), Some(second)) => bulk(format!("{:.4}", distance(first, second) / unit)),
            _ => Reply::Bulk(None),
        });
    }
    Ok(Reply::Array(
        a[2..]
            .iter()
            .map(|member| {
                let Some((longitude, latitude)) = position(member) else {
                    return if eq(a[0], b"GEOPOS") {
                        Reply::NullArray
                    } else {
                        Reply::Bulk(None)
                    };
                };
                if eq(a[0], b"GEOPOS") {
                    Reply::Array(vec![
                        bulk(format_coordinate(longitude)),
                        bulk(format_coordinate(latitude)),
                    ])
                } else {
                    let bits = encode(longitude, latitude, 90.0);
                    let alphabet = b"0123456789bcdefghjkmnpqrstuvwxyz";
                    let mut hash = vec![b'0'; 11];
                    for (i, byte) in hash[..10].iter_mut().enumerate() {
                        *byte = alphabet[((bits >> (52 - (i + 1) * 5)) & 31) as usize];
                    }
                    Reply::Bulk(Some(hash))
                }
            })
            .collect(),
    ))
}
fn coordinate(bytes: &[u8]) -> Result<f64, Reply> {
    let value = collections::score::parse(bytes)?;
    Ok(if value == 0.0 && bytes.first() == Some(&b'-') {
        -0.0
    } else {
        value
    })
}
fn cstring(bytes: &[u8]) -> &[u8] {
    bytes.split(|b| *b == 0).next().unwrap()
}
fn bulk(value: String) -> Reply {
    Reply::Bulk(Some(value.into_bytes()))
}
fn encode(longitude: f64, latitude: f64, latitude_range: f64) -> u64 {
    let lat = ((latitude + latitude_range) / (2.0 * latitude_range) * SCALE) as u32;
    let lon = ((longitude + 180.0) / 360.0 * SCALE) as u32;
    let mut bits = 0;
    for i in 0..32 {
        bits |= ((lat as u64 >> i) & 1) << (i * 2);
        bits |= ((lon as u64 >> i) & 1) << (i * 2 + 1);
    }
    bits
}
fn decode(score: f64) -> (f64, f64) {
    let bits = score as u64;
    let (mut lat, mut lon) = (0u32, 0u32);
    for i in 0..32 {
        lat |= (((bits >> (i * 2)) & 1) as u32) << i;
        lon |= (((bits >> (i * 2 + 1)) & 1) as u32) << i;
    }
    let center = |index: u32, range: f64| {
        let min = (index as f64 / SCALE).mul_add(2.0 * range, -range);
        let max = (index.wrapping_add(1) as f64 / SCALE).mul_add(2.0 * range, -range);
        ((min + max) / 2.0).clamp(-range, range)
    };
    (center(lon, 180.0), center(lat, LAT))
}
fn distance((lon1, lat1): (f64, f64), (lon2, lat2): (f64, f64)) -> f64 {
    let radians = |value: f64| value * (std::f64::consts::PI / 180.0);
    let (lat1, lat2) = (radians(lat1), radians(lat2));
    let u = ((lat2 - lat1) / 2.0).sin();
    let v = ((radians(lon2) - radians(lon1)) / 2.0).sin();
    if v == 0.0 {
        return 6372797.560856 * (lat2 - lat1).abs();
    }
    2.0 * 6372797.560856 * u.mul_add(u, lat1.cos() * lat2.cos() * v * v).sqrt().asin()
}

fn coordinates(lon: &[u8], lat: &[u8]) -> Result<(f64, f64), Reply> {
    let longitude = coordinate(lon)?;
    let latitude = coordinate(lat)?;
    if !(-180.0..=180.0).contains(&longitude) || !(-LAT..=LAT).contains(&latitude) {
        return Err(Reply::OwnedError(format!(
            "ERR invalid longitude,latitude pair {longitude:.6},{latitude:.6}"
        )));
    }
    Ok((longitude, latitude))
}
fn unit(value: &[u8]) -> Result<f64, Reply> {
    match cstring(value).to_ascii_lowercase().as_slice() {
        b"m" => Ok(1.0),
        b"km" => Ok(1000.0),
        b"ft" => Ok(0.3048),
        b"mi" => Ok(1609.34),
        _ => Err(Reply::Error(
            "ERR unsupported unit provided. please use M, KM, FT, MI",
        )),
    }
}

fn format_coordinate(value: f64) -> String {
    crate::protocol::number::double(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_preserve_ttl_and_require_persistence_admission() {
        let mut engine = Engine::new(1 << 20);
        let command = [
            b"GEOADD".as_slice(),
            b"geo",
            b"13.361389",
            b"38.115556",
            b"Palermo",
        ];
        assert_eq!(engine.execute(&command, 0, |_| true), Reply::Integer(1));
        engine.execute(&[b"PEXPIREAT", b"geo", b"5000"], 0, |_| true);
        let before = engine.snapshot(b"geo");
        let watch = engine.watch(b"geo", 0);
        assert_eq!(
            engine.execute(&[b"GEOADD", b"geo", b"1", b"2", b"Palermo"], 1, |_| false),
            Reply::Error("MISCONF persistence backlog full or unavailable")
        );
        assert_eq!(engine.snapshot(b"geo"), before);
        assert!(!engine.watch_dirty(b"geo", &watch, 1));
        assert_eq!(
            engine.execute(&command, 1, |_| panic!("unchanged position")),
            Reply::Integer(0)
        );
        assert_eq!(
            engine.execute(
                &[b"GEOADD", b"geo", b"CH", b"1", b"2", b"Palermo"],
                1,
                |_| true
            ),
            Reply::Integer(1)
        );
        assert_eq!(engine.get(b"geo", 1).unwrap().expires_at_ms, Some(5000));
        assert!(engine.watch_dirty(b"geo", &watch, 1));
    }
}
