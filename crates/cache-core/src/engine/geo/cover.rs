//! Morton-prefix ranges in Redis's center/cardinal/diagonal traversal order.
#[cfg(test)]
use super::encode;
use super::LAT;
const EARTH: f64 = 6372797.560856;
const RAD: f64 = std::f64::consts::PI / 180.0;
const OFFSETS: [(i32, i32); 9] = [
    (0, 0),
    (0, 1),
    (0, -1),
    (1, 0),
    (-1, 0),
    (1, 1),
    (-1, 1),
    (1, -1),
    (-1, -1),
];

#[derive(Clone, Copy)]
struct Cell {
    x: u32,
    y: u32,
    step: u32,
}
impl Cell {
    fn at((longitude, latitude): (f64, f64), step: u32) -> Self {
        let scale = (1u64 << step) as f64;
        Self {
            x: ((longitude + 180.0) / 360.0 * scale) as u32,
            y: ((latitude + LAT) / (2.0 * LAT) * scale) as u32,
            step,
        }
    }
    fn neighbor(self, dx: i32, dy: i32) -> Self {
        let mask = (1u32 << self.step) - 1;
        Self {
            x: if dx == 0 {
                self.x
            } else {
                self.x.wrapping_add_signed(dx) & mask
            },
            y: if dy == 0 {
                self.y
            } else {
                self.y.wrapping_add_signed(dy) & mask
            },
            ..self
        }
    }
    fn bounds(self) -> [f64; 4] {
        let scale = (1u64 << self.step) as f64;
        [
            (self.x as f64 / scale).mul_add(360.0, -180.0),
            (self.y as f64 / scale).mul_add(2.0 * LAT, -LAT),
            (self.x.wrapping_add(1) as f64 / scale).mul_add(360.0, -180.0),
            (self.y.wrapping_add(1) as f64 / scale).mul_add(2.0 * LAT, -LAT),
        ]
    }
    fn bits(self) -> u64 {
        let mut bits = 0;
        for i in 0..32 {
            bits |= (((self.x >> i) & 1) as u64) << (2 * i + 1);
            bits |= (((self.y >> i) & 1) as u64) << (2 * i);
        }
        bits
    }
}

pub(super) fn ranges(
    origin: (f64, f64),
    radius: f64,
    half_width: f64,
    half_height: f64,
) -> Vec<(f64, f64)> {
    let (longitude, latitude) = origin;
    let lat_delta = (half_height / EARTH) / RAD;
    let edge_latitude = if latitude < 0.0 {
        latitude - lat_delta
    } else {
        latitude + lat_delta
    };
    let lon_delta = (half_width / EARTH / (edge_latitude * RAD).cos()) / RAD;
    let bounds = [
        longitude - lon_delta,
        latitude - lat_delta,
        longitude + lon_delta,
        latitude + lat_delta,
    ];
    let mut step = 26i32;
    if radius != 0.0 {
        step = 1;
        let mut size = radius;
        while size < 20037726.37 {
            size *= 2.0;
            step += 1;
        }
        step -= 2;
        if latitude.abs() > 66.0 {
            step -= 1;
        }
        if latitude.abs() > 80.0 {
            step -= 1;
        }
        step = step.clamp(1, 26);
    }
    let mut cell = Cell::at(origin, step as u32);
    if step > 1
        && (cell.neighbor(0, 1).bounds()[3] < bounds[3]
            || cell.neighbor(0, -1).bounds()[1] > bounds[1]
            || cell.neighbor(1, 0).bounds()[2] < bounds[2]
            || cell.neighbor(-1, 0).bounds()[0] > bounds[0])
    {
        step -= 1;
        cell = Cell::at(origin, step as u32);
    }
    let area = cell.bounds();
    let mut ranges = Vec::with_capacity(9);
    let mut previous = None;
    for (index, (dx, dy)) in OFFSETS.into_iter().enumerate() {
        if step >= 2
            && ((dy < 0 && area[1] < bounds[1])
                || (dy > 0 && area[3] > bounds[3])
                || (dx < 0 && area[0] < bounds[0])
                || (dx > 0 && area[2] > bounds[2]))
        {
            continue;
        }
        let bits = cell.neighbor(dx, dy).bits();
        if previous.is_some_and(|(last_index, last_bits)| last_index != 0 && last_bits == bits) {
            continue;
        }
        let shift = 52 - 2 * step as u32;
        ranges.push(((bits << shift) as f64, ((bits + 1) << shift) as f64));
        previous = Some((index, bits));
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_cells_preserve_morton_prefixes_at_coordinate_extrema() {
        for point in [
            (0.0, 0.0),
            (13.361389, 38.115556),
            (180.0, LAT),
            (-180.0, -LAT),
        ] {
            for step in 1..=26 {
                assert_eq!(
                    Cell::at(point, step).bits(),
                    encode(point.0, point.1, LAT) >> (52 - 2 * step)
                );
            }
        }
    }
}
