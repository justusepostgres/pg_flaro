//! Redis-style shortest double text and its fixed/scientific notation boundaries.
pub(crate) fn double(value: f64) -> String {
    if !value.is_finite() || value == 0.0 {
        return value.to_string();
    }
    if value.abs() <= (1u64 << 62) as f64 && value.fract() == 0.0 {
        return (value as i64).to_string();
    }
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap();
    let exponent: i32 = exponent.parse().unwrap();
    let digits = mantissa.bytes().filter(u8::is_ascii_digit).count() as i32;
    let power = exponent - digits + 1;
    if (power >= 0 && exponent.abs() < digits + 7)
        || (power < 0 && (power > -7 || exponent.abs() < 4))
    {
        value.to_string()
    } else {
        format!("{mantissa}e{exponent:+}")
    }
}
