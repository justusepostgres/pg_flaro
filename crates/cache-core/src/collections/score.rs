//! Redis accepts C double syntax, including hexadecimal literals and infinities,
//! but rejects NaNs and values that overflow or underflow all the way to zero.
//! Use an explicit immutable C locale, never PostgreSQL's process-global locale.
use super::{Reply, Result};
use std::{ffi::CString, sync::OnceLock};
unsafe extern "C" {
    fn strtod_l(
        text: *const libc::c_char,
        end: *mut *mut libc::c_char,
        locale: libc::locale_t,
    ) -> f64;
}
static LOCALE: OnceLock<usize> = OnceLock::new();
pub(crate) fn parse(bytes: &[u8]) -> Result<f64> {
    let invalid = || Reply::Error("ERR value is not a valid float");
    if bytes.is_empty() || bytes[0].is_ascii_whitespace() {
        return Err(invalid());
    }
    let text = CString::new(bytes).map_err(|_| invalid())?;
    // SAFETY: newlocale creates an immutable object that lives for the process
    // lifetime. Sharing it does not change any thread's locale. CString provides
    // a terminated buffer; strtod_l's end pointer stays within that buffer.
    unsafe {
        let locale = *LOCALE.get_or_init(|| {
            libc::newlocale(libc::LC_NUMERIC_MASK, c"C".as_ptr(), std::ptr::null_mut()) as usize
        }) as libc::locale_t;
        if locale.is_null() {
            return Err(invalid());
        }
        #[cfg(target_vendor = "apple")]
        let errno = libc::__error();
        #[cfg(target_os = "linux")]
        let errno = libc::__errno_location();
        *errno = 0;
        let mut end = std::ptr::null_mut();
        let value = strtod_l(text.as_ptr(), &mut end, locale);
        if end != text.as_ptr().add(bytes.len()).cast_mut()
            || value.is_nan()
            || *errno == libc::EINVAL
            || *errno == libc::ERANGE && (value.is_infinite() || value == 0.0)
        {
            return Err(invalid());
        }
        Ok(if value == 0.0 { 0.0 } else { value })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn redis_double_syntax_and_range_errors() {
        assert_eq!(parse(b"0x1.8p+1").unwrap(), 3.0);
        assert_eq!(parse(b"+Infinity").unwrap(), f64::INFINITY);
        assert_eq!(parse(b"1e-320").unwrap(), 1e-320);
        for text in [
            b"NaN".as_slice(),
            b"NaN(foo)",
            b"1e999",
            b"1e-999",
            b" 1",
            b"1 ",
            b"1\0",
            b"",
        ] {
            assert!(parse(text).is_err(), "{text:?}");
        }
    }
}
