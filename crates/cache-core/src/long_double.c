/* Keep native long double out of the Rust FFI ABI. */
#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <locale.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef __APPLE__
#include <xlocale.h>
#endif

/* Redis's long-double input/output buffer ceiling. */
#define LD_CAPACITY 5120

static int parse(const unsigned char *bytes, size_t len, locale_t locale,
                 long double *value) {
    if (len == 0 || len >= LD_CAPACITY || isspace(bytes[0])) return 0;
    char input[LD_CAPACITY];
    memcpy(input, bytes, len);
    input[len] = 0;
    char *end;
    errno = 0;
    *value = strtold_l(input, &end, locale);
    return end == input + len && !isnan(*value) && errno != EINVAL &&
        !(errno == ERANGE && (isinf(*value) || *value == 0));
}

/* 0: valid finite operand, 1: invalid float, 2: explicit infinity. */
int pgcache_ld_validate(const unsigned char *bytes, size_t len, void *locale) {
    long double value;
    if (!parse(bytes, len, (locale_t)locale, &value)) return 1;
    return isinf(value) ? 2 : 0;
}

/* Positive: output length; negative: old/increment/result/format error. */
int pgcache_ld_add(const unsigned char *old, size_t old_len,
                  const unsigned char *incr, size_t incr_len,
                  void *locale, unsigned char *out, size_t capacity) {
    long double left, right;
    if (!parse(old, old_len, (locale_t)locale, &left)) return -1;
    if (!parse(incr, incr_len, (locale_t)locale, &right)) return -2;
    long double result = left + right;
    if (!isfinite(result)) return -3;
    /* uselocale affects this thread only, and the previous locale is restored
       before returning. No PostgreSQL callbacks occur within this scope. */
    locale_t previous = uselocale((locale_t)locale);
    if (!previous) return -4;
    int length = snprintf((char *)out, capacity, "%.17Lf", result);
    uselocale(previous);
    if (length <= 0 || (size_t)length >= capacity) return -4;
    while (length > 0 && out[length - 1] == '0') length--;
    if (length > 0 && out[length - 1] == '.') length--;
    if (length == 2 && out[0] == '-' && out[1] == '0') {
        out[0] = '0';
        length = 1;
    }
    out[length] = 0;
    return length;
}

/* Redis blocking timeouts: native long double seconds, rounded up to ms.
   0 success; 1 invalid float; 2 range error; 3 negative timeout. */
int pgcache_ld_timeout(const unsigned char *bytes, size_t len, void *locale,
                       long long now, long long *deadline) {
    long double value;
    if (!parse(bytes, len, (locale_t)locale, &value)) return 1;
    value = ceill(value * 1000.0L);
    if (value < 0) return 3;
    /* Cast only after proving representability, including explicit infinity. */
    if (value >= 9223372036854775808.0L) return 2;
    long long millis = (long long)value;
    if (millis > 0 && millis > 9223372036854775807LL - now) return 2;
    *deadline = millis ? now + millis : 0;
    return 0;
}

/* Preserve the native estimator's integer rounding/overflow behavior. */
long long pgcache_hll_round(double value) {
    return llroundl((long double)value);
}
