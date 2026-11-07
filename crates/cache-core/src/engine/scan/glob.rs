//! Redis byte glob syntax, using iterative star backtracking.
enum Token {
    Star,
    Any,
    Byte(u8),
    Class([u64; 4], bool),
}
pub(crate) struct Pattern(Vec<Token>);
impl Pattern {
    pub(crate) fn new(pattern: &[u8]) -> Self {
        Self::compile(pattern, false)
    }
    pub(crate) fn nocase(pattern: &[u8]) -> Self {
        Self::compile(pattern, true)
    }
    fn compile(pattern: &[u8], nocase: bool) -> Self {
        let mut tokens = Vec::new();
        let mut i = 0;
        while i < pattern.len() {
            match pattern[i] {
                b'*' => {
                    if !matches!(tokens.last(), Some(Token::Star)) {
                        tokens.push(Token::Star);
                    }
                }
                b'?' => tokens.push(Token::Any),
                b'\\' if i + 1 < pattern.len() => {
                    i += 1;
                    tokens.push(byte(pattern[i], nocase));
                }
                b'[' => {
                    i += 1;
                    let negate = pattern.get(i) == Some(&b'^');
                    if negate {
                        i += 1;
                    }
                    let mut bits = [0u64; 4];
                    let mut insert = |c: u8| bits[c as usize / 64] |= 1 << (c % 64);
                    while i < pattern.len() && pattern[i] != b']' {
                        if pattern[i] == b'\\' && i + 1 < pattern.len() {
                            i += 1;
                            insert(pattern[i]);
                        } else if i + 2 < pattern.len() && pattern[i + 1] == b'-' {
                            // Redis compares platform C chars, including binary ranges.
                            let a = pattern[i] as libc::c_char as i16;
                            let b = pattern[i + 2] as libc::c_char as i16;
                            if nocase {
                                // Redis orders raw endpoints before folding case.
                                let lo = (a.min(b) as u8).to_ascii_lowercase();
                                let hi = (a.max(b) as u8).to_ascii_lowercase();
                                for c in 0..=u8::MAX {
                                    if (lo..=hi).contains(&c.to_ascii_lowercase()) {
                                        insert(c);
                                    }
                                }
                            } else {
                                for c in a.min(b)..=a.max(b) {
                                    insert(c as u8);
                                }
                            }
                            i += 2;
                        } else {
                            insert(pattern[i]);
                            if nocase {
                                insert(pattern[i].to_ascii_lowercase());
                                insert(pattern[i].to_ascii_uppercase());
                            }
                        }
                        i += 1;
                    }
                    tokens.push(Token::Class(bits, negate));
                }
                c => tokens.push(byte(c, nocase)),
            }
            i += 1;
        }
        Self(tokens)
    }
    pub(crate) fn matches(&self, value: &[u8]) -> bool {
        if value.is_empty() {
            return self.0.is_empty();
        }
        let (mut p, mut s, mut star) = (0, 0, None);
        while s < value.len() {
            if matches!(self.0.get(p), Some(Token::Star)) {
                p += 1;
                star = Some((p, s));
                continue;
            }
            let matched = match self.0.get(p) {
                Some(Token::Any) => true,
                Some(Token::Byte(c)) => *c == value[s],
                Some(Token::Class(bits, negate)) => {
                    (bits[value[s] as usize / 64] & (1 << (value[s] % 64)) != 0) != *negate
                }
                _ => false,
            };
            if matched {
                p += 1;
                s += 1;
            } else if let Some((resume, start)) = star {
                s = start + 1;
                p = resume;
                star = Some((resume, s));
            } else {
                return false;
            }
        }
        while matches!(self.0.get(p), Some(Token::Star)) {
            p += 1;
        }
        p == self.0.len()
    }
}

fn byte(c: u8, nocase: bool) -> Token {
    if !nocase {
        return Token::Byte(c);
    }
    let mut bits = [0; 4];
    for c in [c.to_ascii_lowercase(), c.to_ascii_uppercase()] {
        bits[c as usize / 64] |= 1 << (c % 64);
    }
    Token::Class(bits, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn command_patterns_fold_case_without_changing_scan_patterns() {
        assert!(Pattern::nocase(b"*BLOCK*").matches(b"client|unblock"));
        assert!(!Pattern::new(b"*BLOCK*").matches(b"client|unblock"));
        assert!(Pattern::nocase(b"[A-Z]*").matches(b"get"));
        assert!(!Pattern::nocase(b"[Z-a]*").matches(b"get"));
        assert!(!Pattern::nocase(br"[\A]*").matches(b"append"));
        assert!(Pattern::nocase(br"\A*").matches(b"append"));
    }
}
