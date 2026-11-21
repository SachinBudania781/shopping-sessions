use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use rayon::prelude::*;
use std::{fs, io, path::Path};

pub struct Parts(Vec<Mmap>);

impl Parts {
    pub fn open(path: &Path) -> io::Result<Self> {
        let paths = if path.is_dir() {
            fs::read_dir(path)?
                .map(|entry| entry.map(|e| e.path()))
                .collect::<io::Result<Vec<_>>>()?
                .into_iter()
                .filter(|p| p.extension().is_some_and(|ext| ext == "csv"))
                .collect()
        } else {
            vec![path.to_owned()]
        };
        let maps = paths
            .par_iter()
            .map(|p| {
                let file = fs::File::open(p)?;
                // Input is mounted read-only for the lifetime of the process.
                unsafe { Mmap::map(&file) }
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self(maps))
    }

    pub fn chunks(&self) -> Vec<&[u8]> {
        const CHUNK: usize = 2 << 20;
        let mut chunks = Vec::new();
        for file in &self.0 {
            let Some(header) = memchr(b'\n', file) else {
                continue;
            };
            let mut data = &file[header + 1..];
            while data.len() > CHUNK {
                let Some(next) = memchr(b'\n', &data[CHUNK..]) else {
                    break;
                };
                let (chunk, rest) = data.split_at(CHUNK + next + 1);
                chunks.push(chunk);
                data = rest;
            }
            if !data.is_empty() {
                chunks.push(data);
            }
        }
        chunks
    }
}

#[inline]
pub fn rows<const N: usize>(bytes: &[u8], mut visit: impl FnMut([&[u8]; N])) {
    let mut start = 0;
    for end in memchr_iter(b'\n', bytes).chain(std::iter::once(bytes.len())) {
        if end > start {
            if let Some(fields) = fields::<N>(&bytes[start..end]) {
                visit(fields);
            }
        }
        start = end + 1;
    }
}

#[inline]
fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut result = [&[][..]; N];
    let mut rest = line;
    for (i, slot) in result.iter_mut().enumerate() {
        let field = if i + 1 == N {
            if rest.contains(&b',') {
                return None;
            }
            rest
        } else {
            let end = rest.iter().position(|&b| b == b',')?;
            let field = &rest[..end];
            rest = &rest[end + 1..];
            field
        };
        let value = match field {
            [b'"', inner @ .., b'"'] => inner,
            _ => field,
        };
        if value.contains(&b'"') {
            return None;
        }
        *slot = value;
    }
    Some(result)
}

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;

#[inline]
pub fn number(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let mut n = 0i64;
    if s.len() <= 18 {
        for &b in s {
            let digit = b.wrapping_sub(b'0');
            if digit > 9 {
                return None;
            }
            n = n * 10 + i64::from(digit);
        }
    } else {
        for &b in s {
            let digit = b.wrapping_sub(b'0');
            if digit > 9 {
                return None;
            }
            n = n.checked_mul(10)?.checked_add(i64::from(digit))?;
        }
    }
    Some(n)
}

#[inline]
fn pair(a: u8, b: u8) -> Option<i32> {
    let (a, b) = (a.wrapping_sub(b'0'), b.wrapping_sub(b'0'));
    if a > 9 || b > 9 {
        return None;
    }
    Some(i32::from(a) * 10 + i32::from(b))
}

#[inline]
pub fn timestamp(s: &[u8]) -> Option<i64> {
    if !matches!(s.len(), 20 | 25) || s[4] != b'-' {
        return number(s);
    }
    if s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = 100 * pair(s[0], s[1])? + pair(s[2], s[3])?;
    let month = pair(s[5], s[6])?;
    let day = pair(s[8], s[9])?;
    let hour = pair(s[11], s[12])?;
    let minute = pair(s[14], s[15])?;
    let second = pair(s[17], s[18])?;
    let last = match month {
        2 => 28 + i32::from(year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)),
        4 | 6 | 9 | 11 => 30,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => return None,
    };
    if day == 0 || day > last || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let offset = if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        let h = pair(s[20], s[21])?;
        let m = pair(s[23], s[24])?;
        if s[22] != b':' || h > 23 || m > 59 {
            return None;
        }
        match s[19] {
            b'+' => h * 60 + m,
            b'-' => -(h * 60 + m),
            _ => return None,
        }
    };
    // March starts the computational year, putting leap day at its end.
    let y = year - i32::from(month <= 2);
    let m = if month <= 2 { month + 9 } else { month - 3 };
    let days =
        365 * y + y.div_euclid(4) - y.div_euclid(100) + y.div_euclid(400) + (153 * m + 2) / 5 + day
            - 1
            - 719468;
    Some(i64::from(days) * DAY + i64::from(((hour * 60 + minute - offset) * 60 + second) * 1000))
}
