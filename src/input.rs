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
    #[cfg(target_arch = "x86_64")]
    if bytes.len() >= 128 && std::is_x86_feature_detected!("avx2") {
        // The runtime check covers the block scanner's target feature.
        unsafe {
            crate::scan::rows(bytes, visit);
        }
        return;
    }
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
pub(crate) fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
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
    if let [a, b, c, d, e, tail @ ..] = s {
        if let Ok(last) = <[u8; 8]>::try_from(tail) {
            let word = u64::from_le_bytes(last);
            let invalid =
                word.wrapping_sub(0x3030303030303030) | word.wrapping_add(0x4646464646464646);
            if invalid & 0x8080808080808080 != 0 {
                return None;
            }
            let pairs = (word & 0x0f0f0f0f0f0f0f0f).wrapping_mul(2561) >> 8;
            let quads = (pairs & 0x00ff00ff00ff00ff).wrapping_mul(6553601) >> 16;
            let suffix = (quads & 0x0000ffff0000ffff).wrapping_mul(42949672960001) >> 32;
            let lead = a.wrapping_sub(b'0');
            if lead > 9 {
                return None;
            }
            let prefix = i32::from(lead) * 10000 + pair(*b, *c)? * 100 + pair(*d, *e)?;
            return Some(i64::from(prefix) * 100_000_000 + suffix as i64);
        }
    }
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

#[cfg(test)]
mod tests {
    use super::{fields, rows};

    #[test]
    fn block_scanner_matches_scalar_fields_at_every_alignment() {
        let mut bytes = Vec::new();
        for padding in 0..80 {
            for record in [
                &b",b,\"c\",d\n"[..],
                b",b,,d\n",
                b",b,\"\",d\r\n",
                b",b,c\",d\n",
                b",\"b,c,d\n",
                b",b,\"c\"\",d\n",
                b",b,c,d,\n",
                b",b,c\n",
                b",b,c,\x00\n",
                b"\n",
            ] {
                bytes.extend(std::iter::repeat_n(b'x', padding));
                bytes.extend_from_slice(record);
            }
        }
        bytes.extend_from_slice(b"a,b,\"c\",d");
        for shift in 0..32 {
            let sample = &bytes[shift..];
            let expected: Vec<_> = sample
                .split(|&b| b == b'\n')
                .filter_map(fields::<4>)
                .map(|f| f.map(<[u8]>::to_vec))
                .collect();
            let mut actual = Vec::new();
            rows::<4>(sample, |f| actual.push(f.map(<[u8]>::to_vec)));
            assert_eq!(actual, expected);
        }
    }
}

fn midnight(s: &[u8; 10]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' {
        return None;
    }
    let year = 100 * pair(s[0], s[1])? + pair(s[2], s[3])?;
    let month = pair(s[5], s[6])?;
    let day = pair(s[8], s[9])?;
    let last = match month {
        2 => 28 + i32::from(year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)),
        4 | 6 | 9 | 11 => 30,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => return None,
    };
    if day == 0 || day > last {
        return None;
    }
    let y = year - i32::from(month <= 2);
    let m = if month <= 2 { month + 9 } else { month - 3 };
    let days =
        365 * y + y.div_euclid(4) - y.div_euclid(100) + y.div_euclid(400) + (153 * m + 2) / 5 + day
            - 1
            - 719468;
    Some(i64::from(days) * DAY)
}

#[derive(Default)]
pub struct Calendar([([u8; 10], i64); 32]);

impl Calendar {
    #[inline]
    fn date(&mut self, date: &[u8; 10]) -> Option<i64> {
        let slot = &mut self.0[(usize::from(date[8]) * 10 + usize::from(date[9])) & 31];
        if slot.0 != *date {
            *slot = (*date, midnight(date)?);
        }
        Some(slot.1)
    }

    #[inline]
    pub fn on_time(&mut self, ingest: &[u8], ts: i64) -> bool {
        // ingest_ts is a valid whole-second UTC instant under the input contract.
        // For an ingest day at or before ts, even its last second is on time.
        if ingest.len() == 20 {
            if let Some(day) = self.date(ingest.first_chunk().unwrap()) {
                if day <= ts {
                    return true;
                }
                if day > ts + DAY {
                    return false;
                }
            }
        }
        self.timestamp(ingest).is_some_and(|t| t <= ts + DAY)
    }
    #[inline]
    pub fn timestamp(&mut self, s: &[u8]) -> Option<i64> {
        if !matches!(s.len(), 20 | 25) || s[4] != b'-' {
            return number(s);
        }
        if s[10] != b'T' || s[13] != b':' || s[16] != b':' {
            return None;
        }
        let date: &[u8; 10] = s[..10].try_into().unwrap();
        let day = self.date(date)?;
        let hour = pair(s[11], s[12])?;
        let minute = pair(s[14], s[15])?;
        let second = pair(s[17], s[18])?;
        if hour > 23 || minute > 59 || second > 59 {
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
        Some(day + i64::from(((hour * 60 + minute - offset) * 60 + second) * 1000))
    }
}
