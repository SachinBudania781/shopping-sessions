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
    if bytes.len() >= 1024
        && bytes.len() < u32::MAX as usize
        && std::is_x86_feature_detected!("avx2")
    {
        // The runtime check above covers the instructions in scan().
        let (delimiters, lines) = unsafe { vector::scan(bytes) };
        let mut at = 0;
        let mut start = 0;
        for (end, quotes) in lines {
            if delimiters.get(at + N - 1) == Some(&end) {
                let mut next = start;
                let mut wrapped = 0;
                let fields = std::array::from_fn(|i| {
                    let stop = delimiters[at + i] as usize;
                    let value = &bytes[next..stop];
                    next = stop + 1;
                    if quotes != 0 {
                        if let [b'"', inner @ .., b'"'] = value {
                            wrapped += 2;
                            return inner;
                        }
                    }
                    value
                });
                // Every quote has to belong to one of these whole-field pairs.
                if wrapped == quotes {
                    visit(fields);
                }
                at += N;
            } else {
                while at < delimiters.len() && delimiters[at] <= end {
                    at += 1;
                }
            }
            start = end as usize + 1;
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

#[cfg(target_arch = "x86_64")]
mod vector {
    use std::arch::x86_64::*;

    // One scan indexes field ends and counts quotes per line. The row decoder
    // can then validate quoted fields without searching inside each value.
    #[target_feature(enable = "avx2")]
    pub unsafe fn scan(bytes: &[u8]) -> (Vec<u32>, Vec<(u32, u32)>) {
        let mut delimiters = Vec::with_capacity(bytes.len() / 10);
        let mut lines = Vec::with_capacity(bytes.len() / 52);
        let mut quote_count = 0;
        let mut at = 0;
        while at + 32 <= bytes.len() {
            // The loop guard ensures that all 32 loaded bytes are in bounds.
            let v = unsafe { _mm256_loadu_si256(bytes.as_ptr().add(at).cast()) };
            let comma = _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b',' as i8));
            let lf = _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'\n' as i8));
            let q = _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'"' as i8));
            let mut ends = _mm256_movemask_epi8(_mm256_or_si256(comma, lf)) as u32;
            let mut newlines = _mm256_movemask_epi8(lf) as u32;
            let mut quotes = _mm256_movemask_epi8(q) as u32;
            while ends != 0 {
                delimiters.push(at as u32 + ends.trailing_zeros());
                ends &= ends - 1;
            }
            while newlines != 0 {
                let before = (newlines & newlines.wrapping_neg()) - 1;
                quote_count += (quotes & before).count_ones();
                lines.push((at as u32 + newlines.trailing_zeros(), quote_count));
                quote_count = 0;
                quotes &= !before;
                newlines &= newlines - 1;
            }
            quote_count += quotes.count_ones();
            at += 32;
        }
        for (i, &byte) in bytes[at..].iter().enumerate() {
            let pos = (at + i) as u32;
            match byte {
                b',' => delimiters.push(pos),
                b'"' => quote_count += 1,
                b'\n' => {
                    delimiters.push(pos);
                    lines.push((pos, quote_count));
                    quote_count = 0;
                }
                _ => (),
            }
        }
        if bytes.last() != Some(&b'\n') {
            delimiters.push(bytes.len() as u32);
            lines.push((bytes.len() as u32, quote_count));
        }
        (delimiters, lines)
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
    pub fn timestamp(&mut self, s: &[u8]) -> Option<i64> {
        if !matches!(s.len(), 20 | 25) || s[4] != b'-' {
            return number(s);
        }
        if s[10] != b'T' || s[13] != b':' || s[16] != b':' {
            return None;
        }
        let date: &[u8; 10] = s[..10].try_into().unwrap();
        let slot = &mut self.0[(usize::from(s[8]) * 10 + usize::from(s[9])) & 31];
        if slot.0 != *date {
            *slot = (*date, midnight(date)?);
        }
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
        Some(slot.1 + i64::from(((hour * 60 + minute - offset) * 60 + second) * 1000))
    }
}
