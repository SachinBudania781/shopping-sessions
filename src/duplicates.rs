use rayon::prelude::*;

// A repeated ID always has the same prefix. Different IDs may share a
// fingerprint; both then go to the exact earliest-ingest reducer.
#[inline]
fn fingerprint(first: u64) -> u32 {
    let hash = (first ^ first.rotate_right(29)).wrapping_mul(0x9e3779b185ebca87);
    (hash ^ (hash >> 32)) as u32
}

#[inline]
fn contains_zero(n: u64) -> bool {
    n.wrapping_sub(0x0101010101010101) & !n & 0x8080808080808080 != 0
}

#[inline]
fn raw_fingerprint(line: &[u8]) -> u32 {
    let id = line.strip_prefix(b"\"").unwrap_or(line);
    let Some(bytes) = id.first_chunk::<8>() else {
        return 0;
    };
    let first = u64::from_le_bytes(*bytes);
    if contains_zero(first ^ 0x2c2c2c2c2c2c2c2c) || contains_zero(first ^ 0x2222222222222222) {
        return 0;
    }
    fingerprint(first)
}

const GROUPS: usize = 64;
pub struct DuplicateFilter {
    repeats: Vec<Vec<u64>>,
    mask: usize,
}

impl DuplicateFilter {
    pub fn build(chunks: &[&[u8]]) -> Self {
        let total_bytes: usize = chunks.iter().map(|part| part.len()).sum();
        let bits = (total_bytes / (4 * GROUPS))
            .max(64)
            .next_power_of_two()
            .min(1 << 26);
        let mask = bits - 1;
        let chunks: Vec<_> = chunks
            .par_iter()
            .map(|&bytes| {
                let mut groups: Vec<_> = (0..GROUPS)
                    .map(|_| Vec::<u32>::with_capacity(bytes.len() / (64 * GROUPS)))
                    .collect();
                let mut start = 0;
                for end in memchr::memchr_iter(b'\n', bytes).chain(std::iter::once(bytes.len())) {
                    if start < end {
                        let h = raw_fingerprint(&bytes[start..end]);
                        groups[(h >> 26) as usize].push(h);
                    }
                    start = end + 1;
                }
                groups
            })
            .collect();
        let mut grouped: Vec<Vec<Vec<u32>>> = (0..GROUPS).map(|_| Vec::new()).collect();
        for chunk in chunks {
            for (dest, values) in grouped.iter_mut().zip(chunk) {
                dest.push(values);
            }
        }
        let repeats = grouped
            .into_par_iter()
            .map(|group| {
                let mut seen = vec![0u64; bits / 64];
                let mut repeats = vec![0u64; bits / 64];
                for h in group.into_iter().flatten() {
                    let pos = h as usize & mask;
                    let bit = 1u64 << (pos & 63);
                    repeats[pos >> 6] |= seen[pos >> 6] & bit;
                    seen[pos >> 6] |= bit;
                }
                repeats
            })
            .collect();
        Self { repeats, mask }
    }

    #[inline]
    pub fn maybe_repeated(&self, id: &[u8]) -> bool {
        let h = id
            .first_chunk::<8>()
            .map_or(0, |p| fingerprint(u64::from_le_bytes(*p)));
        let pos = h as usize & self.mask;
        self.repeats[(h >> 26) as usize][pos >> 6] & (1u64 << (pos & 63)) != 0
    }
}
