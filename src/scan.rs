use std::arch::x86_64::*;

#[derive(Clone, Copy)]
struct Masks {
    commas: u64,
    quotes: u64,
    lines: u64,
}

#[target_feature(enable = "avx2")]
unsafe fn masks(p: *const u8) -> Masks {
    // The caller supplies a readable 64-byte window.
    let a = unsafe { _mm256_loadu_si256(p.cast()) };
    let b = unsafe { _mm256_loadu_si256(p.add(32).cast()) };
    macro_rules! matches {
        ($byte:expr) => {{
            let v = _mm256_set1_epi8($byte as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32;
            u64::from(lo) | (u64::from(hi) << 32)
        }};
    }
    Masks {
        commas: matches!(b','),
        quotes: matches!(b'"'),
        lines: matches!(b'\n'),
    }
}

#[inline]
fn decode<const N: usize>(
    window: &[u8],
    end: usize,
    mut low: u64,
    mut high: u64,
    quotes: u32,
) -> Option<[&[u8]; N]> {
    if low.count_ones() + high.count_ones() != (N - 1) as u32 {
        return None;
    }
    let mut start = 0;
    let mut used = 0;
    let mut fields = [&[][..]; N];
    for (i, field) in fields.iter_mut().enumerate() {
        let stop = if i + 1 == N {
            end
        } else if low != 0 {
            let k = low.trailing_zeros() as usize;
            low &= low - 1;
            k
        } else {
            let k = 64 + high.trailing_zeros() as usize;
            high &= high - 1;
            k
        };
        // All comma positions are strictly increasing and below the LF at end.
        let trim = usize::from(
            (window[start] == b'"')
                & (window[stop.saturating_sub(1)] == b'"')
                & (stop - start >= 2),
        );
        used += trim as u32 * 2;
        *field = &window[start + trim..stop - trim];
        start = stop + 1;
    }
    (used == quotes).then_some(fields)
}

#[target_feature(enable = "avx2")]
pub unsafe fn rows<const N: usize>(bytes: &[u8], visit: impl FnMut([&[u8]; N])) {
    scan(bytes, visit, |p| unsafe { masks(p) })
}

#[target_feature(enable = "avx512bw")]
pub unsafe fn rows512<const N: usize>(bytes: &[u8], visit: impl FnMut([&[u8]; N])) {
    scan(bytes, visit, |p| {
        // scan supplies a readable cache line for each call.
        let v = unsafe { _mm512_loadu_si512(p.cast()) };
        Masks {
            commas: _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8)),
            quotes: _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8)),
            lines: _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8)),
        }
    })
}

#[inline(always)]
fn scan<const N: usize>(
    mut bytes: &[u8],
    mut visit: impl FnMut([&[u8]; N]),
    masks: impl Fn(*const u8) -> Masks,
) {
    // Most rows fit in one or two cache lines. Consume their structural
    // masks immediately rather than materializing a second copy of the CSV index.
    while bytes.len() >= 128 {
        let lo = masks(bytes.as_ptr());
        let (end, low, high, quotes) = if lo.lines != 0 {
            let end = lo.lines.trailing_zeros();
            let before = (1u64 << end) - 1;
            (
                end as usize,
                lo.commas & before,
                0,
                (lo.quotes & before).count_ones(),
            )
        } else {
            let hi = masks(bytes[64..].as_ptr());
            if hi.lines == 0 {
                let end = memchr::memchr(b'\n', &bytes[128..]).map_or(bytes.len(), |i| i + 128);
                if let Some(fields) = crate::input::fields(&bytes[..end]) {
                    visit(fields);
                }
                if end == bytes.len() {
                    return;
                }
                bytes = &bytes[end + 1..];
                continue;
            }
            let end = hi.lines.trailing_zeros();
            let before = (1u64 << end) - 1;
            (
                64 + end as usize,
                lo.commas,
                hi.commas & before,
                lo.quotes.count_ones() + (hi.quotes & before).count_ones(),
            )
        };
        if let Some(fields) = decode(bytes, end, low, high, quotes) {
            visit(fields);
        }
        bytes = &bytes[end + 1..];
    }
    for line in bytes.split(|&b| b == b'\n') {
        if let Some(fields) = crate::input::fields(line) {
            visit(fields);
        }
    }
}
