use crate::ids::{Id, SHARDS};
use crate::input::{self, Calendar, Parts};
use crate::pipeline::{self, Context, EventRecord, PersonCache, Result};
use rayon::prelude::*;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const GROUPS: usize = 512;
const INVALID: u32 = u32::MAX;
const INDEX_BITS: u32 = 32 - SHARDS.trailing_zeros();
const INDEX_MASK: u32 = (1 << INDEX_BITS) - 1;

#[derive(Clone, Copy)]
struct Row {
    id: Id,
    ticket: u32,
    ingest_offset: u32,
}
struct Part {
    chunk: usize,
    rows: Vec<Row>,
}

struct Rejected {
    base: [usize; SHARDS],
    bits: Vec<AtomicU64>,
}

impl Rejected {
    fn new<R>(rows: &[Vec<R>]) -> Self {
        let mut base = [0; SHARDS];
        let mut count = 0;
        for (offset, shard) in base.iter_mut().zip(rows) {
            *offset = count;
            count += shard.len();
        }
        Self {
            base,
            bits: (0..count.div_ceil(64)).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn reject(&self, ticket: u32) {
        if ticket != INVALID {
            let i = self.base[(ticket >> INDEX_BITS) as usize] + (ticket & INDEX_MASK) as usize;
            self.bits[i / 64].fetch_or(1 << (i % 64), Ordering::Relaxed);
        }
    }

    fn keep<R>(mut self, mut rows: Vec<Vec<R>>) -> Vec<Vec<R>> {
        for (&offset, shard) in self.base.iter().zip(&mut rows) {
            let mut i = offset;
            shard.retain(|_| {
                let keep = (*self.bits[i / 64].get_mut() & (1 << (i % 64))) == 0;
                i += 1;
                keep
            });
        }
        rows
    }
}

fn parse<R: EventRecord>(ctx: &Context, bytes: &[u8]) -> (Vec<Vec<Row>>, Vec<Vec<R>>, Rejected) {
    let mut parts: Vec<_> = (0..GROUPS)
        .map(|_| Vec::with_capacity(bytes.len() / (GROUPS * 72)))
        .collect();
    let mut people = pipeline::buckets::<R>();
    let mut calendar = Calendar::default();
    let mut cache = PersonCache::new();
    input::rows(bytes, |fields: [&[u8]; 7]| {
        if fields[0].is_empty() || fields[3].len() != 20 {
            return;
        }
        let id = ctx.ids.key(fields[0]);
        let ticket = match pipeline::validate_event(ctx, &mut calendar, fields) {
            None => INVALID,
            Some(mut event) => {
                event.person = cache.person(ctx, event.person);
                let shard = event.person.shard();
                let position = people[shard].len();
                assert!(
                    position < INDEX_MASK as usize,
                    "too many rows in a CSV chunk"
                );
                people[shard].push(R::from_event(ctx, event));
                ((shard as u32) << INDEX_BITS) | position as u32
            }
        };
        let offset = fields[3].as_ptr() as usize - bytes.as_ptr() as usize;
        let bucket = (id.hash() >> (64 - GROUPS.trailing_zeros())) as usize;
        parts[bucket].push(Row {
            id,
            ticket,
            ingest_offset: u32::try_from(offset).expect("CSV chunk offset"),
        });
    });
    let rejected = Rejected::new(&people);
    (parts, people, rejected)
}

fn select_earliest(chunks: &[&[u8]], rejected: &[Rejected], parts: &[Part]) {
    let count: usize = parts.iter().map(|p| p.rows.len()).sum();
    if count == 0 {
        return;
    }
    let longest = parts.iter().map(|p| p.rows.len()).max().unwrap();
    let row_bits = usize::BITS - (longest - 1).leading_zeros();
    let row_mask = (1usize << row_bits) - 1;
    // A fingerprint seen once cannot be a duplicate. Only ambiguous bins
    // need the exact table, whose entries point back to the original row.
    let bits = (count * 12).max(64).next_power_of_two();
    let mut seen = vec![0u64; bits / 64];
    let mut ambiguous = vec![0u64; bits / 64];
    let mut collisions = 0usize;
    for r in parts.iter().flat_map(|p| &p.rows) {
        let hash = r.id.hash();
        let bit = (hash ^ (hash >> 32)) as usize & (bits - 1);
        let flag = 1u64 << (bit & 63);
        let old = seen[bit / 64] & flag;
        ambiguous[bit / 64] |= old;
        seen[bit / 64] |= flag;
        collisions += usize::from(old != 0);
    }
    let mut slots = vec![0u64; (4 * collisions).max(1).next_power_of_two()];
    let mask = slots.len() - 1;
    const LOCATION: u64 = (1u64 << 48) - 1;
    assert!((parts.len() as u64) << row_bits < LOCATION);
    for part in 0..parts.len() {
        for index in 0..parts[part].rows.len() {
            let row = parts[part].rows[index];
            let hash = row.id.hash();
            let bit = (hash ^ (hash >> 32)) as usize & (bits - 1);
            if ambiguous[bit / 64] & (1u64 << (bit & 63)) == 0 {
                continue;
            }
            let tag = hash.rotate_left(16) & !LOCATION;
            let entry = tag | ((((part << row_bits) | index) as u64) + 1);
            let mut slot = hash.rotate_right(23) as usize & mask;
            loop {
                let old = slots[slot];
                if old == 0 {
                    slots[slot] = entry;
                    break;
                }
                if old & !LOCATION == tag {
                    let location = ((old & LOCATION) - 1) as usize;
                    let (p, i) = (location >> row_bits, location & row_mask);
                    let previous = parts[p].rows[i];
                    if previous.id == row.id {
                        // Valid F1 instants have byte order equal to time order.
                        // Only actual duplicates need their full ingest time read.
                        let a = row.ingest_offset as usize;
                        let b = previous.ingest_offset as usize;
                        if chunks[parts[part].chunk][a..a + 20] < chunks[parts[p].chunk][b..b + 20]
                        {
                            rejected[parts[p].chunk].reject(previous.ticket);
                            slots[slot] = entry;
                        } else {
                            rejected[parts[part].chunk].reject(row.ticket);
                        }
                        break;
                    }
                }
                slot = (slot + 1) & mask;
            }
        }
    }
}

pub fn read<R: EventRecord>(ctx: &Context, dir: &Path) -> Result<Vec<Vec<Vec<R>>>> {
    let mut timer = crate::Timer::start();
    let files = Parts::open(&dir.join("events"))?;
    let chunks = files.chunks();
    let parsed: Vec<_> = chunks
        .par_iter()
        .map(|bytes| parse::<R>(ctx, bytes))
        .collect();
    timer.mark("event parse");
    let mut groups: Vec<Vec<Part>> = (0..GROUPS).map(|_| Vec::new()).collect();
    let mut payloads = Vec::with_capacity(parsed.len());
    let mut rejected = Vec::with_capacity(parsed.len());
    for (chunk, (parts, payload, marks)) in parsed.into_iter().enumerate() {
        payloads.push(payload);
        rejected.push(marks);
        for (group, rows) in groups.iter_mut().zip(parts) {
            if !rows.is_empty() {
                group.push(Part { chunk, rows });
            }
        }
    }
    groups
        .into_par_iter()
        .for_each(|parts| select_earliest(&chunks, &rejected, &parts));
    timer.mark("event dedup");
    let by_person = rejected
        .into_par_iter()
        .zip(payloads)
        .map(|(marks, payload)| marks.keep(payload))
        .collect();
    Ok(pipeline::transpose(by_person))
}
