use crate::ids::Id;
use crate::input::{self, Calendar, Parts};
use crate::pipeline::{self, Context, Event, PersonCache, Result};
use rayon::prelude::*;
use std::path::Path;

const GROUPS: usize = 256;
const INVALID: u32 = u32::MAX;
const EXTRA: u32 = INVALID - 1;

// Most events need just an owner and a millisecond offset. The rare purchase,
// or a timestamp too wide for the offset, points to a full record in extras.
#[derive(Clone, Copy)]
struct Row {
    id: Id,
    owner_or_extra: u64,
    millis: u32,
    ingest_offset: u32,
}
struct Part {
    chunk: usize,
    rows: Vec<Row>,
}

fn parse(ctx: &Context, bytes: &[u8]) -> (Vec<Vec<Row>>, Vec<Event>) {
    let mut parts: Vec<_> = (0..GROUPS)
        .map(|_| Vec::with_capacity(bytes.len() / (GROUPS * 64)))
        .collect();
    let mut extras = Vec::new();
    let mut calendar = Calendar::default();
    input::rows(bytes, |fields: [&[u8]; 7]| {
        if fields[0].is_empty() || fields[3].len() != 20 {
            return;
        }
        let id = ctx.ids.key(fields[0]);
        let (owner_or_extra, millis) =
            match pipeline::validate_event(ctx, &mut calendar, fields, None) {
                None => (0, INVALID),
                Some(e) => {
                    let relative = e.ts - ctx.start;
                    if e.cents == 0 && relative < i64::from(EXTRA) {
                        (e.person.0, relative as u32)
                    } else {
                        let at = extras.len();
                        extras.push(e);
                        (at as u64, EXTRA)
                    }
                }
            };
        let offset = fields[3].as_ptr() as usize - bytes.as_ptr() as usize;
        let bucket = (id.hash() >> (64 - GROUPS.trailing_zeros())) as usize;
        parts[bucket].push(Row {
            id,
            owner_or_extra,
            millis,
            ingest_offset: u32::try_from(offset).expect("CSV chunk offset"),
        });
    });
    (parts, extras)
}

fn reduce(
    ctx: &Context,
    chunks: &[&[u8]],
    extras: &[Vec<Event>],
    mut parts: Vec<Part>,
) -> Vec<Vec<Event>> {
    let mut out = pipeline::buckets();
    let count: usize = parts.iter().map(|p| p.rows.len()).sum();
    if count == 0 {
        return out;
    }
    let mut slots = vec![0u64; (count * 2).next_power_of_two()];
    let mask = slots.len() - 1;
    let longest = parts.iter().map(|p| p.rows.len()).max().unwrap();
    let row_bits = usize::BITS - (longest - 1).leading_zeros();
    let row_mask = (1usize << row_bits) - 1;
    const LOCATION: u64 = (1u64 << 48) - 1;
    assert!((parts.len() as u64) << row_bits < LOCATION);
    for part in 0..parts.len() {
        for index in 0..parts[part].rows.len() {
            let row = parts[part].rows[index];
            let hash = row.id.hash();
            let tag = hash.rotate_left(16) & !LOCATION;
            let entry = tag | ((((part << row_bits) | index) as u64) + 1);
            let mut slot = (hash ^ (hash >> 32)) as usize & mask;
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
                            parts[p].rows[i].millis = INVALID;
                            slots[slot] = entry;
                        } else {
                            parts[part].rows[index].millis = INVALID;
                        }
                        break;
                    }
                }
                slot = (slot + 1) & mask;
            }
        }
    }
    let mut cache = PersonCache::new();
    for part in parts {
        for row in part.rows {
            let mut event = match row.millis {
                INVALID => continue,
                EXTRA => extras[part.chunk][row.owner_or_extra as usize],
                millis => Event {
                    person: Id(row.owner_or_extra),
                    ts: ctx.start + i64::from(millis),
                    cents: 0,
                    category: 0,
                },
            };
            event.person = cache.person(ctx, event.person);
            out[event.person.shard()].push(event);
        }
    }
    out
}

pub fn read(ctx: &Context, dir: &Path) -> Result<Vec<Vec<Vec<Event>>>> {
    let mut timer = crate::Timer::start();
    let files = Parts::open(dir)?;
    let chunks = files.chunks();
    let (parsed, extras): (Vec<_>, Vec<_>) =
        chunks.par_iter().map(|bytes| parse(ctx, bytes)).unzip();
    timer.mark("event parse");
    let mut groups: Vec<Vec<Part>> = (0..GROUPS).map(|_| Vec::new()).collect();
    for (chunk, parts) in parsed.into_iter().enumerate() {
        for (group, rows) in groups.iter_mut().zip(parts) {
            if !rows.is_empty() {
                group.push(Part { chunk, rows });
            }
        }
    }
    let by_person = groups
        .into_par_iter()
        .map(|parts| reduce(ctx, &chunks, &extras, parts))
        .collect();
    Ok(pipeline::transpose(by_person))
}
