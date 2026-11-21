use crate::ids::{Id, Ids, Links, SHARDS};
use crate::input::{self, DAY, Parts, WEEK};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::{error::Error, fs, path::Path};

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
pub type CampaignTotals = FxHashMap<(i32, u32), Stats>;

pub struct Catalog {
    pub categories: Vec<Vec<u8>>,
    pub campaigns: Vec<(Vec<u8>, Vec<u8>)>,
    products: FxHashMap<Id, u32>,
    campaign_ids: FxHashMap<Id, u32>,
}

impl Catalog {
    pub fn open(dir: &Path, ids: &Ids) -> Result<Self> {
        let mut out = Self {
            categories: vec![b"unknown".to_vec()],
            campaigns: vec![(b"organic".to_vec(), b"organic".to_vec())],
            products: FxHashMap::default(),
            campaign_ids: FxHashMap::default(),
        };
        let mut index = FxHashMap::<Vec<u8>, u32>::default();
        for part in Parts::open(&dir.join("products.csv"))?.chunks() {
            input::rows(part, |[product, cat]| {
                let c = match index.get(cat) {
                    Some(&c) => c,
                    None => {
                        let c = out.categories.len() as u32;
                        out.categories.push(cat.to_vec());
                        index.insert(cat.to_vec(), c);
                        c
                    }
                };
                out.products.insert(ids.key(product), c);
            });
        }
        for part in Parts::open(&dir.join("campaigns.csv"))?.chunks() {
            input::rows(part, |[campaign, channel]| {
                let c = out.campaigns.len() as u32;
                out.campaigns.push((campaign.to_vec(), channel.to_vec()));
                out.campaign_ids.insert(ids.key(campaign), c);
            });
        }
        Ok(out)
    }
}

pub struct Context {
    pub ids: Ids,
    pub catalog: Catalog,
    aliases: FxHashMap<Id, Id>,
    start: i64,
    end: i64,
}

impl Context {
    pub fn open(dir: &Path) -> Result<Self> {
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.join("manifest.json"))?)?;
        let instant = |key: &str| {
            manifest[key]
                .as_str()
                .and_then(|s| input::timestamp(s.as_bytes()))
                .ok_or_else(|| format!("Invalid manifest field {key}"))
        };
        let (start, end) = (instant("window_start")?, instant("window_end")?);
        let ids = Ids::default();
        let catalog = Catalog::open(dir, &ids)?;
        let mut links = Links::default();
        for bytes in Parts::open(&dir.join("links"))?.chunks() {
            input::rows(bytes, |[a, b]| {
                if !a.is_empty() && !b.is_empty() {
                    links.join(&ids, ids.key(a), ids.key(b));
                }
            });
        }
        Ok(Self {
            ids,
            catalog,
            aliases: links.finish(),
            start,
            end,
        })
    }

    #[inline]
    fn person(&self, id: Id) -> Id {
        self.aliases.get(&id).copied().unwrap_or(id)
    }
}

#[derive(Clone, Copy, Default)]
pub struct Event {
    pub person: Id,
    pub ts: i64,
    pub cents: u32,
    pub category: u32,
}

#[derive(Clone, Copy)]
struct Candidate {
    id: Id,
    ingest: i64,
    event: Event,
}

#[derive(Clone, Copy)]
pub struct Click {
    pub person: Id,
    pub ts: i64,
    pub id: Id,
    pub campaign: u32,
}

fn buckets<T>() -> Vec<Vec<T>> {
    (0..SHARDS).map(|_| Vec::new()).collect()
}

// Move bucket handles between stages. Each reducer owns its input and can
// release it as soon as its shard has been processed.
fn transpose<T>(parts: Vec<Vec<Vec<T>>>) -> Vec<Vec<Vec<T>>> {
    let mut out: Vec<Vec<Vec<T>>> = buckets();
    for part in parts {
        for (dst, src) in out.iter_mut().zip(part) {
            if !src.is_empty() {
                dst.push(src);
            }
        }
    }
    out
}

pub fn gather<T>(parts: Vec<Vec<T>>) -> Vec<T> {
    let mut out = Vec::with_capacity(parts.iter().map(Vec::len).sum());
    for mut part in parts {
        out.append(&mut part);
    }
    out
}

fn validate_event(
    ctx: &Context,
    [_, user, time, _, kind, product, price]: [&[u8]; 7],
    ingest: i64,
) -> Option<Event> {
    if user.is_empty() || product.is_empty() {
        return None;
    }
    let ts = input::timestamp(time)?;
    if ts < ctx.start || ts >= ctx.end || ingest - ts > DAY {
        return None;
    }
    let (cents, category) = match kind {
        b"view" | b"cart" => (0, 0),
        b"purchase" => {
            let p = input::number(price)?;
            if !(1..=10_000_000).contains(&p) {
                return None;
            }
            (
                p as u32,
                ctx.catalog
                    .products
                    .get(&ctx.ids.key(product))
                    .copied()
                    .unwrap_or(0),
            )
        }
        _ => return None,
    };
    Some(Event {
        person: ctx.ids.key(user),
        ts,
        cents,
        category,
    })
}

pub fn read_events(ctx: &Context, dir: &Path) -> Result<Vec<Vec<Vec<Event>>>> {
    let by_id = {
        let files = Parts::open(dir)?;
        let parsed = files
            .chunks()
            .into_par_iter()
            .map(|bytes| {
                let mut out = buckets();
                input::rows(bytes, |fields: [&[u8]; 7]| {
                    if fields[0].is_empty() {
                        return;
                    }
                    let Some(ingest) = input::timestamp(fields[3]) else {
                        return;
                    };
                    let id = ctx.ids.key(fields[0]);
                    // Keep invalid copies in the contest for earliest ingest.
                    let event = validate_event(ctx, fields, ingest).unwrap_or(Event {
                        ts: i64::MIN,
                        ..Event::default()
                    });
                    out[id.shard()].push(Candidate { id, ingest, event });
                });
                out
            })
            .collect();
        transpose(parsed)
    };

    let by_person = by_id
        .into_par_iter()
        .map(|parts| {
            let size = parts.iter().map(Vec::len).sum();
            let mut winners = FxHashMap::with_capacity_and_hasher(size, Default::default());
            for candidate in parts.into_iter().flatten() {
                let slot = winners
                    .entry(candidate.id)
                    .or_insert((candidate.ingest, candidate.event));
                if candidate.ingest < slot.0 {
                    *slot = (candidate.ingest, candidate.event);
                }
            }
            let mut out = buckets();
            for (_, mut event) in winners.into_values() {
                if event.ts == i64::MIN {
                    continue;
                }
                event.person = ctx.person(event.person);
                out[event.person.shard()].push(event);
            }
            out
        })
        .collect();
    Ok(transpose(by_person))
}

pub fn read_clicks(ctx: &Context, dir: &Path) -> Result<Vec<Vec<Vec<Click>>>> {
    let files = Parts::open(dir)?;
    let parsed = files
        .chunks()
        .into_par_iter()
        .map(|bytes| {
            let mut out = buckets();
            input::rows(bytes, |[id, user, campaign, time]| {
                if id.is_empty() || user.is_empty() {
                    return;
                }
                let Some(ts) = input::timestamp(time) else {
                    return;
                };
                if ts < ctx.start - WEEK || ts >= ctx.end {
                    return;
                }
                let Some(&campaign) = ctx.catalog.campaign_ids.get(&ctx.ids.key(campaign)) else {
                    return;
                };
                let person = ctx.person(ctx.ids.key(user));
                out[person.shard()].push(Click {
                    person,
                    ts,
                    campaign,
                    id: ctx.ids.key(id),
                });
            });
            out
        })
        .collect();
    Ok(transpose(parsed))
}

#[derive(Default, Clone, Copy)]
pub struct Stats {
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assists: i64,
}
impl Stats {
    pub fn add(&mut self, other: Self) {
        self.sessions += other.sessions;
        self.purchasing += other.purchasing;
        self.revenue += other.revenue;
        self.assists += other.assists;
    }
}

#[derive(Default, Clone, Copy)]
pub struct UserDay {
    pub person: Id,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: Option<u32>,
}

// Position and multiplicity allow constant-time removal as clicks age out.
struct Active {
    count: Vec<u32>,
    position: Vec<usize>,
    campaigns: Vec<u32>,
}
impl Active {
    fn new(n: usize) -> Self {
        Self {
            count: vec![0; n],
            position: vec![0; n],
            campaigns: Vec::new(),
        }
    }
    fn add(&mut self, id: u32) {
        let i = id as usize;
        if self.count[i] == 0 {
            self.position[i] = self.campaigns.len();
            self.campaigns.push(id);
        }
        self.count[i] += 1;
    }
    fn remove(&mut self, id: u32) {
        let i = id as usize;
        self.count[i] -= 1;
        if self.count[i] == 0 {
            let at = self.position[i];
            self.campaigns.swap_remove(at);
            if let Some(&moved) = self.campaigns.get(at) {
                self.position[moved as usize] = at;
            }
        }
    }
    fn clear(&mut self) {
        for c in self.campaigns.drain(..) {
            self.count[c as usize] = 0;
        }
    }
}

pub struct Daily {
    pub users: Vec<UserDay>,
    pub campaigns: CampaignTotals,
}

pub fn summarize(ctx: &Context, events: Vec<Vec<Event>>, clicks: Vec<Vec<Click>>) -> Daily {
    let mut events = gather(events);
    let mut clicks = gather(clicks);
    events.sort_unstable_by_key(|e| (e.person, e.ts));
    clicks.sort_unstable_by_key(|c| (c.person, c.ts, c.id));
    clicks.dedup_by_key(|c| (c.person, c.ts, c.id));

    let mut out = Daily {
        users: Vec::new(),
        campaigns: CampaignTotals::default(),
    };
    let mut active = Active::new(ctx.catalog.campaigns.len());
    let mut amounts = vec![0i64; ctx.catalog.categories.len()];
    let mut purchased = Vec::new();
    let mut remaining = clicks.as_slice();
    for user_events in events.chunk_by(|a, b| a.person == b.person) {
        let person = user_events[0].person;
        while remaining.first().is_some_and(|c| c.person < person) {
            remaining = &remaining[1..];
        }
        let end = remaining
            .iter()
            .position(|c| c.person != person)
            .unwrap_or(remaining.len());
        let (user_clicks, rest) = remaining.split_at(end);
        remaining = rest;
        let (mut left, mut right) = (0, 0);
        let mut winner: Option<&Click> = None;
        for session in user_events.chunk_by(|a, b| b.ts - a.ts <= 1_800_000) {
            let start = session[0].ts;
            let day = start.div_euclid(DAY) as i32;
            if out
                .users
                .last()
                .is_none_or(|u| u.person != person || u.day != day)
            {
                for cat in purchased.drain(..) {
                    amounts[cat] = 0;
                }
                out.users.push(UserDay {
                    day,
                    person,
                    ..UserDay::default()
                });
            }
            let user = out.users.last_mut().unwrap();
            let mut revenue = 0;
            for e in session.iter().filter(|e| e.cents != 0) {
                let cat = e.category as usize;
                if amounts[cat] == 0 {
                    purchased.push(cat);
                }
                revenue += i64::from(e.cents);
                amounts[cat] += i64::from(e.cents);
                if user.top.is_none_or(|top| {
                    let best = top as usize;
                    amounts[cat] > amounts[best]
                        || (amounts[cat] == amounts[best]
                            && ctx.catalog.categories[cat] < ctx.catalog.categories[best])
                }) {
                    user.top = Some(e.category);
                }
            }
            user.sessions += 1;
            user.events += session.len() as i64;
            user.revenue += revenue;

            while let Some(click) = user_clicks.get(right).filter(|c| c.ts <= start) {
                active.add(click.campaign);
                if winner.is_none_or(|w| w.ts < click.ts || ctx.ids.cmp(click.id, w.id).is_lt()) {
                    winner = Some(click);
                }
                right += 1;
            }
            while left < right && user_clicks[left].ts < start - WEEK {
                active.remove(user_clicks[left].campaign);
                left += 1;
            }
            let credit = winner
                .filter(|w| w.ts >= start - WEEK)
                .map_or(0, |w| w.campaign);
            let stats = out.campaigns.entry((day, credit)).or_default();
            stats.sessions += 1;
            stats.revenue += revenue;
            stats.purchasing += i64::from(revenue > 0);
            for &campaign in &active.campaigns {
                out.campaigns.entry((day, campaign)).or_default().assists += 1;
            }
        }
        active.clear();
    }
    out
}
