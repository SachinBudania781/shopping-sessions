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
    pub(crate) aliases: FxHashMap<Id, Id>,
    pub(crate) start: i64,
    end: i64,
}

impl Context {
    pub fn open(dir: &Path) -> Result<Self> {
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.join("manifest.json"))?)?;
        let instant = |key: &str| {
            manifest[key]
                .as_str()
                .and_then(|s| input::Calendar::default().timestamp(s.as_bytes()))
                .ok_or_else(|| format!("Invalid manifest field {key}"))
        };
        let (start, end) = (instant("window_start")?, instant("window_end")?);
        let ids = Ids::default();
        let catalog = Catalog::open(dir, &ids)?;
        Ok(Self {
            ids,
            catalog,
            aliases: FxHashMap::default(),
            start,
            end,
        })
    }

    pub fn read_links(&self, path: &Path) -> Result<FxHashMap<Id, Id>> {
        let mut links = Links::default();
        let files = Parts::open(path)?;
        let chunks = files.chunks();
        links.reserve(chunks.iter().map(|b| b.len()).sum::<usize>() / 14);
        for bytes in chunks {
            input::rows(bytes, |[a, b]| {
                if !a.is_empty() && !b.is_empty() {
                    links.join(&self.ids, self.ids.key(a), self.ids.key(b));
                }
            });
        }
        Ok(links.finish())
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

pub(crate) trait EventRecord: Copy + Send {
    fn from_event(ctx: &Context, event: Event) -> Self;
    fn person(self) -> Id;
    fn into_event(self, ctx: &Context) -> Event;
}

impl EventRecord for Event {
    #[inline]
    fn from_event(_ctx: &Context, event: Event) -> Self {
        event
    }

    #[inline]
    fn person(self) -> Id {
        self.person
    }

    #[inline]
    fn into_event(self, _ctx: &Context) -> Event {
        self
    }
}

// The usual window fits in a 32-bit millisecond offset. The high word
// stores up to 10,000,000 cents and an eight-bit catalog category.
#[derive(Clone, Copy)]
pub(crate) struct PackedEvent(Id, u64);

impl PackedEvent {
    pub fn supports(ctx: &Context) -> bool {
        ctx.end - ctx.start <= i64::from(u32::MAX) && ctx.catalog.categories.len() <= 256
    }
}

impl EventRecord for PackedEvent {
    #[inline]
    fn from_event(ctx: &Context, event: Event) -> Self {
        Self(
            event.person,
            (u64::from(event.category) << 56)
                | (u64::from(event.cents) << 32)
                | (event.ts - ctx.start) as u64,
        )
    }

    #[inline]
    fn person(self) -> Id {
        self.0
    }

    #[inline]
    fn into_event(self, ctx: &Context) -> Event {
        Event {
            person: self.0,
            ts: ctx.start + i64::from(self.1 as u32),
            cents: (self.1 >> 32) as u32 & 0x00ff_ffff,
            category: (self.1 >> 56) as u32,
        }
    }
}

#[derive(Clone, Copy)]
pub struct Click {
    pub person: Id,
    pub ts: i64,
    pub id: Id,
    pub campaign: u32,
}

pub(crate) fn buckets<T>() -> Vec<Vec<T>> {
    (0..SHARDS).map(|_| Vec::new()).collect()
}

// Move bucket handles between stages. Each reducer owns its input and can
// release it as soon as its shard has been processed.
pub(crate) fn transpose<T>(parts: Vec<Vec<Vec<T>>>) -> Vec<Vec<Vec<T>>> {
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

pub(crate) fn validate_event(
    ctx: &Context,
    calendar: &mut input::Calendar,
    [_, user, time, ingest_field, kind, product, price]: [&[u8]; 7],
) -> Option<Event> {
    if user.is_empty() || product.is_empty() {
        return None;
    }
    let ts = calendar.timestamp(time)?;
    if ts < ctx.start || ts >= ctx.end || !calendar.on_time(ingest_field, ts) {
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

pub(crate) struct PersonCache(Vec<(Id, Id)>);
impl PersonCache {
    pub(crate) fn new() -> Self {
        Self(vec![(Id::default(), Id::default()); 4096])
    }
    #[inline]
    pub(crate) fn person(&mut self, ctx: &Context, id: Id) -> Id {
        let slot = &mut self.0[(id.hash() >> 36) as usize & 4095];
        if slot.0 != id {
            *slot = (id, ctx.person(id));
        }
        slot.1
    }
}

pub fn read_clicks(ctx: &Context, dir: &Path) -> Result<Vec<Vec<Vec<Click>>>> {
    let files = Parts::open(dir)?;
    let parsed = files
        .chunks()
        .into_par_iter()
        .map(|bytes| {
            let mut out = buckets();
            let mut calendar = input::Calendar::default();
            let mut cache = PersonCache::new();
            input::rows(bytes, |[id, user, campaign, time]| {
                if id.is_empty() || user.is_empty() {
                    return;
                }
                let Some(ts) = calendar.timestamp(time) else {
                    return;
                };
                if ts < ctx.start - WEEK || ts >= ctx.end {
                    return;
                }
                let Some(&campaign) = ctx.catalog.campaign_ids.get(&ctx.ids.key(campaign)) else {
                    return;
                };
                let person = cache.person(ctx, ctx.ids.key(user));
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
    pub campaigns: Counters,
}

impl Daily {
    pub fn new(ctx: &Context) -> Self {
        Self {
            users: Vec::new(),
            campaigns: Counters::new(ctx),
        }
    }
}

pub struct Counters {
    first: i32,
    width: usize,
    dense: Vec<Stats>,
    sparse: CampaignTotals,
}

impl Counters {
    pub fn new(ctx: &Context) -> Self {
        let first = ctx.start.div_euclid(DAY) as i32;
        let days = ((ctx.end - 1).div_euclid(DAY) - i64::from(first) + 1).max(0) as usize;
        let width = ctx.catalog.campaigns.len();
        let cells = days.saturating_mul(width);
        Self {
            first,
            width,
            dense: if cells <= 65536 {
                vec![Stats::default(); cells]
            } else {
                Vec::new()
            },
            sparse: CampaignTotals::default(),
        }
    }
    fn record(&mut self, day: i32, credit: u32, revenue: i64, assists: &[u32]) {
        let credited = |s: &mut Stats| {
            s.sessions += 1;
            s.revenue += revenue;
            s.purchasing += i64::from(revenue > 0);
        };
        if self.dense.is_empty() {
            credited(self.sparse.entry((day, credit)).or_default());
            for &c in assists {
                self.sparse.entry((day, c)).or_default().assists += 1;
            }
        } else {
            let base = (day - self.first) as usize * self.width;
            let values = &mut self.dense[base..base + self.width];
            credited(&mut values[credit as usize]);
            for &c in assists {
                values[c as usize].assists += 1;
            }
        }
    }
    pub fn merge(&mut self, other: &Self) {
        for (dst, src) in self.dense.iter_mut().zip(&other.dense) {
            dst.add(*src);
        }
        for (&k, &v) in &other.sparse {
            self.sparse.entry(k).or_default().add(v);
        }
    }
    pub fn into_rows(self) -> Vec<((i32, u32), Stats)> {
        if self.dense.is_empty() {
            return self.sparse.into_iter().collect();
        }
        self.dense
            .into_iter()
            .enumerate()
            .filter_map(|(i, s)| {
                (s.sessions != 0 || s.assists != 0).then_some((
                    (
                        self.first + (i / self.width) as i32,
                        (i % self.width) as u32,
                    ),
                    s,
                ))
            })
            .collect()
    }
}

#[derive(Default)]
struct Person {
    id: Id,
    end: usize,
    purchases: Vec<Event>,
    clicks: Vec<Click>,
}

pub fn summarize<R: EventRecord>(
    ctx: &Context,
    events: Vec<Vec<R>>,
    clicks: Vec<Vec<Click>>,
    out: &mut Daily,
) {
    let count: usize = events.iter().map(Vec::len).sum();
    if count == 0 {
        return;
    }
    let mut names = Vec::with_capacity(count / 24);
    let mut counts = Vec::<u32>::with_capacity(count / 24);
    let mut index = FxHashMap::with_capacity_and_hasher(count / 24, Default::default());
    let mut tickets = Vec::with_capacity(count);
    for event in events.iter().flatten() {
        let person = event.person();
        let i = *index.entry(person).or_insert_with(|| {
            let i = names.len() as u32;
            names.push(person);
            counts.push(0);
            i
        });
        counts[i as usize] += 1;
        tickets.push(i);
    }
    let mut base = 0;
    let mut people: Vec<_> = names
        .into_iter()
        .zip(counts)
        .map(|(id, count)| {
            let end = base;
            base += count as usize;
            Person {
                id,
                end,
                ..Person::default()
            }
        })
        .collect();
    let mut times = vec![0i64; count];
    for (e, rank) in events.into_iter().flatten().zip(tickets) {
        let e = e.into_event(ctx);
        let p = &mut people[rank as usize];
        times[p.end] = e.ts;
        p.end += 1;
        if e.cents != 0 {
            p.purchases.push(e);
        }
    }
    for click in clicks.into_iter().flatten() {
        if let Some(&rank) = index.get(&click.person) {
            people[rank as usize].clicks.push(click);
        }
    }

    out.users.reserve(people.len() * 4);
    let mut active = Active::new(ctx.catalog.campaigns.len());
    let mut amounts = vec![0i64; ctx.catalog.categories.len()];
    let mut purchased = Vec::new();
    let mut begin = 0;
    for mut p in people {
        let person = p.id;
        let user_events = &mut times[begin..p.end];
        begin = p.end;
        user_events.sort_unstable();
        p.purchases.sort_unstable_by_key(|e| e.ts);
        let mut sales = p.purchases.as_slice();
        let user_clicks = &mut p.clicks;
        user_clicks.sort_unstable_by_key(|c| (c.ts, c.id));
        user_clicks.dedup_by_key(|c| (c.ts, c.id));
        let (mut left, mut right) = (0, 0);
        let mut winner: Option<&Click> = None;
        for session in user_events.chunk_by(|a, b| b - a <= 1_800_000) {
            let start = session[0];
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
            let end = *session.last().unwrap();
            while let Some((e, rest)) = sales.split_first().filter(|(e, _)| e.ts <= end) {
                sales = rest;
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
            out.campaigns
                .record(day, credit, revenue, &active.campaigns);
        }
        active.clear();
    }
}
