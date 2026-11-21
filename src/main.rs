mod events;
mod ids;
mod input;
mod output;
mod pipeline;
#[cfg(target_arch = "x86_64")]
mod scan;

use pipeline::{Context, Daily, Event, EventRecord, PackedEvent, Result};
use rayon::prelude::*;
use std::time::Instant;
use std::{
    env,
    path::{Path, PathBuf},
};

pub(crate) struct Timer(Option<Instant>);
impl Timer {
    pub fn start() -> Self {
        Self(env::var_os("SESSION_TIMINGS").map(|_| Instant::now()))
    }
    pub fn mark(&mut self, stage: &str) {
        if let Some(t) = &mut self.0 {
            eprintln!("{stage} {:.6}s", t.elapsed().as_secs_f64());
            *t = Instant::now();
        }
    }
}

fn main() -> Result<()> {
    let mut timer = Timer::start();
    let data = PathBuf::from(env::var_os("DATA_DIR").unwrap_or_else(|| "/data".into()));
    let out = PathBuf::from(env::var_os("OUTPUT_DIR").unwrap_or_else(|| "/output".into()));
    let workers: usize = env::var("NCPU").unwrap_or_else(|_| "8".into()).parse()?;
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers.max(1))
        .build_global()?;
    let mut ctx = Context::open(&data)?;
    timer.mark("dimensions");
    if PackedEvent::supports(&ctx) {
        run::<PackedEvent>(&mut ctx, &data, &out, &mut timer)
    } else {
        run::<Event>(&mut ctx, &data, &out, &mut timer)
    }
}

fn run<R: EventRecord>(
    ctx: &mut Context,
    data: &Path,
    out: &Path,
    timer: &mut Timer,
) -> Result<()> {
    ctx.aliases = ctx.read_links(&data.join("links"))?;
    timer.mark("identity links");
    let (events, clicks) = rayon::join(
        || events::read::<R>(ctx, data),
        || pipeline::read_clicks(ctx, &data.join("clicks")),
    );
    timer.mark("events and clicks");
    let daily = events?
        .into_par_iter()
        .zip(clicks?)
        .with_min_len(4)
        .fold(
            || Daily::new(ctx),
            |mut daily, (e, c)| {
                pipeline::summarize(ctx, e, c, &mut daily);
                daily
            },
        )
        .collect::<Vec<_>>();
    timer.mark("user and campaign aggregation");
    output::write(out, ctx, &daily)?;
    timer.mark("parquet");
    Ok(())
}
