mod duplicates;
mod ids;
mod input;
mod output;
mod pipeline;

use pipeline::{Context, Result};
use rayon::prelude::*;
use std::{env, path::PathBuf};

fn main() -> Result<()> {
    let data = PathBuf::from(env::var_os("DATA_DIR").unwrap_or_else(|| "/data".into()));
    let out = PathBuf::from(env::var_os("OUTPUT_DIR").unwrap_or_else(|| "/output".into()));
    let workers: usize = env::var("NCPU").unwrap_or_else(|_| "8".into()).parse()?;
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers.max(1))
        .build_global()?;
    let ctx = Context::open(&data)?;
    let events = pipeline::read_events(&ctx, &data.join("events"))?;
    let clicks = pipeline::read_clicks(&ctx, &data.join("clicks"))?;
    let daily = events
        .into_par_iter()
        .zip(clicks)
        .map(|(e, c)| pipeline::summarize(&ctx, e, c))
        .collect::<Vec<_>>();
    output::write(&out, &ctx, &daily)
}
