use crate::pipeline::{Context, Counters, Daily, Result, UserDay};
use parquet::{
    data_type::{ByteArray, ByteArrayType, DataType, Int32Type, Int64Type},
    file::{
        properties::{EnabledStatistics, WriterProperties},
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::parser::parse_message_type,
};
use rayon::prelude::*;
use std::{
    fs::{self, File},
    io::BufWriter,
    path::Path,
    sync::Arc,
};

const USERS: &str = "message user_daily {
    REQUIRED BYTE_ARRAY user_id (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 events;
    REQUIRED INT64 revenue_cents;
    OPTIONAL BYTE_ARRAY top_category (UTF8);
}";
const CAMPAIGNS: &str = "message campaign_daily {
    REQUIRED BYTE_ARRAY campaign_id (UTF8);
    REQUIRED BYTE_ARRAY channel (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 purchasing_sessions;
    REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions;
}";

type FileWriter = SerializedFileWriter<BufWriter<File>>;
type Group<'a> = SerializedRowGroupWriter<'a, BufWriter<File>>;

fn writer(path: &Path, schema: &str) -> Result<FileWriter> {
    let settings = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    Ok(FileWriter::new(
        BufWriter::new(File::create(path)?),
        Arc::new(parse_message_type(schema)?),
        Arc::new(settings),
    )?)
}

fn column<T: DataType>(
    group: &mut Group<'_>,
    values: &[T::T],
    levels: Option<&[i16]>,
) -> Result<()> {
    let mut col = group.next_column()?.expect("column declared in schema");
    col.typed::<T>().write_batch(values, levels, None)?;
    col.close()?;
    Ok(())
}

fn users(
    path: &Path,
    rows: impl Iterator<Item = UserDay>,
    ctx: &Context,
    categories: &[ByteArray],
) -> Result<()> {
    let mut file = writer(path, USERS)?;
    let mut rows = rows.peekable();
    while rows.peek().is_some() {
        let batch: Vec<_> = rows.by_ref().take(32768).collect();
        let mut group = file.next_row_group()?;
        let mut previous = None;
        let mut name = ByteArray::default();
        let names: Vec<_> = batch
            .iter()
            .map(|r| {
                if previous != Some(r.person) {
                    name = ByteArray::from(ctx.ids.bytes(r.person));
                    previous = Some(r.person);
                }
                name.clone()
            })
            .collect();
        column::<ByteArrayType>(&mut group, &names, None)?;
        column::<Int32Type>(
            &mut group,
            &batch.iter().map(|r| r.day).collect::<Vec<_>>(),
            None,
        )?;
        column::<Int64Type>(
            &mut group,
            &batch.iter().map(|r| r.sessions).collect::<Vec<_>>(),
            None,
        )?;
        column::<Int64Type>(
            &mut group,
            &batch.iter().map(|r| r.events).collect::<Vec<_>>(),
            None,
        )?;
        column::<Int64Type>(
            &mut group,
            &batch.iter().map(|r| r.revenue).collect::<Vec<_>>(),
            None,
        )?;
        let values: Vec<_> = batch
            .iter()
            .filter_map(|r| r.top.map(|c| categories[c as usize].clone()))
            .collect();
        let levels: Vec<_> = batch.iter().map(|r| i16::from(r.top.is_some())).collect();
        column::<ByteArrayType>(&mut group, &values, Some(&levels))?;
        group.close()?;
    }
    file.close()?;
    Ok(())
}

pub fn write(dir: &Path, ctx: &Context, parts: &[Daily]) -> Result<()> {
    let user_dir = dir.join("user_daily");
    let campaign_dir = dir.join("campaign_daily");
    fs::create_dir_all(&user_dir)?;
    fs::create_dir_all(&campaign_dir)?;
    let categories: Vec<_> = ctx
        .catalog
        .categories
        .iter()
        .cloned()
        .map(ByteArray::from)
        .collect();
    let parts_per_file = parts.len().div_ceil(rayon::current_num_threads());
    parts
        .par_chunks(parts_per_file)
        .enumerate()
        .try_for_each(|(i, parts)| {
            if i != 0 && parts.iter().all(|p| p.users.is_empty()) {
                return Ok(());
            }
            let path = user_dir.join(format!("part-{i:03}.parquet"));
            users(
                &path,
                parts.iter().flat_map(|p| p.users.iter().copied()),
                ctx,
                &categories,
            )
        })?;

    let mut totals = Counters::new(ctx);
    for part in parts {
        totals.merge(&part.campaigns);
    }
    let mut file = writer(&campaign_dir.join("part-000.parquet"), CAMPAIGNS)?;
    let dims: Vec<_> = ctx
        .catalog
        .campaigns
        .iter()
        .map(|(id, ch)| (ByteArray::from(id.clone()), ByteArray::from(ch.clone())))
        .collect();
    let rows = totals.into_rows();
    for batch in rows.chunks(32768) {
        let mut group = file.next_row_group()?;
        column::<ByteArrayType>(
            &mut group,
            &batch
                .iter()
                .map(|((_, c), _)| dims[*c as usize].0.clone())
                .collect::<Vec<_>>(),
            None,
        )?;
        column::<ByteArrayType>(
            &mut group,
            &batch
                .iter()
                .map(|((_, c), _)| dims[*c as usize].1.clone())
                .collect::<Vec<_>>(),
            None,
        )?;
        column::<Int32Type>(
            &mut group,
            &batch.iter().map(|((d, _), _)| *d).collect::<Vec<_>>(),
            None,
        )?;
        column::<Int64Type>(
            &mut group,
            &batch.iter().map(|(_, s)| s.sessions).collect::<Vec<_>>(),
            None,
        )?;
        column::<Int64Type>(
            &mut group,
            &batch.iter().map(|(_, s)| s.purchasing).collect::<Vec<_>>(),
            None,
        )?;
        column::<Int64Type>(
            &mut group,
            &batch.iter().map(|(_, s)| s.revenue).collect::<Vec<_>>(),
            None,
        )?;
        column::<Int64Type>(
            &mut group,
            &batch.iter().map(|(_, s)| s.assists).collect::<Vec<_>>(),
            None,
        )?;
        group.close()?;
    }
    file.close()?;
    Ok(())
}
