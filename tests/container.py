"""Exercise long UTF-8 IDs, lexical ties, and DATE columns in the built image."""
import subprocess
import tempfile
from datetime import date
from pathlib import Path

import polars as pl


with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    data = root / "input"
    output = root / "out"
    output.mkdir(mode=0o777)
    output.chmod(0o777)
    for folder in ("events", "clicks", "links"):
        (data / folder).mkdir(parents=True)
    user = "A-long-别名"
    t = "2026-10-01T00:00:00Z"
    (data / "manifest.json").write_text(
        '{"window_start":"2026-10-01T00:00:00Z","window_end":"2026-10-15T00:00:00Z"}'
    )
    (data / "products.csv").write_text("product_id,category\np,shoes\n")
    (data / "campaigns.csv").write_text("campaign_id,channel\nc_1,email\nc_2,search\n")
    (data / "links" / "one.csv").write_text(f"user_id_a,user_id_b\nu,{user}\n")
    (data / "events" / "one.csv").write_text(
        "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"
        f"e,u,{t},{t},purchase,p,250\n"
    )
    (data / "clicks" / "one.csv").write_text(
        "click_id,user_id,campaign_id,ts\n"
        f'zzzzzzzzzz-long,u,c_1,{t}\n"z",{user},c_2,{t}\n'
    )
    subprocess.run([
        "docker", "run", "--rm", "--network", "none", "--read-only",
        "--user", "1000:1000", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--tmpfs", "/tmp:size=64m", "-v", f"{data}:/data:ro",
        "-v", f"{output}:/output", "sessions",
    ], check=True)
    # Let runners with a different UID clean up the container-owned files.
    subprocess.run([
        "docker", "run", "--rm", "--network", "none", "--read-only",
        "--entrypoint", "chmod", "-v", f"{output}:/output",
        "sessions", "-R", "a+rwX", "/output",
    ], check=True)
    day = date(2026, 10, 1)
    users = pl.read_parquet(output / "user_daily" / "*.parquet")
    assert users.select("user_id", "day", "sessions", "events", "revenue_cents", "top_category").rows() == [
        (user, day, 1, 1, 250, "shoes")
    ]
    campaigns = pl.read_parquet(output / "campaign_daily" / "*.parquet").sort("campaign_id")
    assert campaigns.select("campaign_id", "channel", "day", "sessions", "purchasing_sessions",
                            "revenue_cents", "assisted_sessions").rows() == [
        ("c_1", "email", day, 0, 0, 0, 1), ("c_2", "search", day, 1, 1, 250, 1)
    ]
