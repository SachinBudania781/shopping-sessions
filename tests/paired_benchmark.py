import datetime
import json
import os
import pathlib
import statistics
import subprocess
import tempfile

root = pathlib.Path(os.environ["RUNNER_TEMP"])
cpus = sorted(os.sched_getaffinity(0))[:8]
data = root / "practice"
print("vCPUs", cpus, "input bytes", sum(p.stat().st_size for p in data.rglob("*") if p.is_file()), flush=True)
times = {"submitted": [], "candidate": []}
for image in ["submitted", "candidate", "candidate", "submitted", "submitted", "candidate"]:
    with tempfile.TemporaryDirectory(dir=root) as out:
        for table in ["user_daily", "campaign_daily"]:
            (pathlib.Path(out) / table).mkdir(mode=0o777)
            (pathlib.Path(out) / table).chmod(0o777)
        pathlib.Path(out).chmod(0o777)
        cid = subprocess.check_output([
            "docker", "create", "--read-only", "--network", "none", "--user", "1000:1000",
            "--cpuset-cpus", ",".join(map(str, cpus)), "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "-e", f"NCPU={len(cpus)}",
            "-v", f"{data}:/data:ro", "-v", f"{out}:/output", image
        ], text=True).strip()
        subprocess.run(["docker", "start", "-a", cid], check=True)
        state = json.loads(subprocess.check_output(["docker", "inspect", cid]))[0]["State"]
        assert state["ExitCode"] == 0, state
        start, end = [datetime.datetime.fromisoformat(state[k].replace("Z", "+00:00")) for k in ("StartedAt","FinishedAt")]
        duration = (end-start).total_seconds()
        times[image].append(duration)
        print(image, duration, flush=True)
        subprocess.run(["docker","rm",cid],check=True,stdout=subprocess.DEVNULL)
for image, samples in times.items():
    print(image, "median", statistics.median(samples))
