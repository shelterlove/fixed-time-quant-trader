from __future__ import annotations

import argparse
from datetime import UTC, datetime
import importlib.metadata
import json
from pathlib import Path
import platform
import subprocess
import traceback

from ..config import load_strategy
from ..state import RuntimeLock
from .data import MinuteStore, digest, prepare, repair_snapshot, utc, write_json
from .replay import build_signals, replay
from .report import report


def main() -> None:
    parser=argparse.ArgumentParser(description="Frozen hourly -> targeted minutes -> conservative replay")
    commands=parser.add_subparsers(dest="command",required=True)
    data=commands.add_parser("prepare")
    data.add_argument("--source",type=Path,required=True)
    data.add_argument("--snapshot",type=Path,required=True)
    data.add_argument("--start",default="2021-01-01T00:00:00Z")
    data.add_argument("--end",required=True)
    data.add_argument("--supplement-hours",action="store_true")
    repair=commands.add_parser("repair-hours")
    repair.add_argument("--snapshot",type=Path,required=True)
    repair.add_argument("--target",type=Path,required=True)
    repair.add_argument("--local-source",type=Path,required=True)
    run=commands.add_parser("run")
    run.add_argument("--snapshot",type=Path,required=True)
    run.add_argument("--run",type=Path,required=True)
    run.add_argument("--minute-source",type=Path,required=True)
    run.add_argument("--minute-cache",type=Path,default=Path("data/minutes"))
    run.add_argument("--download-minutes",action="store_true")
    run.add_argument("--root",type=Path,default=Path("."))
    args=parser.parse_args()
    if args.command=="prepare":
        with RuntimeLock(args.snapshot/"prepare"):
            prepare(args.source,args.snapshot,utc(args.start),utc(args.end),args.supplement_hours)
        return
    if args.command=="repair-hours":
        with RuntimeLock(args.target/"prepare"):
            repair_snapshot(args.snapshot,args.target,args.local_source)
        return
    args.run.mkdir(parents=True,exist_ok=True)
    with RuntimeLock(args.run/"research"):
        if (args.run/"baseline.json").exists():
            raise ValueError("completed baseline is immutable; use a new run id")
        config=load_strategy(args.root)
        code=Path(__file__).parents[1]
        fingerprint={"strategy":digest(args.root/"strategy.toml"),"data_manifest":digest(args.snapshot/"manifest.json"),
                     "code":{str(p.relative_to(code)):digest(p) for p in sorted(code.rglob("*.py"))}}
        runfile=args.run/"run.json"
        if runfile.exists():
            previous=json.loads(runfile.read_text(encoding="utf-8"))
            if previous["fingerprint"]!=fingerprint:
                raise ValueError("code/config/data changed since run started; use a new run id")
        else:
            write_json(runfile,{"created_at":datetime.now(UTC),"fingerprint":fingerprint,"snapshot":str(args.snapshot.resolve()),
                "git_revision":subprocess.check_output(["git","rev-parse","HEAD"],cwd=args.root,text=True).strip(),
                "git_dirty":bool(subprocess.check_output(["git","status","--porcelain"],cwd=args.root,text=True)),
                "python":platform.python_version(),"packages":{p:importlib.metadata.version(p) for p in ["numpy","polars"]},
                "minute_policy":"local first; missing required hour only; complete 60 bars and aggregate match; then worst feasible intraminute path"})
        try:
            manifest=json.loads((args.snapshot/"manifest.json").read_text(encoding="utf-8"))
            for record in manifest["records"]:
                if digest(args.snapshot/record["path"])!=record["sha256"]:
                    raise ValueError(f"frozen data was changed: {record['path']}")
            signals=build_signals(args.snapshot,args.run,config)
            hourly=args.run/"hourly"
            if not (hourly/"summary.json").exists():
                replay(args.snapshot,signals,hourly,config)
            requirements=json.loads((hourly/"minute_requirements.json").read_text(encoding="utf-8"))
            # Refined paths can alter later capital/holdings. Demand discovery must
            # continue on the new path; the initial hourly list is not exhaustive.
            minutes=MinuteStore(args.minute_source,args.minute_cache,download=args.download_minutes)
            replay(args.snapshot,signals,args.run/"refined",config,minutes)
            requirements+=json.loads((args.run/"refined/minute_requirements.json").read_text(encoding="utf-8"))
            unique={(x["symbol"],x["hour"],x["trade_id"]):x for x in requirements}
            write_json(args.run/"minute_requirements_all.json",list(unique.values()))
            report(args.run)
            write_json(args.run/"status.json",{"status":"complete","updated_at":datetime.now(UTC)})
            print(f"BASELINE COMPLETE {args.run/'REPORT.md'}",flush=True)
        except Exception as exc:
            write_json(args.run/"status.json",{"status":"failed","updated_at":datetime.now(UTC),"error":str(exc),"traceback":traceback.format_exc()})
            raise


if __name__=="__main__":
    main()
