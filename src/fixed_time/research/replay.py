from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
import math
from pathlib import Path

import numpy as np
import polars as pl

from ..config import StrategyConfig
from ..strategy import admissions, candidate_history, entry_rejection
from .data import HOUR, MinuteStore, digest, utc, write_frame, write_json
from .exits import ExitState, resolve_bar


def build_signals(snapshot: Path, output: Path, config: StrategyConfig) -> pl.DataFrame:
    manifest=json.loads((snapshot/"manifest.json").read_text(encoding="utf-8"))
    fingerprint={"data":digest(snapshot/"manifest.json"),"strategy":digest(config.root/"strategy.toml"),
                 "implementation":digest(Path(__file__).parents[1]/"strategy.py")}
    if (output/"signals.parquet").exists():
        if json.loads((output/"signals_manifest.json").read_text())!=fingerprint:
            raise ValueError("signal cache belongs to different data/code; use a new run directory")
        return pl.read_parquet(output/"signals.parquet")
    start,end=utc(manifest["start"]),utc(manifest["end"])
    month=start.replace(day=1)
    scan=pl.scan_parquet(str(snapshot/"bars/*.parquet"),hive_partitioning=False)
    rows=[]
    while month<end:
        following=(month.replace(day=28)+timedelta(days=4)).replace(day=1)
        stop=min(following,end)
        frame=scan.filter((pl.col("open_time")>=month-timedelta(hours=81))&(pl.col("open_time")<stop)).collect()
        selected=candidate_history(frame,max(month,start),stop,config)
        for row in selected:
            row=dict(row); row["priority"]=json.dumps(row["priority"]); rows.append(row)
        print(f"SIGNALS {month:%Y-%m} added={len(selected)} total={len(rows)}",flush=True)
        month=following
    frame=pl.DataFrame(rows).sort(["decision_time","trade_id"])
    write_frame(output/"signals.parquet",frame)
    write_json(output/"signals_manifest.json",fingerprint)
    return frame


class HourlyMarket:
    def __init__(self, snapshot: Path):
        self.snapshot=snapshot
        self.frames={}
        self.indices={}

    def bar(self, symbol: str, hour: datetime) -> dict:
        if symbol not in self.frames:
            frame=pl.read_parquet(self.snapshot/"bars"/(symbol+".parquet"))
            self.frames[symbol]=frame
            self.indices[symbol]={int(t):i for i,t in enumerate(frame["open_time"].dt.epoch("s").to_numpy())}
        index=self.indices[symbol].get(int(hour.timestamp()))
        if index is None:
            raise ValueError(f"HELD_DATA_GAP {symbol} {hour.isoformat()}; repair data, never drop a trade using future coverage")
        return self.frames[symbol].row(index,named=True)


def replay(snapshot: Path, signals: pl.DataFrame, output: Path, config: StrategyConfig,
           minutes: MinuteStore | None=None, initial_equity: float=10000) -> dict:
    if (output/"summary.json").exists():
        raise ValueError(f"completed run is immutable: {output}")
    output.mkdir(parents=True,exist_ok=True)
    manifest=json.loads((snapshot/"manifest.json").read_text(encoding="utf-8"))
    start,end=utc(manifest["start"]),utc(manifest["end"])
    market=HourlyMarket(snapshot)
    by_time=defaultdict(list)
    for row in signals.to_dicts():
        row["priority"]=tuple(json.loads(row["priority"]))
        by_time[row["decision_time"]].append(row)
    for rows in by_time.values():
        rows.sort(key=lambda x:x["priority"])
    ledger=initial_equity; e0=initial_equity
    lots=[]; trades=[]; decisions=[]; equity=[]; requirements=[]; extensions=[]
    bought=set(); stopped=set(); day=None
    fee=float(config.values["execution"]["taker_fee_per_side"])
    slip=float(config.values["execution"]["slippage_per_side"])
    peak=initial_equity; envelope_peak=initial_equity; worst_envelope=0.; max_account_error=0.
    realized=0.; minutes_used=0; minutes_unavailable=0; minute_ambiguities=0
    hour=start

    def close(lot, reference, reason, when, resolution):
        nonlocal ledger,realized
        fill=reference*(1-lot["direction"]*slip)
        gross=lot["direction"]*lot["quantity"]*(fill-lot["entry_price"])
        exit_fee=lot["quantity"]*fill*fee
        net=gross-exit_fee-lot["entry_fee"]
        ledger+=gross-exit_fee; realized+=net
        record={k:v for k,v in lot.items() if k!="state"}
        record.update(exit_time=when,exit_reference=reference,exit_price=fill,exit_reason=reason,
            exit_fee=exit_fee,net_pnl=net,net_return=net/lot["entry_notional"],resolution=resolution,
            first_extension_activation=lot["state"].activation,profit_armed=lot["state"].armed)
        trades.append(record)
        if lot["strategy"]=="long" and reason=="HARD_STOP":
            stopped.add(lot["symbol"])
        lots.remove(lot)

    while hour<=end:
        # Stop-day locks follow the UTC exit decision boundary, not entry date.
        if day!=hour.date():
            day=hour.date(); bought=set(); stopped=set()
        before_ledger=ledger
        low_marks=[]; high_marks=[]
        if hour>start:
            for lot in list(lots):
                bar=market.bar(lot["symbol"],hour-HOUR)
                result,ambiguous=resolve_bar(lot["state"],bar,hour,fee,slip)
                resolution="hourly"
                if ambiguous:
                    requirements.append({"symbol":lot["symbol"],"hour":hour-HOUR,"trade_id":lot["trade_id"],
                        "reason":"EXIT_ORDER_AMBIGUITY","reference":lot["reference_price"],"armed_before":lot["state"].armed})
                    detail=minutes.load(lot["symbol"],hour-HOUR,bar) if minutes else None
                    if detail is not None:
                        minutes_used+=1; resolution="minute_pessimistic"
                        state=lot["state"]
                        lows=[]; highs=[]
                        for minute in detail.iter_rows(named=True):
                            # Activation remains hour-boundary based, preserving
                            # the frozen strategy clock while refining price order.
                            step,uncertain=resolve_bar(state,minute,hour,fee,slip)
                            minute_ambiguities+=int(uncertain)
                            state=step.state; lows.append(step.low); highs.append(step.high)
                            result=step
                            if step.exit_price is not None:
                                break
                        result.low=min(lows); result.high=max(highs)
                    elif minutes:
                        minutes_unavailable+=1; resolution="hourly_pessimistic_missing_minutes"
                lot["state"]=result.state
                q=lot["quantity"]; direction=lot["direction"]; entry=lot["entry_price"]
                low_price=result.low if direction>0 else result.high
                high_price=result.high if direction>0 else result.low
                low_marks.append(direction*q*(low_price-entry)-q*low_price*fee)
                high_marks.append(direction*q*(high_price-entry))
                if result.exit_price is not None:
                    close(lot,result.exit_price,result.reason,hour+timedelta(minutes=1),resolution)
                    continue
                if hour>=lot["scheduled_exit_time"]:
                    activation=result.state.activation
                    planned=lot["planned_exit_time"]
                    if lot["strategy"]=="long" and not lot["extended"] and activation and planned-timedelta(hours=4)<activation<=planned and hour<end:
                        lot["extended"]=True; lot["scheduled_exit_time"]=planned+timedelta(hours=24)
                        extensions.append({"trade_id":lot["trade_id"],"activated_at":activation,"extended_at":hour,"new_exit":lot["scheduled_exit_time"]})
                    else:
                        close(lot,float(bar["close"]),"EXTENSION_CAP" if lot["extended"] else "PLANNED_EXIT",hour+timedelta(minutes=1),resolution)
            envelope_high=before_ledger+sum(high_marks)
            envelope_low=before_ledger+sum(low_marks)
            envelope_peak=max(envelope_peak,envelope_high)
            worst_envelope=max(worst_envelope,1-envelope_low/envelope_peak)
        else:
            envelope_low=envelope_high=initial_equity
        marks={lot["trade_id"]:float(market.bar(lot["symbol"],hour-HOUR)["close"]) for lot in lots}
        floating=sum(lot["direction"]*lot["quantity"]*(marks[lot["trade_id"]]-lot["entry_price"]) for lot in lots)
        marked=ledger+floating
        if hour.hour==0:
            e0=marked
        if hour==end:
            for lot in list(lots):
                close(lot,marks[lot["trade_id"]],"WINDOW_END",hour+timedelta(minutes=1),"terminal_mark")
            marked=ledger
        else:
            rows=by_time.get(hour,[])
            admitted=[]
            batch=[]
            for row in rows:
                rejection=entry_rejection(row,lots,bought,stopped)
                if rejection:
                    decisions.append({"decision_time":hour,"trade_id":row["trade_id"],"symbol":row["symbol"],"outcome":rejection,"target_notional":0.,"E0":e0,"equity":marked})
                else:
                    admitted.append(row)
            plan=admissions(admitted,lots,Decimal(str(e0)),Decimal(str(marked)),config)
            targets={a.candidate["trade_id"]:float(a.target_notional) for a in plan}
            for row in admitted:
                target=targets.get(row["trade_id"],0.)
                if target<=max(e0,1)*1e-14:
                    decisions.append({"decision_time":hour,"trade_id":row["trade_id"],"symbol":row["symbol"],"outcome":"NO_BUDGET","target_notional":0.,"E0":e0,"equity":marked}); continue
                direction=1 if row["strategy"]=="long" else -1
                fill=row["reference_price"]*(1+direction*slip)
                quantity=target/fill
                entry_fee=target*fee
                ledger-=entry_fee
                lot=dict(row,direction=direction,quantity=quantity,entry_price=fill,entry_fee=entry_fee,entry_notional=target,
                    scheduled_exit_time=row["planned_exit_time"],extended=False,state=ExitState(row["reference_price"],direction))
                lot.pop("priority",None)
                lots.append(lot); batch.append(lot)
                if direction>0:
                    bought.add(row["symbol"])
                decisions.append({"decision_time":hour,"trade_id":row["trade_id"],"symbol":row["symbol"],"outcome":"OPEN","target_notional":target,"E0":e0,"equity":marked})
            floating+=sum(x["direction"]*x["quantity"]*(x["reference_price"]-x["entry_price"]) for x in batch)
            marked=ledger+floating
            occupied=sum(x["entry_notional"] for x in lots)
            if batch and occupied>min(e0,marked)*(1+1e-10):
                raise AssertionError(f"budget violation {hour}: occupied={occupied} equity={marked} E0={e0}")
        expected=initial_equity+realized-sum(lot["entry_fee"] for lot in lots)
        error=abs(ledger-expected)/max(1.,abs(ledger),abs(expected))
        max_account_error=max(max_account_error,error)
        if error>1e-10 or not math.isfinite(marked) or marked<=0:
            raise AssertionError(f"accounting/solvency failure at {hour}: {error} {marked}")
        peak=max(peak,marked)
        equity.append({"time":hour,"equity":marked,"ledger":ledger,"E0":e0,"drawdown":1-marked/peak,
                       "envelope_low":envelope_low,"envelope_high":envelope_high,"envelope_drawdown":worst_envelope,
                       "open_lots":len(lots),"long_occupied":sum(x["entry_notional"] for x in lots if x["strategy"]=="long"),
                       "short_occupied":sum(x["entry_notional"] for x in lots if x["strategy"]=="short")})
        if hour.day==1 and hour.hour==0:
            print(f"REPLAY {output.name} {hour:%Y-%m} trades={len(trades)} equity={marked:.4f}",flush=True)
        hour+=HOUR
    trade_frame=pl.DataFrame(trades)
    eq_frame=pl.DataFrame(equity)
    decision_frame=pl.DataFrame(decisions)
    for name,frame in (("trades",trade_frame),("equity_hourly",eq_frame),("decisions",decision_frame),("extensions",pl.DataFrame(extensions))):
        write_frame(output/(name+".parquet"),frame)
        if frame.width and name in {"trades","equity_hourly"}:
            frame.write_csv(output/(name+".csv"))
    write_json(output/"minute_requirements.json",requirements)
    if minutes:
        write_json(output/"minute_audit.json",minutes.audit)
    summary={"start":start,"end":end,"initial_equity":initial_equity,"final_equity":ledger,"equity_multiple":ledger/initial_equity,
        "trades":len(trades),"signals":signals.height,"win_rate":sum(x["net_pnl"]>0 for x in trades)/len(trades) if trades else None,
        "hard_stops":sum(x["exit_reason"]=="HARD_STOP" for x in trades),"max_close_drawdown":max(x["drawdown"] for x in equity),
        "max_envelope_drawdown":worst_envelope,"extensions":len(extensions),"terminal_exits":sum(x["exit_reason"]=="WINDOW_END" for x in trades),
        "minute_lot_hours_requested":len(requirements),"minute_unique_hours":len({(x['symbol'],x['hour']) for x in requirements}),
        "minute_lot_hours_used":minutes_used,"minute_lot_hours_unavailable":minutes_unavailable,"ambiguous_minute_lot_bars":minute_ambiguities,
        "max_ledger_relative_error":max_account_error,"remaining_lots":len(lots),"funding_model":"excluded",
        "execution_model":"hour-boundary fills; reference close +/- 10bp; fee 5bp/side; T+1m is timestamp approximation",
        "intrabar_model":"locally pessimistic feasible paths; not a compound-account mathematical lower bound",
        "limitations":["funding excluded","no capacity impact","no exchange lot sizes","no maintenance margin/liquidation",
                       "incomplete historical contract metadata","first entry minute not removed","live polls vs historical high/low differ"]}
    # Independent trade cashflow formulas (not a second call to the simulator).
    expected_pnl=sum(x["direction"]*x["quantity"]*(x["exit_price"]-x["entry_price"])-x["entry_fee"]-x["exit_fee"] for x in trades)
    if abs(ledger-initial_equity-expected_pnl)>max(1,abs(ledger))*1e-10:
        raise AssertionError("terminal cashflow does not reconcile")
    write_json(output/"validation.json",{"status":"passed","terminal_cashflow_relative_error":abs(ledger-initial_equity-expected_pnl)/max(1,abs(ledger)),
        "hourly_ledger_max_relative_error":max_account_error,"open_positions":len(lots),"held_hour_gaps":0})
    write_json(output/"summary.json",summary)
    print(f"REPLAY COMPLETE {output.name}: trades={len(trades)} multiple={ledger/initial_equity:.6f} minute_hours={summary['minute_unique_hours']}",flush=True)
    return summary
