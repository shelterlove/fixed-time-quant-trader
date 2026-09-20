from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import polars as pl
import pytest

from fixed_time.config import load_strategy
from fixed_time.research.data import MinuteStore, SCHEMA, repair_snapshot, write_frame, write_json
from fixed_time.research.exits import ExitState, resolve_bar
from fixed_time.research.replay import replay
from fixed_time.strategy import candidate_history, candidates

NOW=datetime(2026,1,1,14,tzinfo=UTC)


def test_intrabar_arm_retrace_is_flagged_and_worst_feasible_exit_selected():
    result,ambiguous=resolve_bar(ExitState(100,1),dict(open=390,high=510,low=365,close=480),NOW)
    assert ambiguous
    assert result.reason=="PROFIT_FLOOR270"
    assert result.exit_price==370
    assert result.high==400  # Future post-exit high must not inflate held exposure.


def test_hard_stop_gap_and_irrelevant_activation_do_not_need_minutes():
    result,ambiguous=resolve_bar(ExitState(100,1),dict(open=100,high=140,low=65,close=90),NOW)
    assert result.exit_price==70
    assert not ambiguous  # Both paths exit at the same stop; activation is irrelevant.
    result,_=resolve_bar(ExitState(100,-1),dict(open=145,high=150,low=140,close=148),NOW)
    assert result.exit_price==145


def test_minute_validation_requires_full_hour_and_matching_parent():
    rows=[dict(symbol="AUSDT",open_time=NOW+timedelta(minutes=i),open=100.,high=101.,low=99.,close=100.,quote_volume=10.,trade_count=1) for i in range(60)]
    frame=pl.DataFrame(rows,schema=SCHEMA)
    parent=dict(open=100,high=101,low=99,close=100,quote_volume=600,trade_count=60)
    assert MinuteStore._valid(frame,parent,NOW)
    assert not MinuteStore._valid(frame.head(59),parent,NOW)
    assert not MinuteStore._valid(frame,dict(parent,high=102),NOW)


def test_hour_repair_uses_archive_when_rest_is_partial(tmp_path,monkeypatch):
    source,target=tmp_path/"source",tmp_path/"target"
    def bar(hour):
        return dict(symbol="AUSDT",open_time=NOW+timedelta(hours=hour),open=100.,high=101.,
                    low=99.,close=100.,quote_volume=10.,trade_count=1)
    write_frame(source/"bars/AUSDT.parquet",pl.DataFrame([bar(0),bar(3)],schema=SCHEMA))
    record=dict(symbol="AUSDT",path="bars/AUSDT.parquet",rows=2,sha256="original",
                gaps=[dict(symbol="AUSDT",start=NOW+timedelta(hours=1),
                           end=NOW+timedelta(hours=3),hours=2)])
    write_json(source/"manifest.json",dict(records=[record],rows=2,internal_gap_hours=2))

    class FakePublicData:
        def __init__(self,_cache): pass
        def klines(self,_symbol,_interval,_start,_end):
            return pl.DataFrame([bar(1)],schema=SCHEMA)
        def archive_day(self,_symbol,_day,_interval):
            return pl.DataFrame([bar(2)],schema=SCHEMA)

    monkeypatch.setattr("fixed_time.research.data.PublicData",FakePublicData)
    manifest=repair_snapshot(source,target,tmp_path/"local")
    assert manifest["internal_gap_hours"]==0
    assert pl.read_parquet(target/"bars/AUSDT.parquet").height==4


def test_batch_signal_kernel_matches_live_and_ignores_future_bars():
    config=load_strategy(".")
    rows=[]
    for i in range(84):
        close=100*1.002**i
        rows.append(dict(symbol="AUSDT",open_time=NOW-timedelta(hours=81-i),open=close,high=close*1.01,
                         low=close*.99,close=close,quote_volume=1000.,trade_count=10))
    frame=pl.DataFrame(rows,schema=SCHEMA)
    assert candidate_history(frame,NOW,NOW+timedelta(hours=1),config)==candidates(frame,NOW,config)


def test_replay_cashflows_fees_and_terminal_liquidation(tmp_path):
    config=load_strategy(".")
    snapshot=tmp_path/"snapshot"
    end=NOW+timedelta(hours=2)
    write_json(snapshot/"manifest.json",dict(start=NOW,end=end))
    bars=pl.DataFrame([dict(symbol="AUSDT",open_time=NOW-timedelta(hours=1)+timedelta(hours=i),
        open=100.,high=105.,low=95.,close=100.,quote_volume=1000.,trade_count=10) for i in range(3)],schema=SCHEMA)
    write_frame(snapshot/"bars/AUSDT.parquet",bars)
    signals=pl.DataFrame([dict(trade_id="test",symbol="AUSDT",strategy="long",position_side="LONG",source="MAIN",
        decision_time=NOW,entry_time=NOW+timedelta(minutes=1),planned_exit_time=NOW+timedelta(hours=18),
        reference_price=100.,priority=json.dumps(["AUSDT"]))])
    result=replay(snapshot,signals,tmp_path/"run",config)
    trade=pl.read_parquet(tmp_path/"run/trades.parquet").row(0,named=True)
    assert trade["entry_notional"]==2500
    assert trade["exit_reason"]=="WINDOW_END"
    q=2500/100.1
    expected=10000-1.25+q*(99.9-100.1)-q*99.9*.0005
    assert result["final_equity"]==pytest.approx(expected)
    assert result["remaining_lots"]==0
