"""Causal OHLC exit simulator; ambiguity is a data requirement, not hidden luck."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime


@dataclass
class ExitState:
    reference: float
    direction: int
    activation: datetime | None = None
    armed: bool = False


@dataclass
class PathResult:
    state: ExitState
    exit_price: float | None
    reason: str | None
    low: float
    high: float


def _walk(state: ExitState, points: list[float], at: datetime) -> PathResult:
    state=replace(state)
    ref=state.reference
    low=high=points[0]
    opening=points[0]
    if state.direction<0:
        threshold=ref*1.3
        if opening>=threshold:
            return PathResult(state,opening,"HARD_STOP",opening,opening)
    else:
        threshold=ref*(3.7 if state.armed else .7)
        if opening<=threshold:
            return PathResult(state,opening,"PROFIT_FLOOR270" if state.armed else "HARD_STOP",opening,opening)
        if opening>=ref*5:
            return PathResult(state,ref*5,"PROFIT_CAP400",ref*5,ref*5)
        if opening>=ref*1.3 and state.activation is None:
            state.activation=at
        if opening>=ref*4:
            state.armed=True
    previous=opening
    for point in points[1:]:
        if state.direction<0 and point>=ref*1.3:
            high=max(high,ref*1.3)
            return PathResult(state,ref*1.3,"HARD_STOP",low,high)
        if state.direction>0:
            stop=ref*(3.7 if state.armed else .7)
            if point<previous and point<=stop:
                low=min(low,stop)
                return PathResult(state,stop,"PROFIT_FLOOR270" if state.armed else "HARD_STOP",low,high)
            if point>previous:
                if point>=ref*1.3 and state.activation is None:
                    state.activation=at
                if point>=ref*4:
                    state.armed=True
                if point>=ref*5:
                    high=max(high,ref*5)
                    return PathResult(state,ref*5,"PROFIT_CAP400",low,high)
        low=min(low,point); high=max(high,point); previous=point
    return PathResult(state,None,None,low,high)


def resolve_bar(state: ExitState, bar: dict, at: datetime, fee: float=.0005, slip: float=.001) -> tuple[PathResult,bool]:
    """Worst feasible lot value at bar close, including an intrabar arm/retrace.

    This local conservative convention is not a mathematical lower bound on a
    compound portfolio. States are copied for each path; no future bar is read.
    """
    o,h,l,c=(float(bar[k]) for k in ("open","high","low","close"))
    paths=[[o,l,h,c],[o,h,l,c]]
    if state.direction>0 and not state.armed and o<state.reference*4<=h and l<=state.reference*3.7:
        paths.append([o,state.reference*4,l,h,c])
    results=[_walk(state,path,at) for path in paths]
    def value(result):
        if result.exit_price is None:
            return state.direction*c
        fill=result.exit_price*(1-state.direction*slip)
        return state.direction*fill-fee*fill
    def signature(r):
        return (r.exit_price,r.reason) if r.exit_price is not None else (None,r.state.activation,r.state.armed)
    ambiguous=len({signature(x) for x in results})>1
    selected=min(enumerate(results),key=lambda pair:(value(pair[1]),pair[0]))[1]
    return selected,ambiguous
