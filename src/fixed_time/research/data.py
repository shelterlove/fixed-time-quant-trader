from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
import hashlib
import io
import json
from pathlib import Path
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, quote
from urllib.request import Request, urlopen
from zipfile import ZipFile

import polars as pl

from ..strategy import BAR_COLUMNS

SCHEMA = {"symbol":pl.String,"open_time":pl.Datetime("us","UTC"),
          **{k:pl.Float64 for k in BAR_COLUMNS[2:-1]},"trade_count":pl.Int64}
HOUR = timedelta(hours=1)


def utc(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z","+00:00"))
    if result.tzinfo is None:
        raise ValueError("explicit UTC offset required")
    return result.astimezone(UTC)


def digest(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f,"sha256").hexdigest()


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    temp = path.with_suffix(path.suffix+".tmp")
    temp.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    temp.replace(path)


def write_frame(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(".tmp")
    frame.write_parquet(temp,compression="zstd")
    temp.replace(path)


class DownloadError(RuntimeError):
    pass


class PublicData:
    """Public GET only; global pacing, bounded retry, IP-weight awareness.

    At most two concurrent users, >= 0.35s between request starts. Respect
    Retry-After across all workers. 418/403 stop the downloader immediately.
    """
    def __init__(self, cache: Path):
        self.cache=cache
        self.lock=threading.Lock()
        self.slots=threading.BoundedSemaphore(2)
        self.next_request=0.0
        self.stopped=False
        self.weight_limit=2400

    def get(self, url: str) -> bytes:
        key=hashlib.sha256(url.encode()).hexdigest()
        path=self.cache/(key+".bin")
        if path.exists():
            return path.read_bytes()
        with self.slots:
            for attempt in range(4):
                while True:
                    with self.lock:
                        if self.stopped:
                            raise DownloadError("download stopped after exchange ban/WAF response")
                        wait=max(0,self.next_request-time.monotonic())
                        if not wait:
                            self.next_request=time.monotonic()+.35
                            break
                    time.sleep(min(wait,60))
                try:
                    with urlopen(Request(url,headers={"User-Agent":"fixed-time-research/2.0"}),timeout=30) as response:
                        payload=response.read()
                        expected=int(response.headers.get("Content-Length",0))
                        if expected and len(payload)!=expected:
                            raise OSError("truncated download")
                        used=int(response.headers.get("X-MBX-USED-WEIGHT-1M",0))
                        if used>=self.weight_limit*.7:
                            with self.lock:
                                self.next_request=max(self.next_request,time.monotonic()+60)
                        headers={k:v for k,v in response.headers.items() if k.lower() in {"date","etag","last-modified","x-mbx-used-weight-1m"}}
                    path.parent.mkdir(parents=True,exist_ok=True)
                    temp=path.with_suffix(".tmp")
                    temp.write_bytes(payload)
                    temp.replace(path)
                    write_json(path.with_suffix(".json"),{"url":url,"retrieved_at":datetime.now(UTC),
                        "sha256":hashlib.sha256(payload).hexdigest(),"headers":headers})
                    return payload
                except HTTPError as exc:
                    if exc.code in {418,403}:
                        self.stopped=True
                        raise DownloadError(f"HTTP {exc.code}: halted; no automatic ban bypass") from exc
                    if exc.code not in {429,408,500,502,503,504}:
                        raise
                    retry=exc.headers.get("Retry-After")
                    try:
                        delay=float(retry) if retry else max(5,2**attempt)
                    except ValueError:
                        delay=max(0,(parsedate_to_datetime(retry)-datetime.now(UTC)).total_seconds())
                    if delay>60:
                        raise DownloadError(f"server requests {delay:.0f}s cooldown; resume later with cached downloads") from exc
                    with self.lock:
                        self.next_request=max(self.next_request,time.monotonic()+delay)
                except (OSError,URLError) as exc:
                    if attempt==3:
                        raise DownloadError(f"network failure: {url}") from exc
                    with self.lock:
                        self.next_request=max(self.next_request,time.monotonic()+2**attempt)
            raise DownloadError(f"retry budget exhausted: {url}")

    def catalogue(self) -> dict:
        result=json.loads(self.get("https://fapi.binance.com/fapi/v1/exchangeInfo"))
        for limit in result.get("rateLimits",[]):
            if limit.get("rateLimitType")=="REQUEST_WEIGHT" and limit.get("interval")=="MINUTE" and limit.get("intervalNum")==1:
                self.weight_limit=int(limit["limit"])
        return result

    def klines(self, symbol: str, interval: str, start: datetime, end: datetime) -> pl.DataFrame:
        step=HOUR if interval=="1h" else timedelta(minutes=1)
        rows=[]
        while start<end:
            params={"symbol":symbol,"interval":interval,"startTime":int(start.timestamp()*1000),
                    "endTime":int(end.timestamp()*1000)-1,"limit":499}
            raw=json.loads(self.get("https://fapi.binance.com/fapi/v1/klines?"+urlencode(params)))
            if not isinstance(raw,list):
                raise DownloadError(f"invalid public klines response: {symbol}")
            if not raw:
                break
            page=[{"symbol":symbol,"open_time":datetime.fromtimestamp(int(x[0])/1000,UTC),
                   "open":float(x[1]),"high":float(x[2]),"low":float(x[3]),"close":float(x[4]),
                   "quote_volume":float(x[7]),"trade_count":int(x[8])} for x in raw]
            if page[-1]["open_time"]<start:
                raise DownloadError("non advancing kline pagination")
            rows.extend(x for x in page if start<=x["open_time"]<end)
            start=page[-1]["open_time"]+step
        return pl.DataFrame(rows,schema=SCHEMA)

    def archive_day(self, symbol: str, day: datetime, interval: str) -> pl.DataFrame:
        name=f"{symbol}-{interval}-{day:%Y-%m-%d}.zip"
        base=f"https://data.binance.vision/data/futures/um/daily/klines/{quote(symbol)}/{interval}/{quote(name)}"
        payload=self.get(base)
        checksum=self.get(base+".CHECKSUM").decode().split()[0]
        if hashlib.sha256(payload).hexdigest()!=checksum:
            raise DownloadError("official archive checksum mismatch")
        with ZipFile(io.BytesIO(payload)) as archive:
            if archive.testzip() is not None or len(archive.namelist())!=1:
                raise DownloadError("invalid archive")
            content=archive.read(archive.namelist()[0])
        frame=pl.read_csv(content,has_header=content.startswith(b"open_time"),infer_schema_length=0)
        names=frame.columns
        result=frame.select(pl.lit(symbol).alias("symbol"),
            pl.from_epoch(pl.col(names[0]).cast(pl.Int64),time_unit="ms").dt.replace_time_zone("UTC").cast(pl.Datetime("us","UTC")).alias("open_time"),
            *[pl.col(names[i]).cast(pl.Float64).alias(k) for i,k in [(1,"open"),(2,"high"),(3,"low"),(4,"close"),(7,"quote_volume")]],
            pl.col(names[8]).cast(pl.Int64).alias("trade_count"))
        return result

    def minute_archive(self, symbol: str, hour: datetime) -> pl.DataFrame:
        return self.archive_day(symbol,hour,"1m").filter((pl.col("open_time")>=hour)&(pl.col("open_time")<hour+HOUR))


def validate(frame: pl.DataFrame, step_seconds: int=3600) -> None:
    if frame.is_empty():
        return
    if frame.select(pl.struct("symbol","open_time").is_duplicated().any()).item():
        raise ValueError("duplicate market data keys")
    invalid=frame.filter(~pl.all_horizontal([pl.col(k).is_finite().fill_null(False) for k in BAR_COLUMNS[2:]])
        | (pl.col("low")<=0) | (pl.col("high")<pl.max_horizontal("open","close","low"))
        | (pl.col("low")>pl.min_horizontal("open","close")) | (pl.col("quote_volume")<0)
        | (pl.col("trade_count")<0) | (pl.col("open_time").dt.epoch("s")%step_seconds!=0))
    if invalid.height:
        raise ValueError(f"invalid market bars: {invalid.head(2).to_dicts()}")


def prepare(source: Path, target: Path, start: datetime, end: datetime, supplement: bool=False) -> dict:
    if (target/"manifest.json").exists():
        manifest=json.loads((target/"manifest.json").read_text(encoding="utf-8"))
        if utc(manifest["start"])!=start or utc(manifest["end"])!=end:
            raise ValueError("snapshot exists with different boundaries")
        return manifest
    target.mkdir(parents=True,exist_ok=True)
    raw=source/"data/raw/klines_1h"
    if not raw.is_dir():
        raise ValueError(f"missing hourly source: {raw}")
    warmup=start-timedelta(hours=81)
    http=PublicData(target.parent.parent/"raw/http")
    folders={p.name.split("=",1)[1]:p for p in raw.glob("symbol=*")}
    current={}
    if supplement:
        catalogue=http.catalogue()
        write_json(target/"exchange_info.json",catalogue)
        current={x["symbol"]:x for x in catalogue["symbols"] if x.get("quoteAsset")=="USDT" and x.get("contractType")=="PERPETUAL" and x.get("status")=="TRADING"}
    symbols=sorted(set(folders)|set(current))

    def import_symbol(symbol):
        path=target/"bars"/(symbol+".parquet")
        receipt=target/"receipts"/(symbol+".json")
        if path.exists() and receipt.exists():
            return json.loads(receipt.read_text(encoding="utf-8"))
        files=sorted(folders[symbol].rglob("*.parquet")) if symbol in folders else []
        frames=[]; inputs=[]
        for file in files:
            year=int(file.parent.parent.name.split("=")[1]); month=int(file.parent.name.split("=")[1])
            if (year,month)<(warmup.year,warmup.month) or (year,month)>(end.year,end.month):
                continue
            f=pl.read_parquet(file).select(BAR_COLUMNS).cast(SCHEMA).filter((pl.col("open_time")>=warmup)&(pl.col("open_time")<end))
            if f.height:
                frames.append(f); inputs.append({"path":str(file),"sha256":digest(file),"rows":f.height})
        frame=pl.concat(frames).sort("open_time") if frames else pl.DataFrame(schema=SCHEMA)
        downloads=[]
        if supplement and symbol in current:
            first_missing=frame["open_time"].max()+HOUR if frame.height else max(warmup,datetime.fromtimestamp(current[symbol]["onboardDate"]/1000,UTC).replace(minute=0,second=0,microsecond=0))
            if first_missing<end:
                extra=http.klines(symbol,"1h",first_missing,end)
                downloads.append({"start":first_missing,"end":end,"rows":extra.height})
                frame=pl.concat([frame,extra]).sort("open_time")
        validate(frame)
        gaps=[]
        times=frame["open_time"].to_list()
        for a,b in zip(times,times[1:]):
            if b-a>HOUR:
                gaps.append({"symbol":symbol,"start":a+HOUR,"end":b,"hours":int((b-a)/HOUR)-1})
        write_frame(path,frame)
        result={"symbol":symbol,"rows":frame.height,"first":times[0] if times else None,"last":times[-1] if times else None,
                "path":str(path.relative_to(target)),"sha256":digest(path),"inputs":inputs,"supplement":downloads,"gaps":gaps}
        write_json(receipt,result)
        return result

    records=[]
    # Disk reads are parallel; all public calls share the two-slot global limiter.
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs={pool.submit(import_symbol,s):s for s in symbols}
        for job in as_completed(jobs):
            records.append(job.result())
            if len(records)%100==0:
                print(f"DATA {len(records)}/{len(symbols)} symbols",flush=True)
    records.sort(key=lambda x:x["symbol"])
    gaps=[x for r in records for x in r["gaps"]]
    manifest={"schema_version":1,"created_at":datetime.now(UTC),"start":start,"end":end,"warmup":warmup,
        "source_root":str(source),"symbols":len(records),"rows":sum(x["rows"] for x in records),"records":records,
        "internal_gap_hours":sum(x["hours"] for x in gaps),"current_catalogue_supplement":supplement,
        "universe_note":"Historical symbols retained. Current catalogue only supplements recent data; historical contract eligibility metadata not reconstructed."}
    write_json(target/"coverage_gaps.json",gaps)
    write_json(target/"manifest.json",manifest)
    print(f"DATA COMPLETE rows={manifest['rows']} internal_gap_hours={manifest['internal_gap_hours']}",flush=True)
    return manifest


def repair_snapshot(source: Path, target: Path, local_source: Path) -> dict:
    """Repair observed internal gaps into a NEW snapshot, never fill forward.

    Aggregate complete local minutes first. Download only still-missing hourly
    ranges; delisted symbols can use official daily archives. Unavailable bars
    remain explicit gaps and will fail a replay if held.
    """
    if source.resolve()==target.resolve():
        raise ValueError("repairs require a new snapshot id")
    if (target/"manifest.json").exists():
        return json.loads((target/"manifest.json").read_text(encoding="utf-8"))
    manifest=json.loads((source/"manifest.json").read_text(encoding="utf-8"))
    http=PublicData(target.parent.parent/"raw/http")
    records=[]
    import shutil
    for record in manifest["records"]:
        symbol=record["symbol"]
        destination=target/record["path"]
        receipt=target/"receipts"/(symbol+".json")
        if destination.exists() and receipt.exists():
            records.append(json.loads(receipt.read_text(encoding="utf-8"))); continue
        destination.parent.mkdir(parents=True,exist_ok=True)
        if not record["gaps"]:
            shutil.copyfile(source/record["path"],destination)
            write_json(receipt,record); records.append(record); continue
        frame=pl.read_parquet(source/record["path"])
        patches=[]; log=[]
        for gap in record["gaps"]:
            begin,finish=utc(gap["start"]),utc(gap["end"])
            wanted={begin+HOUR*i for i in range(int((finish-begin)/HOUR))}
            days=sorted({x.date() for x in wanted})
            local_count=0
            for day in days:
                path=local_source/"data/raw/klines_1m"/f"symbol={symbol}"/f"date={day.isoformat()}"/"part.parquet"
                if not path.exists():
                    continue
                minutes=pl.read_parquet(path)
                for hour in sorted(x for x in wanted if x.date()==day):
                    part=minutes.filter((pl.col("open_time")>=hour)&(pl.col("open_time")<hour+HOUR)).sort("open_time")
                    if part.height!=60 or part["open_time"].to_list()!=[hour+timedelta(minutes=i) for i in range(60)]:
                        continue
                    validate(part,60)
                    patches.append(pl.DataFrame([{"symbol":symbol,"open_time":hour,"open":part["open"][0],"close":part["close"][-1],
                        "high":part["high"].max(),"low":part["low"].min(),"quote_volume":part["quote_volume"].sum(),"trade_count":part["trade_count"].sum()}],schema=SCHEMA))
                    wanted.remove(hour); local_count+=1
            # Request only contiguous outstanding ranges.
            missing=sorted(wanted); ranges=[]
            for hour in missing:
                if ranges and ranges[-1][1]==hour:
                    ranges[-1]=(ranges[-1][0],hour+HOUR)
                else:
                    ranges.append((hour,hour+HOUR))
            network_count=0
            for left,right in ranges:
                rest=pl.DataFrame(schema=SCHEMA)
                try:
                    rest=http.klines(symbol,"1h",left,right)
                except HTTPError as exc:
                    if exc.code!=400:
                        raise

                # The live REST endpoint can return HTTP 200 with an empty or
                # partial result for old contract history.  Fall back for every
                # requested hour REST did not supply, rather than only on 400.
                expected={left+HOUR*i for i in range(int((right-left)/HOUR))}
                absent=expected-set(rest["open_time"].to_list())
                archive_frames=[]
                for day in sorted({hour.replace(hour=0) for hour in absent}):
                    try:
                        archive_frames.append(http.archive_day(symbol,day,"1h"))
                    except HTTPError as missing_archive:
                        if missing_archive.code!=404:
                            raise
                available=[rest,*archive_frames]
                extra=(pl.concat(available).filter(
                    (pl.col("open_time")>=left)&(pl.col("open_time")<right)
                ).unique(["symbol","open_time"],keep="first").sort("open_time")
                    if available else pl.DataFrame(schema=SCHEMA))
                patches.append(extra); network_count+=extra.height
            log.append({**gap,"local_minute_aggregated_hours":local_count,"downloaded_hours":network_count})
        if patches:
            frame=pl.concat([frame,*patches]).sort("open_time")
        validate(frame)
        times=frame["open_time"].to_list()
        gaps=[{"symbol":symbol,"start":a+HOUR,"end":b,"hours":int((b-a)/HOUR)-1,
               "classification":"verified_no_published_kline"}
              for a,b in zip(times,times[1:]) if b-a>HOUR]
        write_frame(destination,frame)
        revised=dict(record,rows=frame.height,sha256=digest(destination),gaps=gaps,repairs=log)
        write_json(receipt,revised); records.append(revised)
        print(f"REPAIR {symbol}: added={frame.height-record['rows']} remaining_gap_hours={sum(x['hours'] for x in gaps)}",flush=True)
    remaining=[x for r in records for x in r["gaps"]]
    verified_no_kline_hours=sum(x["hours"] for x in remaining)
    result=dict(manifest,created_at=str(datetime.now(UTC)),base_manifest_sha256=digest(source/"manifest.json"),records=records,
                rows=sum(x["rows"] for x in records),internal_gap_hours=verified_no_kline_hours,
                unresolved_gap_hours=0,verified_no_published_kline_hours=verified_no_kline_hours,
                coverage_note="REST and official daily archives were both checked; remaining intervals have no published Binance kline and are not imputed.")
    write_json(target/"coverage_gaps.json",remaining)
    write_json(target/"manifest.json",result)
    print(f"REPAIR COMPLETE remaining_gap_hours={result['internal_gap_hours']}",flush=True)
    return result


class MinuteStore:
    def __init__(self, local_source: Path, cache: Path, *, download: bool):
        self.local_source=local_source
        self.cache=cache
        self.http=PublicData(cache.parent/"raw/http")
        self.download=download
        self.audit=[]

    def load(self, symbol: str, hour: datetime, parent: dict) -> pl.DataFrame | None:
        target=self.cache/symbol/f"{hour:%Y%m%d%H}.parquet"
        origin="cache"
        if target.exists():
            frame=pl.read_parquet(target)
        else:
            local=self.local_source/"data/raw/klines_1m"/f"symbol={symbol}"/f"date={hour:%Y-%m-%d}"/"part.parquet"
            frame=pl.read_parquet(local).filter((pl.col("open_time")>=hour)&(pl.col("open_time")<hour+HOUR)) if local.exists() else pl.DataFrame(schema=SCHEMA)
            origin="local"
            if not self._valid(frame,parent,hour) and self.download:
                try:
                    frame=self.http.klines(symbol,"1m",hour,hour+HOUR); origin="public_rest"
                except HTTPError as exc:
                    if exc.code!=400:
                        raise
                    try:
                        frame=self.http.minute_archive(symbol,hour); origin="official_daily_archive"
                    except HTTPError as archive_error:
                        if archive_error.code!=404:
                            raise
                        frame=pl.DataFrame(schema=SCHEMA); origin="unavailable"
            if self._valid(frame,parent,hour):
                write_frame(target,frame)
        valid=self._valid(frame,parent,hour)
        record={"symbol":symbol,"hour":hour,"origin":origin,"rows":frame.height,"accepted":valid,
                "sha256":digest(target) if valid and target.exists() else None}
        self.audit.append(record)
        self.cache.mkdir(parents=True,exist_ok=True)
        with (self.cache/"requests.jsonl").open("a",encoding="utf-8") as handle:
            handle.write(json.dumps(record,default=str)+"\n")
        return frame if valid else None

    @staticmethod
    def _valid(frame, parent, hour):
        if frame.height!=60:
            return False
        try:
            validate(frame,60)
        except ValueError:
            return False
        frame=frame.sort("open_time")
        if frame["open_time"].to_list()!=[hour+timedelta(minutes=i) for i in range(60)]:
            return False
        aggregated={"open":frame["open"][0],"close":frame["close"][-1],"high":frame["high"].max(),"low":frame["low"].min(),
                    "quote_volume":frame["quote_volume"].sum(),"trade_count":frame["trade_count"].sum()}
        return all(abs(aggregated[k]-float(parent[k]))<=max(1e-8,abs(float(parent[k]))*1e-7) for k in aggregated)
