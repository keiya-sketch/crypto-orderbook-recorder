"""Binance の板情報(L2 order book diff)と約定(trade)を記録し、Bronze 層の Parquet に書き出す。

目的: 「AIシグナルより執行品質(約定価格・逆選択)で差がつく」という PolyMarket bot 開発の知見が
暗号資産(CEX)でも実測できるかを検証するためのデータ収集。分析は行わない(収集のみ)。

設計原則(investment-validation リポジトリを踏襲):
  - Bronze は生データを改変しない。価格・数量は Binance のレスポンス文字列のまま保存する
    (型変換・逆選択計算などの解釈が必要な処理は Silver 層 = Mac 側で行う)。
  - 常駐プロセスを作らない。GitHub Actions の1回の実行内で完結する有限時間バッチとして動く
    (DURATION_SECONDS 経過で自ら終了する。「気づいたら止まっていた」を起こさない)。
  - べき等に近づける。1時間パーティションごとにファイルを丸ごと上書きするので、
    同じ時間帯を再実行しても壊れない。
  - 途中経過を失わない。FLUSH_INTERVAL_SECONDS ごとに中間フラッシュする
    (ジョブが強制終了されても、直前のフラッシュまでのデータは残る)。

板の再構成手順(Binance 公式ドキュメント準拠):
  1. WebSocket で diff イベント (<symbol>@depth@100ms) の受信を開始する。
  2. REST で snapshot を取得する(lastUpdateId 付き)。
  3. 適用済みイベントの u <= lastUpdateId のものは古いイベントとして無視する。
  4. イベントの U (最初の更新ID) が (直前のu + 1) と一致しない場合は欠落とみなし、
     REST snapshot を取り直して resync する。
  簡略化: 事前バッファリングはせず、snapshot取得直後に届いたイベントで整合性が崩れていたら
  そのまま resync をやり直す(REST往復の数百msぶんだけ余計にresyncが走りうるが、
  apply_event の整合性チェックにより誤った板は残らない)。

エンドポイント: GitHub Actions のランナー(主に米国IP)は api.binance.com / stream.binance.com
から地域制限(451)を受けることがある(investment-validation の binance_klines.py で実測済み)。
market data 専用の data-api.binance.vision / data-stream.binance.vision は地域制限がないため
既定で使う(いずれも公開エンドポイントで API キー不要)。

出力:
  {out}/bronze/binance/orderbook/symbol={SYMBOL}/date={YYYY-MM-DD}/hour={HH}/data.parquet
  {out}/bronze/binance/trades/symbol={SYMBOL}/date={YYYY-MM-DD}/hour={HH}/data.parquet
  {out}/ledger/orderbook_runs/date={YYYY-MM-DD}/run_id={run_id}_{symbol}.parquet
  {out}/ledger/latest_run.json  (直近の実行サマリ。GitHub Actions が commit してリポジトリを生かし続ける用)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
import websockets

DEFAULT_REST_BASE = "https://data-api.binance.vision"
DEFAULT_WS_BASE = "wss://data-stream.binance.vision"
DEFAULT_DEPTH_LEVELS = 20        # 上位N階層をスナップショットとして保存
SNAPSHOT_INTERVAL_SECONDS = 1.0  # 板スナップショットを書き出す間隔
FLUSH_INTERVAL_SECONDS = 300     # 中間フラッシュの間隔(5分)
IDLE_TIMEOUT_SECONDS = 30        # この間隔でイベントが来なければ生存確認ログを出す
SCHEMA_VERSION = 1

BOOK_SCHEMA = pa.schema([
    ("ts", pa.timestamp("us", tz="UTC")),
    ("symbol", pa.string()),
    ("last_update_id", pa.int64()),
    ("bid_prices", pa.list_(pa.string())),
    ("bid_qtys", pa.list_(pa.string())),
    ("ask_prices", pa.list_(pa.string())),
    ("ask_qtys", pa.list_(pa.string())),
    ("ingested_at", pa.timestamp("us", tz="UTC")),
])

TRADE_SCHEMA = pa.schema([
    ("ts", pa.timestamp("ms", tz="UTC")),
    ("symbol", pa.string()),
    ("trade_id", pa.int64()),
    ("price", pa.string()),
    ("qty", pa.string()),
    ("is_buyer_maker", pa.bool_()),
    ("ingested_at", pa.timestamp("us", tz="UTC")),
])

RUN_SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("symbol", pa.string()),
    ("started_at", pa.timestamp("us", tz="UTC")),
    ("finished_at", pa.timestamp("us", tz="UTC")),
    ("book_snapshots", pa.int64()),
    ("trades_captured", pa.int64()),
    ("resync_count", pa.int64()),
    ("status", pa.string()),
    ("error_message", pa.string()),
    ("schema_version", pa.int32()),
])


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class LocalOrderBook:
    """Binance の diff イベントから板をローカルに再構成する。"""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: dict[str, str] = {}
        self.asks: dict[str, str] = {}
        self.last_update_id: int | None = None

    def load_snapshot(self, snapshot: dict) -> None:
        self.bids = {p: q for p, q in snapshot["bids"]}
        self.asks = {p: q for p, q in snapshot["asks"]}
        self.last_update_id = snapshot["lastUpdateId"]

    def apply_event(self, event: dict) -> bool:
        """diff イベントを適用する。整合性が崩れていれば False (呼び出し側が resync する)。"""
        if self.last_update_id is None:
            return False
        if event["u"] <= self.last_update_id:
            return True  # 古いイベント。無視してよい
        if event["U"] > self.last_update_id + 1:
            return False  # 欠落がある

        for price, qty in event["b"]:
            if float(qty) == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for price, qty in event["a"]:
            if float(qty) == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

        self.last_update_id = event["u"]
        return True

    def top_levels(self, n: int) -> tuple[list[str], list[str], list[str], list[str]]:
        bid_items = sorted(self.bids.items(), key=lambda kv: float(kv[0]), reverse=True)[:n]
        ask_items = sorted(self.asks.items(), key=lambda kv: float(kv[0]))[:n]
        return (
            [p for p, _ in bid_items], [q for _, q in bid_items],
            [p for p, _ in ask_items], [q for _, q in ask_items],
        )


def fetch_snapshot(symbol: str, limit: int, rest_base: str, max_retries: int = 5) -> dict:
    url = f"{rest_base}/api/v3/depth"
    last_error = "不明なエラー"
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params={"symbol": symbol, "limit": limit}, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            last_error = f"{type(e).__name__}: {e}"
            wait = 2 ** attempt
            print(f"  [{symbol}] snapshot取得失敗 ({e}). {wait}秒後に再試行", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"[{symbol}] snapshot取得に失敗: {last_error}")


def _rows_to_table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    cols = {f.name: [r[f.name] for r in rows] for f in schema}
    arrays = [pa.array(cols[f.name], type=f.type) for f in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _write_parquet_append(table: pa.Table, path: Path) -> None:
    """既存ファイルがあれば連結して上書きする(1時間パーティション内での複数回フラッシュに対応)。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = pq.read_table(path)
        table = pa.concat_tables([existing, table])
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)


def _partition_path(out_root: Path, kind: str, symbol: str, dt: datetime) -> Path:
    return (
        out_root / "bronze" / "binance" / kind
        / f"symbol={symbol}" / f"date={dt.date().isoformat()}" / f"hour={dt.hour:02d}"
        / "data.parquet"
    )


class Recorder:
    """板スナップショットと約定を蓄積し、定期的に Parquet へフラッシュする。"""

    def __init__(self, symbol: str, out_root: Path, depth_levels: int):
        self.symbol = symbol
        self.out_root = out_root
        self.depth_levels = depth_levels
        self.book_rows: list[dict] = []
        self.trade_rows: list[dict] = []
        self.book_snapshots_total = 0
        self.trades_total = 0

    def record_book(self, book: LocalOrderBook) -> None:
        now = _utc_now()
        bp, bq, ap, aq = book.top_levels(self.depth_levels)
        self.book_rows.append({
            "ts": now, "symbol": self.symbol, "last_update_id": book.last_update_id,
            "bid_prices": bp, "bid_qtys": bq, "ask_prices": ap, "ask_qtys": aq,
            "ingested_at": now,
        })
        self.book_snapshots_total += 1

    def record_trade(self, event: dict) -> None:
        now = _utc_now()
        self.trade_rows.append({
            "ts": datetime.fromtimestamp(event["T"] / 1000, tz=timezone.utc),
            "symbol": self.symbol, "trade_id": event["t"],
            "price": event["p"], "qty": event["q"],
            "is_buyer_maker": event["m"], "ingested_at": now,
        })
        self.trades_total += 1

    def flush(self) -> None:
        now = _utc_now()
        if self.book_rows:
            table = _rows_to_table(self.book_rows, BOOK_SCHEMA)
            _write_parquet_append(table, _partition_path(self.out_root, "orderbook", self.symbol, now))
            self.book_rows = []
        if self.trade_rows:
            table = _rows_to_table(self.trade_rows, TRADE_SCHEMA)
            _write_parquet_append(table, _partition_path(self.out_root, "trades", self.symbol, now))
            self.trade_rows = []


async def record_symbol(
    symbol: str, duration_seconds: int, out_root: Path,
    rest_base: str, ws_base: str, depth_levels: int, run_id: str,
) -> dict:
    """1シンボル分の板と約定を duration_seconds 秒間記録する。"""
    started = _utc_now()
    recorder = Recorder(symbol, out_root, depth_levels)
    book = LocalOrderBook(symbol)
    resync_count = 0
    status, error = "ok", ""

    stream_name = f"{symbol.lower()}@depth@100ms/{symbol.lower()}@trade"
    url = f"{ws_base}/stream?streams={stream_name}"

    deadline = time.monotonic() + duration_seconds
    last_flush = time.monotonic()
    last_snapshot_write = 0.0

    async def resync() -> None:
        nonlocal resync_count
        snapshot = fetch_snapshot(symbol, 1000, rest_base)
        book.load_snapshot(snapshot)
        resync_count += 1
        print(f"[{symbol}] resync完了 lastUpdateId={book.last_update_id}", file=sys.stderr)

    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
            await resync()

            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=IDLE_TIMEOUT_SECONDS)
                except asyncio.TimeoutError:
                    print(f"[{symbol}] {IDLE_TIMEOUT_SECONDS}秒間データなし。生存確認のみ", file=sys.stderr)
                    continue

                msg = json.loads(raw)
                data = msg.get("data", msg)
                event_type = data.get("e")

                if event_type == "depthUpdate":
                    if not book.apply_event(data):
                        await resync()
                        continue
                    now = time.monotonic()
                    if now - last_snapshot_write >= SNAPSHOT_INTERVAL_SECONDS:
                        recorder.record_book(book)
                        last_snapshot_write = now
                elif event_type == "trade":
                    recorder.record_trade(data)

                if time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS:
                    recorder.flush()
                    last_flush = time.monotonic()

    except Exception as e:  # noqa: BLE001 — 記録済み分は残して終了する
        status, error = "failed", f"{type(e).__name__}: {e}"
        print(f"[{symbol}] エラーで終了: {error}", file=sys.stderr)

    recorder.flush()
    finished = _utc_now()

    return {
        "run_id": run_id, "symbol": symbol, "started_at": started, "finished_at": finished,
        "book_snapshots": recorder.book_snapshots_total, "trades_captured": recorder.trades_total,
        "resync_count": resync_count, "status": status, "error_message": error,
        "schema_version": SCHEMA_VERSION,
    }


def write_run_ledger(summaries: list[dict], out_root: Path) -> None:
    day = summaries[0]["started_at"].date().isoformat()
    run_id = summaries[0]["run_id"]
    table = _rows_to_table(summaries, RUN_SCHEMA)
    path = out_root / "ledger" / "orderbook_runs" / f"date={day}" / f"run_id={run_id}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)

    # 人間 / Actions のコミットステップが読める軽量サマリ(pyarrow不要)
    latest_path = out_root / "ledger" / "latest_run.json"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            **s,
            "started_at": s["started_at"].isoformat(),
            "finished_at": s["finished_at"].isoformat(),
        }
        for s in summaries
    ]
    latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


async def record_all(
    symbols: list[str], duration_seconds: int, out_root: Path,
    rest_base: str, ws_base: str, depth_levels: int, run_id: str,
) -> list[dict]:
    tasks = [
        record_symbol(s, duration_seconds, out_root, rest_base, ws_base, depth_levels, run_id)
        for s in symbols
    ]
    return await asyncio.gather(*tasks)


def main() -> int:
    p = argparse.ArgumentParser(description="Binance の板(L2 diff)と約定を記録し Bronze Parquet に書く")
    p.add_argument("--symbols", default="BTCUSDT", help="カンマ区切り(まずBTCのみで開始。ETH等は後から追加可)")
    p.add_argument("--duration-seconds", type=int, default=3000, help="記録する秒数(既定50分)")
    p.add_argument("--depth-levels", type=int, default=DEFAULT_DEPTH_LEVELS)
    p.add_argument("--out", default="./data")
    p.add_argument("--rest-base", default=os.environ.get("BINANCE_REST_BASE", DEFAULT_REST_BASE))
    p.add_argument("--ws-base", default=os.environ.get("BINANCE_WS_BASE", DEFAULT_WS_BASE))
    args = p.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    out_root = Path(args.out)
    run_id = os.environ.get("GITHUB_RUN_ID", "local") + "-" + _utc_now().strftime("%Y%m%dT%H%M%S")

    summaries = asyncio.run(record_all(
        symbols, args.duration_seconds, out_root, args.rest_base, args.ws_base,
        args.depth_levels, run_id,
    ))
    write_run_ledger(summaries, out_root)

    failed = [s for s in summaries if s["status"] == "failed"]
    for s in summaries:
        print(
            f"[{s['symbol']}] status={s['status']} "
            f"板{s['book_snapshots']}件 約定{s['trades_captured']}件 resync{s['resync_count']}回"
        )
    print(f"\n完了: {len(summaries)}シンボル (失敗{len(failed)})")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
