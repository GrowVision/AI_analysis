# scripts/daily_job.py
"""
日次集計ジョブ（Render Cronから実行想定）

機能:
  - 昨日1日分を集計（デフォルト。Cron運用）
  - 任意期間を再集計 (--from YYYY-MM-DD --to YYYY-MM-DD)
  - 全期間を作り直し (--mode all)
  - 店舗を絞って再集計 (--store-id)

仕様:
  - 入力テーブル: raw_sales
      (store_id int, ts timestamptz, menu_id text, qty int, price int, ...)
  - 出力テーブル: feat_menu_daily
      主キー (day, store_id, menu_id)
  - 何度実行しても ON CONFLICT でUPSERT（冪等）
  - タイムゾーンは既定 UTC。AGG_TZ="Asia/Tokyo" など環境変数で変更可
  - ログは print() に出力（RenderのLogsで確認）
"""

import os
import sys
import argparse
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

import psycopg

# ==== 設定 ============================================================
DB_URL = os.environ["DATABASE_URL"]

# 集計用タイムゾーン（例: "UTC", "Asia/Tokyo"）
AGG_TZ = os.environ.get("AGG_TZ", "UTC")

# 一括再集計時の過去を見る最大年数（安全装置）
MAX_YEARS_BACK = int(os.environ.get("MAX_YEARS_BACK", "5"))

# ==== DDL & インデックス =============================================
DDL_RAW_SALES_INDEXES = """
-- 集計速度のための推奨インデックス（存在しなければ作成）
create index if not exists idx_raw_sales_store_ts on raw_sales(store_id, ts);
create index if not exists idx_raw_sales_ts on raw_sales(ts);
"""

DDL_FEAT_MENU_DAILY = """
create table if not exists feat_menu_daily(
  day date not null,
  store_id int not null,
  menu_id text not null,
  qty_sum int not null,
  sales_sum bigint not null,
  orders int not null,
  avg_price numeric(10,2),
  primary key (day, store_id, menu_id)
);
"""

UPSERT_RANGE = """
insert into feat_menu_daily
  (day, store_id, menu_id, qty_sum, sales_sum, orders, avg_price)
select
  (ts at time zone 'UTC' at time zone %(tz)s)::date as day_local,
  store_id,
  menu_id,
  sum(qty) as qty_sum,
  sum(qty*price) as sales_sum,
  count(*) as orders,
  avg(price)::numeric(10,2) as avg_price
from raw_sales
where ts >= %(start_utc)s and ts < %(end_utc)s
  and (%(store_id)s is null or store_id=%(store_id)s)
group by 1,2,3
on conflict (day, store_id, menu_id) do update
  set qty_sum   = excluded.qty_sum,
      sales_sum = excluded.sales_sum,
      orders    = excluded.orders,
      avg_price = excluded.avg_price;
"""

# ==== ユーティリティ ===================================================
def log(msg: str):
    print(f"[daily_job] {msg}", flush=True)

def ensure_schema_and_indexes(conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute(DDL_FEAT_MENU_DAILY)
        cur.execute(DDL_RAW_SALES_INDEXES)

def to_utc_range_for_local_day(day_local: date, tz: ZoneInfo) -> tuple[datetime, datetime]:
    """
    ローカル日付(day_local)の00:00〜24:00(次日00:00)を、UTCの区間に変換して返す。
    """
    start_local = datetime.combine(day_local, datetime.min.time()).replace(tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    # tz→UTCへ変換
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    return start_utc, end_utc

def aggregate_range(conn: psycopg.Connection, start_utc: datetime, end_utc: datetime,
                    store_id: int | None, tz_name: str):
    with conn.cursor() as cur:
        cur.execute(
            UPSERT_RANGE,
            {"start_utc": start_utc, "end_utc": end_utc,
             "store_id": store_id, "tz": tz_name}
        )

# ==== メイン処理 ======================================================
def run_yesterday(store_id: int | None):
    tz = ZoneInfo(AGG_TZ)
    today_local = datetime.now(tz).date()
    target_day = today_local - timedelta(days=1)
    s_utc, e_utc = to_utc_range_for_local_day(target_day, tz)

    log(f"aggregate yesterday (local={AGG_TZ}) day={target_day} "
        f"range_utc=[{s_utc.isoformat()} .. {e_utc.isoformat()}) "
        f"store_id={store_id}")

    with psycopg.connect(DB_URL, autocommit=True) as conn:
        ensure_schema_and_indexes(conn)
        aggregate_range(conn, s_utc, e_utc, store_id, AGG_TZ)

def run_all(store_id: int | None):
    tz = ZoneInfo(AGG_TZ)
    # 安全装置: MAX_YEARS_BACK 年より古いものは拾わない
    min_day = (datetime.now(tz) - timedelta(days=365 * MAX_YEARS_BACK)).date()

    log(f"rebuild ALL (last {MAX_YEARS_BACK} years, local={AGG_TZ}) store_id={store_id}")

    with psycopg.connect(DB_URL, autocommit=True) as conn:
        ensure_schema_and_indexes(conn)
        with conn.cursor() as cur:
            # 最古・最新のUTC時刻を raw_sales から拾う（ある程度の上限つき）
            cur.execute("""
                select min(ts), max(ts) from raw_sales
                where (%s is null or store_id=%s)
            """, (store_id, store_id))
            row = cur.fetchone()
            if not row or not row[0] or not row[1]:
                log("raw_sales has no data. nothing to do.")
                return
            min_ts_utc, max_ts_utc = row

        # ローカル日付レンジへ（丸め）
        first_local_day = min_ts_utc.astimezone(tz).date()
        last_local_day  = max_ts_utc.astimezone(tz).date()
        if first_local_day < min_day:
            first_local_day = min_day

        # 1日ずつ処理（必要に応じて週単位/バルクに最適化可）
        cur_day = first_local_day
        count = 0
        with psycopg.connect(DB_URL, autocommit=True) as conn2:
            ensure_schema_and_indexes(conn2)
            while cur_day <= last_local_day:
                s_utc, e_utc = to_utc_range_for_local_day(cur_day, tz)
                aggregate_range(conn2, s_utc, e_utc, store_id, AGG_TZ)
                if count % 20 == 0:
                    log(f" aggregated up to {cur_day} ...")
                cur_day += timedelta(days=1)
                count += 1
        log(f"done. days={count}, range={first_local_day}..{last_local_day}")

def run_range(date_from: str, date_to: str | None, store_id: int | None):
    """
    date_from (含む) 〜 date_to(含まない) をローカルTZで集計。
    date_to 未指定なら date_from の1日分。
    """
    tz = ZoneInfo(AGG_TZ)
    try:
        df = datetime.strptime(date_from, "%Y-%m-%d").date()
    except ValueError:
        raise SystemExit("ERROR: --from は YYYY-MM-DD 形式で指定してください。")
    if date_to:
        try:
            dt = datetime.strptime(date_to, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit("ERROR: --to は YYYY-MM-DD 形式で指定してください。")
    else:
        dt = df + timedelta(days=1)

    log(f"aggregate range (local={AGG_TZ}) from={df} to={dt} store_id={store_id}")

    with psycopg.connect(DB_URL, autocommit=True) as conn:
        ensure_schema_and_indexes(conn)
        # 1日単位でループ（境界にサマータイム等があっても安全）
        cur_day = df
        count = 0
        while cur_day < dt:
            s_utc, e_utc = to_utc_range_for_local_day(cur_day, tz)
            aggregate_range(conn, s_utc, e_utc, store_id, AGG_TZ)
            cur_day += timedelta(days=1)
            count += 1
        log(f"done. days={count}")

def main(argv=None):
    parser = argparse.ArgumentParser(description="Daily aggregation job")
    parser.add_argument("--mode", choices=["yesterday", "range", "all"], default="yesterday",
                        help="yesterday: 昨日1日 / range: 任意期間 / all: 全期間")
    parser.add_argument("--from", dest="date_from", help="YYYY-MM-DD（mode=range必須）")
    parser.add_argument("--to", dest="date_to", help="YYYY-MM-DD（含まない上限。省略で1日分）")
    parser.add_argument("--store-id", type=int, default=None, help="特定店舗だけ集計する場合に指定")
    args = parser.parse_args(argv)

    log(f"start mode={args.mode}, tz={AGG_TZ}, store_id={args.store_id}")

    if args.mode == "yesterday":
        run_yesterday(args.store_id)
    elif args.mode == "range":
        if not args.date_from:
            raise SystemExit("ERROR: mode=range では --from YYYY-MM-DD が必須です。")
        run_range(args.date_from, args.date_to, args.store_id)
    elif args.mode == "all":
        run_all(args.store_id)

    log("finished")

if __name__ == "__main__":
    main()
