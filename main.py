# main.py
import os, io, csv
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
import psycopg

app = FastAPI(title="restaurant-ai")

DB_URL = os.environ["DATABASE_URL"]

DDL = """
create table if not exists raw_sales(
  id bigserial primary key,
  store_id int not null,
  ts timestamptz not null,
  menu_id text not null,
  qty int not null,
  price int not null,
  age_band text, gender text, party_size int, channel text
);
create table if not exists menu_master(
  store_id int not null,
  menu_id text not null,
  name text, category text,
  price int, cost int,
  img_url text,
  primary key (store_id, menu_id)
);
create table if not exists ai_reports(
  id bigserial primary key,
  store_id int,
  kind text,
  period tsrange,
  body_md text,
  created_at timestamptz default now()
);
"""

def ensure_schema():
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)

@app.on_event("startup")
def on_startup():
    ensure_schema()

@app.get("/", response_class=HTMLResponse)
def index():
    return """
    <h1>restaurant-ai</h1>
    <ul>
      <li><a href="/health">/health</a></li>
      <li><a href="/db-ping">/db-ping</a></li>
      <li><a href="/docs">/docs</a></li>
    </ul>
    """

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/db-ping")
def db_ping():
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("select 1")
            one = cur.fetchone()[0]
    return {"db": "ok", "select1": one}

def parse_ts(s: str) -> datetime:
    """よくあるフォーマットだけ素直に対応（必要なら拡張）"""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except Exception:
            pass
    raise ValueError(f"Unsupported datetime format: {s}")

@app.post("/ingest/sales")
async def ingest_sales(
    store_id: int = Query(..., description="店舗ID（例: 1）"),
    file: UploadFile = File(..., description="売上CSV")
):
    """
    取り込み想定ヘッダ:
    timestamp,menu_id,qty,price,age_band,gender,party_size,channel
    例:
    2025-08-20 18:30:00,ramen,2,1200,30-39,M,2,instagram
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(400, "CSVファイルをアップロードしてください。")

    data = await file.read()
    text = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    required = {"timestamp","menu_id","qty","price"}
    if not required.issubset({h.strip() for h in reader.fieldnames or []}):
        raise HTTPException(400, f"CSVヘッダに {required} が必要です。実際: {reader.fieldnames}")

    inserted = 0
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            for row in reader:
                try:
                    ts = parse_ts(row["timestamp"])
                    menu_id = row["menu_id"].strip()
                    qty = int(row["qty"])
                    price = int(row["price"])
                    age_band = (row.get("age_band") or "").strip() or None
                    gender   = (row.get("gender") or "").strip() or None
                    party_sz = row.get("party_size")
                    party_sz = int(party_sz) if party_sz not in (None,"") else None
                    channel  = (row.get("channel") or "").strip() or None

                    cur.execute(
                        """
                        insert into raw_sales
                          (store_id, ts, menu_id, qty, price, age_band, gender, party_size, channel)
                        values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (store_id, ts, menu_id, qty, price, age_band, gender, party_sz, channel)
                    )
                    inserted += 1
                except Exception as e:
                    # 1行おきに失敗しても全体は続行
                    print("ROW ERROR:", e, row)
    return JSONResponse({"status": "ok", "inserted": inserted})

# --- 既存 main.py の末尾などに追記 ---
from typing import Optional
from fastapi import Query

@app.get("/analytics/menu-daily")
def menu_daily(
    store_id: Optional[int] = Query(None),
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="YYYY-MM-DD（含まれない上限）")
):
    cond = []
    params = {}
    if store_id is not None:
        cond.append("store_id = %(store_id)s")
        params["store_id"] = store_id
    if date_from:
        cond.append("day >= %(from)s")
        params["from"] = date_from
    if date_to:
        cond.append("day < %(to)s")
        params["to"] = date_to
    where = (" where " + " and ".join(cond)) if cond else ""
    sql = f"""
      select day, store_id, menu_id, qty_sum, sales_sum, orders, avg_price
      from feat_menu_daily
      {where}
      order by day desc, sales_sum desc
      limit 500
    """
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return {"rows": rows}

