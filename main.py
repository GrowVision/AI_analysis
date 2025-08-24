# main.py
import os, io, csv
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
import psycopg
import boto3
from botocore.config import Config

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
def s3_client():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],  # 例: https://<accountid>.r2.cloudflarestorage.com
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
    )
    return s3, os.environ["S3_BUCKET"]
  
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

# --- 末尾あたりに追加 ---
@app.post("/ingest/menu-photo")
async def ingest_menu_photo(
    store_id: int = Query(...),
    menu_id: str = Query(...),
    file: UploadFile = File(...)
):
    s3, bucket = s3_client()
    key = f"menu/{store_id}/{menu_id}.jpg"
    body = await file.read()
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=file.content_type or "image/jpeg")
    # 画像URLは後で公開設定に応じて決める。ここでは key を返す
    img_url = f"s3://{bucket}/{key}"

    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("""
              insert into menu_master (store_id, menu_id, img_url)
              values (%s,%s,%s)
              on conflict (store_id, menu_id) do update set img_url = excluded.img_url
            """, (store_id, menu_id, img_url))
    return {"status":"ok","key":key}
2-3) テスト
/docs → POST /ingest/menu-photo → store_id=1, menu_id=ramen → 画像ファイルを選んで Execute

成功すれば {"status":"ok","key":"menu/1/ramen.jpg"}

ステップ3｜（雛形）Geminiでレポ生成の枠だけ作る
まだ GEMINI_API_KEY がダミーでも大丈夫。キーが無い場合はサンプルの疑似レポを返すようにしておきます。

3-1) requirements.txt に追記
diff
コピーする
編集する
 boto3==1.34.162
+google-genai==0.5.0
3-2) main.py にレポ生成APIを追加
python
コピーする
編集する
# --- 先頭付近に追記 ---
def getenv(name: str) -> str | None:
    v = os.environ.get(name)
    return v if (v and v != "PENDING") else None

# --- 末尾に追加 ---
@app.post("/report/generate")
def report_generate(store_id: int = Query(...)):
    # 直近7日分の売上トップ5を箇条書きで作成
    sql = """
      select menu_id, sum(qty) as qty, sum(qty*price) as sales
      from raw_sales where store_id=%s and ts >= now() - interval '7 days'
      group by menu_id order by sales desc limit 5
    """
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (store_id,))
            rows = cur.fetchall()

    summary = "\n".join([f"- {m}: 売上{int(s)} / 注文件数{int(q)}" for m,q,s in rows])

    # キーが未設定ならダミーを返す
    api_key = getenv("GEMINI_API_KEY")
    if not api_key:
        body = f"【ダミー】直近7日サマリー\n{summary}\n\n本番キーを設定すると文章化します。"
        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "insert into ai_reports(store_id, kind, body_md) values (%s,%s,%s)",
                    (store_id, "weekly", body),
                )
        return {"status":"ok","preview": body, "used":"dummy"}

    # 本番：Geminiで文章化
    from google import genai
    client = genai.Client(api_key=api_key)
    prompt = f"""
あなたは飲食店のデータアナリストです。直近7日のサマリーを元に、
・売れ筋メニュー/価格帯
・客層の仮説（可能ならCSV内のage_band/genderを言及）
・次週の施策（バンドル/写真改善/投稿時間）
を箇条書きで日本語300-500字で提案してください。

サマリー:
{summary}
"""
    res = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
    text = res.text or "(no text)"

    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "insert into ai_reports(store_id, kind, body_md) values (%s,%s,%s)",
                (store_id, "weekly", text),
            )
    return {"status":"ok","used":"gemini","preview": text[:400]}
