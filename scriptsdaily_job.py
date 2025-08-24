# scripts/daily_job.py
import os
from datetime import datetime, timedelta, timezone
import psycopg

DB_URL = os.environ["DATABASE_URL"]

DDL = """
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

UPSERT_SQL = """
insert into feat_menu_daily
  (day, store_id, menu_id, qty_sum, sales_sum, orders, avg_price)
select
  date(ts) as day,
  store_id,
  menu_id,
  sum(qty) as qty_sum,
  sum(qty*price) as sales_sum,
  count(*) as orders,
  avg(price)::numeric(10,2) as avg_price
from raw_sales
where ts >= %(start)s and ts < %(end)s
group by 1,2,3
on conflict (day, store_id, menu_id) do update
  set qty_sum = excluded.qty_sum,
      sales_sum = excluded.sales_sum,
      orders = excluded.orders,
      avg_price = excluded.avg_price;
"""

def main():
    # 昨日(UTC)の一日
    utc = timezone.utc
    today = datetime.now(utc).date()
    start = datetime.combine(today - timedelta(days=1), datetime.min.time(), tzinfo=utc)
    end   = start + timedelta(days=1)

    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute(UPSERT_SQL, {"start": start, "end": end})
    print(f"aggregated: {start} .. {end}")

if __name__ == "__main__":
    main()
