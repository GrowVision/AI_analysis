# main.py
import os
from fastapi import FastAPI
import psycopg

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/db-ping")
def db_ping():
    url = os.environ["DATABASE_URL"]
    # Renderの内部URLでも外部URLでもそのまま使える
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("select 1")
            one = cur.fetchone()[0]
    return {"db": "ok", "select1": one}
