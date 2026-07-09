from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncpg

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/metrics/{symbol}")
async def get_metrics(symbol: str):
    conn = await asyncpg.connect(
        user='stockuser', password='stockpass',
        database='stockdb', host='localhost', port=5432
    )
    rows = await conn.fetch(
        "SELECT * FROM stock_metrics WHERE symbol=$1 ORDER BY window_start DESC LIMIT 20",
        symbol
    )
    await conn.close()
    return [dict(r) for r in rows]