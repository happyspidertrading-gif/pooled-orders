from fastapi import FastAPI, Request
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

app = FastAPI()

# -----------------------------
# DATABASE CONNECTION SETTINGS
# -----------------------------
DB_NAME = "pooled_orders_db"
DB_USER = "postgres"
DB_PASSWORD = "POSTGRES"
DB_HOST = "localhost"
DB_PORT = "5432"

def get_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# -----------------------------
# ROOT ENDPOINT (TEST)
# -----------------------------
@app.get("/")
def home():
    return {"status": "FastAPI server is running"}

# -----------------------------
# WEBHOOK ENDPOINT FOR WIX
# -----------------------------
@app.post("/pooled-order")
async def pooled_order_webhook(request: Request):
    data = await request.json()

    # Extract fields from Wix webhook
    order_id = data.get("orderId")
    order_line_id = data.get("orderLineId")
    sku = data.get("sku")
    title = data.get("title")
    qty = data.get("quantity")
    unit_price = data.get("unitPrice")
    supplier_code = data.get("supplierCode")
    is_collect = data.get("isCollect", False)

    # Insert into PostgreSQL
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO pooled_orders (
            order_id, order_line_id, sku, title, qty, unit_price, supplier_code, is_collect
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        order_id,
        order_line_id,
        sku,
        title,
        qty,
        unit_price,
        supplier_code,
        is_collect
    ))

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "success", "received": data}
