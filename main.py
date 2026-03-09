from fastapi import FastAPI, Form
from datetime import datetime, timedelta
import json
import os

app = FastAPI()

POOL_FILE = "pooled_orders.json"
THRESHOLD = 100  # £100 threshold


# ---------------------------
# Helpers
# ---------------------------
def load_pool():
    if not os.path.exists(POOL_FILE):
        return []
    with open(POOL_FILE, "r") as f:
        return json.load(f)


def save_pool(pool):
    with open(POOL_FILE, "w") as f:
        json.dump(pool, f, indent=2)


def business_days_between(start: datetime, end: datetime):
    days = 0
    current = start
    while current.date() < end.date():
        if current.weekday() < 5:
            days += 1
        current += timedelta(days=1)
    return days


def send_to_vow(pooled_orders):
    # TODO: Replace with real VOW integration
    print("Sending pooled order to VOW...")
    print(json.dumps(pooled_orders, indent=2))


@app.get("/debug-pool")
def debug_pool():
    return load_pool()


# ---------------------------
# Main Webhook Endpoint
# ---------------------------
@app.post("/pooled-order")
async def pooled_order(
    orderId: str = Form(None),               # Order number
    createdAt: str = Form(None),             # Date created
    customerName: str = Form(None),          # Billing info → full name
    email: str = Form(None),                 # Customer email
    total: str = Form(None),                 # Order total summary → total
    items: str = Form(None),                 # Ordered items (JSON string)
    payment_status: str = Form(None),        # Payment status
    fulfillment_status: str = Form(None),    # Fulfillment status
    sales_channel: str = Form(None),         # Sales channel type
    created_by: str = Form(None)             # Status or USER
):

    # ---------------------------
    # Normalise missing fields
    # ---------------------------
    payment_status = payment_status or "PAID"
    fulfillment_status = fulfillment_status or "PICKUP"
    created_by = created_by or "USER"

    # Convert total safely
