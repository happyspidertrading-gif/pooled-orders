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
    orderId: str = Form(None),
    createdAt: str = Form(None),

    customerName_first: str = Form(None),
    customerName_last: str = Form(None),

    email: str = Form(None),
    total: str = Form(None),

    item_name: str = Form(None),
    item_sku: str = Form(None),
    item_quantity: str = Form(None),
    item_price: str = Form(None),

    payment_status: str = Form(None),
    fulfillment_status: str = Form(None),
    sales_channel: str = Form(None),
    created_by: str = Form(None)
):

    payment_status = (payment_status or "").upper()
    fulfillment_status = (fulfillment_status or "").upper()
    sales_channel = (sales_channel or "").lower()
    created_by = (created_by or "").upper()

    customerName = f"{customerName_first or ''} {customerName_last or ''}".strip()

    try:
        total_value = float(total) if total else 0.0
    except:
        total_value = 0.0

    parsed_items = []
    if item_name or item_sku or item_quantity or item_price:
        parsed_items.append({
            "name": item_name,
            "sku": item_sku,
            "quantity": item_quantity,
            "price": item_price
        })

    # ---------------------------
    # FINAL COLLECTION LOGIC
    # ---------------------------
    # Treat ALL orders as collection unless explicitly delivery
    delivery_statuses = ["DELIVERED", "SHIPPED", "OUT_FOR_DELIVERY", "IN_TRANSIT"]

    is_delivery = fulfillment_status in delivery_statuses

    if is_delivery:
        return {"status": "ignored", "reason": "Delivery order"}

    # Everything else = collection
    # ---------------------------

    pool = load_pool()

    order = {
        "orderId": orderId,
        "createdAt": createdAt,
        "customerName": customerName,
        "email": email,
        "total": total_value,
        "items": parsed_items,
        "payment_status": payment_status,
        "fulfillment_status": fulfillment_status,
        "sales_channel": sales_channel,
        "created_by": created_by
    }

    pool.append(order)
    save_pool(pool)

    running_total = sum(float(o.get("total", 0) or 0) for o in pool)
    if running_total >= THRESHOLD:
        send_to_vow(pool)
        save_pool([])
        return {"status": "sent", "reason": "Threshold £100 reached"}

    try:
        first_created = pool[0].get("createdAt")
        first_order_time = datetime.fromisoformat(first_created.replace("Z", "+00:00")) if first_created else datetime.utcnow()
    except Exception:
        first_order_time = datetime.utcnow()

    now = datetime.utcnow()

    if business_days_between(first_order_time, now) >= 2:
        send_to_vow(pool)
        save_pool([])
        return {"status": "sent", "reason": "2 business days passed"}

    return {
        "status": "pooled",
        "reason": "Waiting for threshold or 2 business days",
        "current_total": running_total
    }