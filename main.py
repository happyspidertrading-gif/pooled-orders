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
    orderId: str = Form(None),
    createdAt: str = Form(None),

    # POS name fields
    customerName_first: str = Form(None),
    customerName_last: str = Form(None),

    email: str = Form(None),
    total: str = Form(None),

    # POS item fields (single item only)
    item_name: str = Form(None),
    item_sku: str = Form(None),
    item_quantity: str = Form(None),
    item_price: str = Form(None),

    payment_status: str = Form(None),
    fulfillment_status: str = Form(None),
    sales_channel: str = Form(None),
    created_by: str = Form(None)
):

    # ---------------------------
    # Normalise basic fields
    # ---------------------------
    payment_status = (payment_status or "").upper()
    fulfillment_status = (fulfillment_status or "").upper()
    sales_channel = (sales_channel or "").lower()
    created_by = (created_by or "").upper()

    # Combine first + last name
    customerName = f"{customerName_first or ''} {customerName_last or ''}".strip()

    # Convert total safely
    try:
        total_value = float(total) if total else 0.0
    except:
        total_value = 0.0

    # Build POS item list (single item)
    parsed_items = []
    if item_name or item_sku or item_quantity or item_price:
        parsed_items.append({
            "name": item_name,
            "sku": item_sku,
            "quantity": item_quantity,
            "price": item_price
        })

    # ---------------------------
    # Collection rules
    # ---------------------------
    # POS orders are ALWAYS collection orders.
    is_pos = sales_channel in ["point of sale", "pos"]

    # Online orders must explicitly be collection
    is_online_collection = fulfillment_status in ["PICKUP", "READY_FOR_PICKUP"]

    # Final rule:
    # - POS → always pooled
    # - Online pickup → pooled
    # - Delivery → ignored
    is_collection = is_pos or is_online_collection

    if not is_collection:
        return {"status": "ignored", "reason": "Not a collection order"}

    # ---------------------------
    # Pooling
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

    # Check £100 threshold
    running_total = sum(float(o.get("total", 0) or 0) for o in pool)
    if running_total >= THRESHOLD:
        send_to_vow(pool)
        save_pool([])
        return {"status": "sent", "reason": "Threshold £100 reached"}

    # Check 2 business days
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

    # Otherwise keep pooling
    return {
        "status": "pooled",
        "reason": "Waiting for threshold or 2 business days",
        "current_total": running_total
    }