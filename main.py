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
    customerName: str = Form(None),
    email: str = Form(None),
    total: str = Form(None),
    items: str = Form(None),
    fulfillment_method: str = Form(None),
    payment_status: str = Form(None),
    created_by: str = Form(None)
):

    # ---------------------------
    # Normalise missing fields
    # ---------------------------

    # Wix Automations often sends None for these
    payment_status = payment_status or "PAID"  # assume paid unless told otherwise
    fulfillment_method = fulfillment_method or "PICKUP"  # assume pickup
    created_by = created_by or "USER"

    # Convert total safely
    try:
        total_value = float(total) if total else 0.0
    except:
        total_value = 0.0

    # Parse items JSON if present
    try:
        parsed_items = json.loads(items) if items else []
    except:
        parsed_items = []

    # Allow admins to place unpaid orders
    admin_override = created_by.upper() == "ADMIN"

    # Customers must pay online
    if not admin_override and payment_status.upper() != "PAID":
        return {"status": "ignored", "reason": "Customer order not paid"}

    # Only process in-store collection
    if fulfillment_method.upper() != "PICKUP":
        return {"status": "ignored", "reason": "Not in-store collection"}

    # Load pool
    pool = load_pool()

    # Add new order
    order = {
        "orderId": orderId,
        "createdAt": createdAt,
        "customerName": customerName,
        "email": email,
        "total": total_value,
        "items": parsed_items,
        "payment_status": payment_status,
        "created_by": created_by
    }

    pool.append(order)
    save_pool(pool)

    # Check £100 threshold
    running_total = sum(float(o["total"]) for o in pool)
    if running_total >= THRESHOLD:
        send_to_vow(pool)
        save_pool([])
        return {"status": "sent", "reason": "Threshold £100 reached"}

    # Check 2 business days
    try:
        first_order_time = datetime.fromisoformat(pool[0]["createdAt"].replace("Z", "+00:00"))
    except:
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