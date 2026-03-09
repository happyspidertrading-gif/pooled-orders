import os
import json
import requests
from fastapi import FastAPI, Form
from datetime import datetime, timedelta

app = FastAPI()

POOL_FILE = "pooled_orders.json"
THRESHOLD = 100  # £100 threshold
WIX_API_KEY = os.getenv("WIX_API_KEY")
WIX_SITE_ID = "6679c4f0-a411-4f78-a18a-d05d939ebd76"

# ----------------------------------------------------
# Helpers
# ----------------------------------------------------
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

# ----------------------------------------------------
# Fetch full order details from Wix
# ----------------------------------------------------
def fetch_wix_order(order_id: str):
    url = f"https://www.wixapis.com/stores/v1/orders/{order_id}"
    headers = {
        "Authorization": WIX_API_KEY,
        "wix-site-id": WIX_SITE_ID,
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Wix API error:", response.text)
        return None

    return response.json().get("order")

# ----------------------------------------------------
# Debug endpoint
# ----------------------------------------------------
@app.get("/debug-pool")
def debug_pool():
    return load_pool()

# ----------------------------------------------------
# Main webhook endpoint
# ----------------------------------------------------
@app.post("/pooled-order")
async def pooled_order(orderId: str = Form(None)):
    if not orderId:
        return {"status": "error", "reason": "No orderId received"}

    # Fetch full order details from Wix
    order = fetch_wix_order(orderId)
    if not order:
        return {"status": "error", "reason": "Failed to fetch order from Wix"}

    # Extract fields from Wix order object
    customer_name = order.get("buyerInfo", {}).get("fullName", "")
    email = order.get("buyerInfo", {}).get("email", "")
    total_value = float(order.get("priceSummary", {}).get("total", {}).get("amount", 0))

    items = []
    for line in order.get("lineItems", []):
        items.append({
            "name": line.get("name"),
            "sku": line.get("sku"),
            "quantity": line.get("quantity"),
            "price": line.get("priceData", {}).get("price", {}).get("amount")
        })

    fulfillment_status = order.get("fulfillmentStatus", "").upper()
    sales_channel = order.get("channelInfo", {}).get("type", "").lower()
    created_at = order.get("createdDate")

    # ----------------------------------------------------
    # FINAL COLLECTION LOGIC
    # Treat ALL orders as collection unless explicitly delivery
    # ----------------------------------------------------
    delivery_statuses = ["DELIVERED", "SHIPPED", "OUT_FOR_DELIVERY", "IN_TRANSIT"]
    is_delivery = fulfillment_status in delivery_statuses

    if is_delivery:
        return {"status": "ignored", "reason": "Delivery order"}

    # Everything else = collection
    pool = load_pool()

    pooled_entry = {
        "orderId": orderId,
        "createdAt": created_at,
        "customerName": customer_name,
        "email": email,
        "total": total_value,
        "items": items,
        "payment_status": order.get("paymentStatus"),
        "fulfillment_status": fulfillment_status,
        "sales_channel": sales_channel
    }

    pool.append(pooled_entry)
    save_pool(pool)

    # Check threshold
    running_total = sum(float(o.get("total", 0)) for o in pool)
    if running_total >= THRESHOLD:
        send_to_vow(pool)
        save_pool([])
        return {"status": "sent", "reason": "Threshold £100 reached"}

    # Check 2 business days
    try:
        first_created = pool[0].get("createdAt")
        first_order_time = datetime.fromisoformat(first_created.replace("Z", "+00:00"))
    except:
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