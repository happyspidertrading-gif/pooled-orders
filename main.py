# main.py
import asyncio
import hashlib
import json
import logging
import time
from typing import Dict, Any, Tuple, Optional

from datetime import datetime, timedelta
import pytz

import httpx
from fastapi import FastAPI, Request
from xml.etree.ElementTree import Element, SubElement, tostring

# -------------------------
# Configuration
# -------------------------
VOW_URL = "https://vow.example.com/api/orders"  # replace with real VOW endpoint
VOW_API_KEY = "REPLACE_WITH_REAL_KEY"           # replace with real key or token
SEND_AS_XML = False      # True if VOW expects XML, False for JSON
BATCH_SEND = True        # True to send all pooled orders in one request
MAX_RETRIES = 4
INITIAL_BACKOFF = 1.0    # seconds
MAX_BACKOFF = 16.0
REQUEST_TIMEOUT = 15.0   # seconds
POUND_THRESHOLD = 100.00
TIMEOUT_BUSINESS_DAYS = 2
UK_TZ = pytz.timezone("Europe/London")

# -------------------------
# Logging
# -------------------------
logger = logging.getLogger("pooled_orders")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# -------------------------
# App and in-memory pool
# -------------------------
app = FastAPI(title="Pooled Orders Service")
order_pool: Dict[str, Dict[str, Any]] = {}
ignored_orders_log: Dict[str, Dict[str, Any]] = {}  # store ignored orders for audit/detection

# -------------------------
# Utility functions
# -------------------------
def parse_iso_to_aware(iso_str: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO8601 string to timezone-aware datetime in UK timezone.
    Accepts strings ending with 'Z' or with offset.
    """
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except Exception:
        try:
            dt = datetime.strptime(iso_str.split(".")[0], "%Y-%m-%dT%H:%M:%S")
            dt = dt.replace(tzinfo=pytz.UTC)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt.astimezone(UK_TZ)


def business_days_between(start: datetime, end: datetime) -> int:
    """
    Count business days (Mon-Fri) strictly between start and end datetimes.
    """
    if start is None or end is None:
        return 0
    s = start.date()
    e = end.date()
    if s >= e:
        return 0
    days = 0
    current = s
    while current < e:
        current += timedelta(days=1)
        if current.weekday() < 5:
            days += 1
    return days


def is_business_timeout(created_iso: Optional[str]) -> bool:
    """
    Returns True if the order created at created_iso is older than TIMEOUT_BUSINESS_DAYS.
    """
    created_dt = parse_iso_to_aware(created_iso)
    if created_dt is None:
        return False
    now = datetime.now(UK_TZ)
    days = business_days_between(created_dt, now)
    return days >= TIMEOUT_BUSINESS_DAYS


def pool_total() -> float:
    """
    Sum the pooled orders' totals (assumes totals.total is numeric).
    """
    total = 0.0
    for o in order_pool.values():
        try:
            total += float(o.get("totals", {}).get("total", 0) or 0)
        except Exception:
            continue
    return total


def make_idempotency_key(orders: Dict[str, Any]) -> str:
    """
    Deterministic idempotency key for a batch of orders.
    """
    ids = sorted(orders.keys())
    key_source = "|".join(ids)
    return hashlib.sha256(key_source.encode("utf-8")).hexdigest()


# -------------------------
# Delivery detection
# -------------------------
def _safe_get(d: dict, *keys):
    """
    Helper to walk nested dicts safely.
    """
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
        if cur is None:
            return None
    return cur


def is_delivery_order(order: dict) -> bool:
    """
    Heuristic checks for delivery orders. Returns True if order appears to be DELIVERY.
    This function checks multiple common fields used by Wix payloads:
      - fulfillment, shipping, shippingInfo, deliveryInfo
      - channelInfo.type or channel
      - presence of a shipping address (and absence of pickup flags)
      - shipping method names containing delivery/shipping/courier
    Adjust keywords or field names to match your actual payloads if needed.
    """
    if not isinstance(order, dict):
        return False

    # Common containers
    fulfillment = order.get("fulfillment") or order.get("shipping") or order.get("shippingInfo") or order.get("deliveryInfo") or {}
    channel = _safe_get(order, "channelInfo", "type") or order.get("channel") or ""
    shipping_method = fulfillment.get("method") or fulfillment.get("type") or fulfillment.get("shippingMethod") or ""
    shipping_label = _safe_get(order, "shippingInfo", "method") or _safe_get(order, "deliveryInfo", "method") or ""

    # Normalize and check keywords
    candidates = [channel, shipping_method, shipping_label]
    for v in candidates:
        if not v:
            continue
        vs = str(v).lower()
        if any(keyword in vs for keyword in ("delivery", "ship", "shipping", "home delivery", "courier", "carrier")):
            return True

    # If a shipping address exists and there is no explicit pickup/collection flag, treat as delivery
    shipping_address = order.get("shippingAddress") or _safe_get(order, "buyerInfo", "shippingAddress") or _safe_get(order, "shippingInfo", "address")
    pickup_flag = False
    # Check common pickup indicators
    pickup_candidates = [
        fulfillment.get("type"),
        fulfillment.get("method"),
        _safe_get(order, "fulfillment", "method"),
        _safe_get(order, "shippingInfo", "fulfillmentType")
    ]
    for p in pickup_candidates:
        if not p:
            continue
        ps = str(p).lower()
        if any(k in ps for k in ("collection", "pickup", "click_and_collect", "in_store_pickup")):
            pickup_flag = True
            break

    if shipping_address and not pickup_flag:
        return True

    return False


# -------------------------
# Payload formatting helpers
# -------------------------
def orders_to_xml(orders: Dict[str, Any]) -> str:
    root = Element("Orders")
    for oid, o in orders.items():
        order_el = SubElement(root, "Order")
        SubElement(order_el, "OrderId").text = str(o.get("order_id") or "")
        SubElement(order_el, "Number").text = str(o.get("number") or "")
        SubElement(order_el, "Created").text = str(o.get("created") or "")
        SubElement(order_el, "Channel").text = str(o.get("channel") or "")
        SubElement(order_el, "CustomerName").text = str(o.get("customer_name") or "")
        SubElement(order_el, "CustomerEmail").text = str(o.get("customer_email") or "")
        totals = SubElement(order_el, "Totals")
        SubElement(totals, "Total").text = str(o.get("totals", {}).get("total", 0))
        items_el = SubElement(order_el, "LineItems")
        for item in o.get("items", []):
            item_el = SubElement(items_el, "Item")
            SubElement(item_el, "Name").text = str(item.get("name") or "")
            SubElement(item_el, "SKU").text = str(item.get("sku") or "")
            SubElement(item_el, "Quantity").text = str(item.get("quantity") or 0)
            SubElement(item_el, "Price").text = str(item.get("price") or 0)
    return tostring(root, encoding="utf-8", method="xml").decode("utf-8")


def orders_to_json(orders: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"orders": []}
    for oid, o in orders.items():
        payload["orders"].append({
            "orderId": o.get("order_id"),
            "number": o.get("number"),
            "created": o.get("created"),
            "channel": o.get("channel"),
            "customer": {
                "name": o.get("customer_name"),
                "email": o.get("customer_email")
            },
            "totals": o.get("totals", {}),
            "items": o.get("items", []),
        })
    return payload


# -------------------------
# HTTP helper with retries
# -------------------------
async def _post_with_retries(client: httpx.AsyncClient, url: str, headers: dict, data, is_json: bool) -> Tuple[bool, dict]:
    attempt = 0
    backoff = INITIAL_BACKOFF
    while attempt <= MAX_RETRIES:
        try:
            if is_json:
                resp = await client.post(url, json=data, headers=headers, timeout=REQUEST_TIMEOUT)
            else:
                resp = await client.post(url, content=data, headers=headers, timeout=REQUEST_TIMEOUT)

            if 200 <= resp.status_code < 300:
                try:
                    return True, resp.json()
                except Exception:
                    return True, {"status_code": resp.status_code, "text": resp.text}

            if 400 <= resp.status_code < 500 and resp.status_code != 429:
                logger.error("VOW client error %s: %s", resp.status_code, resp.text)
                return False, {"status_code": resp.status_code, "text": resp.text}

            logger.warning("VOW transient error %s: %s. Retrying...", resp.status_code, resp.text)
        except (httpx.RequestError, httpx.TimeoutException) as exc:
            logger.warning("Request error: %s. Retrying...", str(exc))

        jitter = backoff * 0.1 * (0.5 - (time.time() % 1))
        sleep_for = min(MAX_BACKOFF, backoff) + jitter
        await asyncio.sleep(sleep_for)
        backoff *= 2
        attempt += 1

    return False, {"error": "max_retries_exceeded"}


# -------------------------
# VOW push function
# -------------------------
async def trigger_vow(orders: Dict[str, Any]) -> Dict[str, Any]:
    if not orders:
        return {"status": "no_orders"}

    idempotency_key = make_idempotency_key(orders)
    base_headers = {
        "Authorization": f"Bearer {VOW_API_KEY}",
        "User-Agent": "pooled-orders/1.0",
    }

    async with httpx.AsyncClient() as client:
        if BATCH_SEND:
            if SEND_AS_XML:
                body = orders_to_xml(orders)
                headers = {**base_headers, "Content-Type": "application/xml", "Idempotency-Key": idempotency_key}
                success, resp = await _post_with_retries(client, VOW_URL, headers, body, is_json=False)
            else:
                body = orders_to_json(orders)
                headers = {**base_headers, "Content-Type": "application/json", "Idempotency-Key": idempotency_key}
                success, resp = await _post_with_retries(client, VOW_URL, headers, body, is_json=True)

            if success:
                logger.info("VOW batch send success. Idempotency: %s", idempotency_key)
                return {"status": "sent", "mode": "batch", "idempotency_key": idempotency_key, "response": resp}
            else:
                logger.error("VOW batch send failed: %s", resp)
                return {"status": "failed", "mode": "batch", "idempotency_key": idempotency_key, "error": resp}
        else:
            results = {}
            for oid, order in orders.items():
                single_key = hashlib.sha256(oid.encode("utf-8")).hexdigest()
                if SEND_AS_XML:
                    body = orders_to_xml({oid: order})
                    headers = {**base_headers, "Content-Type": "application/xml", "Idempotency-Key": single_key}
                    success, resp = await _post_with_retries(client, VOW_URL, headers, body, is_json=False)
                else:
                    body = orders_to_json({oid: order})
                    headers = {**base_headers, "Content-Type": "application/json", "Idempotency-Key": single_key}
                    success, resp = await _post_with_retries(client, VOW_URL, headers, body, is_json=True)

                results[oid] = {"success": success, "response": resp}
                if not success:
                    logger.error("Failed to send order %s to VOW: %s", oid, resp)

            all_ok = all(r["success"] for r in results.values())
            return {"status": "sent" if all_ok else "partial_failure", "results": results}


# -------------------------
# FastAPI endpoints
# -------------------------
@app.post("/pooled-order")
async def pooled_order(request: Request):
    """
    Receives the FULL Wix order payload from the Wix Automation webhook.
    Excludes DELIVERY orders and deduplicates by order_id.
    """
    payload = await request.json()

    # Wix may wrap the order in "data" or "event" depending on automation version
    order = payload.get("data") or payload.get("event") or payload

    order_id = order.get("id")
    if not order_id:
        logger.warning("Received payload without order id")
        return {"error": "No order ID found in payload", "payload": payload}

    # Deduplicate: ignore if already pooled
    if order_id in order_pool:
        logger.info("Duplicate webhook for order %s ignored (already in pool)", order_id)
        return {"status": "ignored", "reason": "duplicate", "order_id": order_id}

    # Exclude delivery orders
    if is_delivery_order(order):
        logger.info("Order %s identified as DELIVERY and ignored", order_id)
        ignored_orders_log[order_id] = {
            "order_id": order_id,
            "timestamp": datetime.now(UK_TZ).isoformat(),
            "reason": "delivery_order",
            "sample": {k: order.get(k) for k in ("channelInfo", "fulfillment", "shippingInfo", "shippingAddress")}
        }
        return {"status": "ignored", "reason": "delivery_order", "order_id": order_id}

    # Build pooled record
    pooled = {
        "order_id": order_id,
        "number": order.get("number"),
        "created": order.get("createdDate") or order.get("createdAt") or order.get("created"),
        "channel": (order.get("channelInfo") or {}).get("type") if order.get("channelInfo") else order.get("channel"),
        "payment_status": order.get("paymentStatus"),
        "fulfillment_status": order.get("fulfillmentStatus"),
        "customer_email": (order.get("billingInfo") or {}).get("email"),
        "customer_name": " ".join(filter(None, [
            (order.get("buyerInfo") or {}).get("firstName"),
            (order.get("buyerInfo") or {}).get("lastName")
        ])).strip(),
        "items": [
            {
                "name": item.get("name"),
                "quantity": item.get("quantity"),
                "price": (item.get("priceData") or {}).get("price") or item.get("price"),
                "sku": item.get("sku")
            }
            for item in order.get("lineItems", []) or []
        ],
        "totals": order.get("totals", {}),
        "raw": order
    }

    # Add to pool
    order_pool[order_id] = pooled
    logger.info("Pooled order %s. Pool size: %d. Pool total: %.2f", order_id, len(order_pool), pool_total())

    # Check thresholds
    total_value = pool_total()
    timeout_trigger = any(is_business_timeout(o.get("created")) for o in order_pool.values())

    if total_value >= POUND_THRESHOLD or timeout_trigger:
        logger.info("Threshold reached (total=%.2f timeout=%s). Triggering VOW.", total_value, timeout_trigger)
        result = await trigger_vow(order_pool)
        # Clear pool on success or partial failure; if failed, keep pool for retry
        if result.get("status") in ("sent", "partial_failure"):
            order_pool.clear()
            logger.info("Pool cleared after VOW push. Result: %s", result.get("status"))
        else:
            logger.error("VOW push failed; keeping pool for retry. Error: %s", result.get("error"))
        return {
            "status": "triggered",
            "reason": "threshold" if total_value >= POUND_THRESHOLD else "timeout",
            "total_value": total_value,
            "vow_result": result
        }

    return {
        "status": "pooled",
        "order_id": order_id,
        "current_pool_value": total_value,
        "pool_size": len(order_pool)
    }


@app.get("/debug-pool")
async def debug_pool():
    """
    Returns the current pooled orders and ignored orders for debugging.
    """
    return {
        "pool_size": len(order_pool),
        "pool_total": pool_total(),
        "orders": order_pool,
        "ignored_orders_count": len(ignored_orders_log),
        "ignored_orders_sample": ignored_orders_log
    }


@app.get("/clear-pool")
async def clear_pool():
    """
    Clears the in-memory pool manually.
    """
    order_pool.clear()
    logger.info("Pool manually cleared")
    return {"status": "cleared"}


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now(UK_TZ).isoformat()}


# -------------------------
# Run with: uvicorn main:app --host 0.0.0.0 --port 8000
# -------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)