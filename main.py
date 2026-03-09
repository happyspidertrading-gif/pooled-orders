from fastapi import FastAPI, Form

app = FastAPI()

@app.post("/pooled-order")
async def pooled_order(
    orderId: str = Form(None),
    createdAt: str = Form(None),
    customerName: str = Form(None),
    email: str = Form(None),
    total: str = Form(None),
    items: str = Form(None)
):
    return {
        "orderId": orderId,
        "createdAt": createdAt,
        "customerName": customerName,
        "email": email,
        "total": total,
        "items": items
    }