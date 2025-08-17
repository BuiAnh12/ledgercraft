from fastapi.testclient import TestClient
from services.ingest_api.app.main import app
from uuid import uuid4

client = TestClient(app)

def test_idempotent_transaction():
    # Create customer & account quickly
    rc = client.post("/v1/customers", json={"email":"idemtester@example.com","country":"VN","kyc_level":1})
    cust_id = rc.json()["id"]
    ra = client.post("/v1/accounts", json={"customer_id": cust_id, "currency":"VND", "country":"VN"})
    acc_id = ra.json()["id"]

    key = str(uuid4())
    body = {
        "account_id": acc_id,
        "type":"PAYMENT",
        "amount":"123.45",
        "currency":"VND",
        "merchant_name":"Test",
        "country":"VN"  
    }

    r1 = client.post("/v1/transactions", headers={"Idempotency-Key": key}, json=body)
    assert r1.status_code == 201, r1.text
    first = r1.json()

    r2 = client.post("/v1/transactions", headers={"Idempotency-Key": key}, json=body)
    assert r2.status_code == 201
    assert r2.json() == first

    # Change body but reuse key -> 409
    bad = body | {"amount":"200.00"}
    r3 = client.post("/v1/transactions", headers={"Idempotency-Key": key}, json=bad)
    assert r3.status_code == 409
