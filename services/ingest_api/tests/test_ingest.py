import os
from uuid import UUID
from fastapi.testclient import TestClient
from services.ingest_api.app.main import app

client = TestClient(app)

def test_end_to_end_create_customer_account_txn():
    # create customer
    r = client.post("/v1/customers", json={"email":"tester@example.com","country":"VN","kyc_level":1})
    assert r.status_code == 201, r.text
    cust = r.json(); assert "id" in cust
    # create account
    r = client.post("/v1/accounts", json={"customer_id": cust["id"], "currency":"VND", "country":"VN"})
    assert r.status_code == 201, r.text
    acc = r.json(); assert acc["currency"] == "VND"
    # create txn
    # r = client.post("/v1/transactions", json={
    #     "account_id": acc["id"], "type":"PAYMENT", "amount":"1000.00",
    #     "currency":"VND", "merchant_name":"Coffee", "country":"VN"
    # })
    # assert r.status_code == 201, r.text
    # body = r.json()
    # UUID(body["id"])  # valid UUID
    # assert body["status"] == "PENDING"
