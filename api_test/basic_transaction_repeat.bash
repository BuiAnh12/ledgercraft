ACC=43ad0099-97e8-424c-ab5c-2dee3bb09690
KEY=6f44eb82-3450-4615-a916-b2d688b2e8d0

curl -s -X POST http://localhost:8001/v1/transactions \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $KEY" \
  -d "{
    \"account_id\":\"$ACC\",
    \"type\":\"PAYMENT\",
    \"amount\":\"250000.00\",
    \"currency\":\"VND\",
    \"merchant_name\":\"Starbucks Phu Nhuan\",
    \"merchant_category\":\"coffee\",
    \"description\":\"Latte + brownie\",
    \"country\":\"VN\"
  }"