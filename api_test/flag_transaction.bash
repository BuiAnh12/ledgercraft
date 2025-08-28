ACC=73cd6c3c-daf6-4cf3-96d7-7fb0090a679d	
for i in 1 2 3 4 5 6; do
  KEY=$(python - <<'PY'
import uuid; print(uuid.uuid4())
PY
)
  curl -s -X POST http://localhost:8001/v1/transactions \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $KEY" \
    -d "{
      \"account_id\":\"$ACC\",
      \"type\":\"PAYMENT\",
      \"amount\":\"250000.00\",
      \"currency\":\"VND\",
      \"merchant_name\":\"TestCafe\",
      \"merchant_category\":\"coffee\",
      \"country\":\"VN\"
    }" > /dev/null
done
