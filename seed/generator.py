import os, random, string, argparse
from datetime import datetime, timedelta
from faker import Faker
from dotenv import load_dotenv
import psycopg

load_dotenv('.env')

OLTP_DB = os.getenv('OLTP_DB')
OLTP_USER = os.getenv('OLTP_USER')
OLTP_PASSWORD = os.getenv('OLTP_PASSWORD')
OLTP_PORT = int(os.getenv('OLTP_PORT', '5432'))

def conn():
    return psycopg.connect(
        host="localhost", port=OLTP_PORT, dbname=OLTP_DB,
        user=OLTP_USER, password=OLTP_PASSWORD
    )

def gen_email(fake):
    # realistic unique email domain mix
    base = fake.user_name()
    domain = random.choice(["gmail.com","yahoo.com","outlook.com","example.com"])
    return f"{base}{random.randint(1,9999)}@{domain}"

def random_currency():
    return random.choice(["VND","USD"])

def main(n_customers=200, max_accounts_per_customer=2):
    fake = Faker()
    Faker.seed(42); random.seed(42)

    with conn() as c, c.cursor() as cur:
        print(f"Seeding {n_customers} customers...")
        customers = []
        for _ in range(n_customers):
            email = gen_email(fake)
            country = random.choice(["VN","SG","US","JP"])
            kyc_level = random.choice([0,1,2,3])
            risk = random.choice(["LOW","LOW","MEDIUM"])  # skew low
            cur.execute(
                """
                INSERT INTO customers (email, country, kyc_level, risk_band)
                VALUES (%s,%s,%s,%s)
                RETURNING id
                """,
                (email, country, kyc_level, risk)
            )
            customers.append(cur.fetchone()[0])

        print("Seeding accounts...")
        for cust_id in customers:
            for _ in range(random.randint(1, max_accounts_per_customer)):
                cur.execute(
                   """
                    INSERT INTO accounts (customer_id, currency, country, status)
                    VALUES (%s, %s, %s, 'ACTIVE')
                    ON CONFLICT (customer_id, currency)
                    WHERE status <> 'CLOSED'
                    DO NOTHING
                    """, (cust_id, random_currency(), "VN")
                )

        c.commit()
        print("Done.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=200)
    ap.add_argument("--max-accounts-per-customer", type=int, default=2)
    args = ap.parse_args()
    print(args.customers,args.max_accounts_per_customer)
    main(args.customers, args.max_accounts_per_customer)
