```
ledgercraft
├─ Makefile
├─ README.md
├─ adr
│  └─ .keep
├─ api_test
│  ├─ basic_transaction.bash
│  ├─ basic_transaction_repeat.bash
│  └─ flag_transaction.bash
├─ docs
│  └─ .keep
├─ frontend
│  └─ web
│     └─ .keep
├─ infra
│  ├─ docker-compose.yml
│  ├─ grafana
│  ├─ kafka-connect
│  │  └─ debezium-postgres-source.json
│  └─ prometheus
├─ pipelines
│  ├─ airflow
│  │  ├─ .keep
│  │  ├─ dags
│  │  │  ├─ gx_validate.py
│  │  │  └─ oltp_to_stg.py
│  │  ├─ plugins
│  │  └─ requirements.txt
│  ├─ dbt
│  │  └─ .keep
│  └─ ge
│     └─ .keep
├─ seed
│  ├─ .keep
│  ├─ generator.py
│  └─ requirements.txt
├─ services
│  ├─ analytics_api
│  │  └─ .keep
│  ├─ ingest_api
│  │  ├─ .keep
│  │  ├─ app
│  │  │  ├─ core
│  │  │  │  ├─ config.py
│  │  │  │  ├─ db.py
│  │  │  │  ├─ errors.py
│  │  │  │  ├─ idem.py
│  │  │  │  ├─ models.py
│  │  │  │  └─ schemas.py
│  │  │  ├─ main.py
│  │  │  └─ routes
│  │  │     ├─ accounts.py
│  │  │     ├─ customers.py
│  │  │     ├─ debug.py
│  │  │     └─ transactions.py
│  │  ├─ requirements.txt
│  │  └─ tests
│  │     ├─ test_idem.py
│  │     └─ test_ingest.py
│  └─ stream
│     ├─ .keep
│     ├─ config.py
│     ├─ db.py
│     ├─ faust_app.py
│     ├─ lc-stream-data
│     │  └─ v1
│     │     └─ tables
│     └─ requirements.txt
├─ start.bash
└─ warehouse
   ├─ ddl
   │  ├─ .keep
   │  ├─ dwh_curated.sql
   │  ├─ dwh_dev_bootstrap.sql
   │  ├─ dwh_staging.sql
   │  ├─ oltp.sql
   │  └─ oltp_risk_flags.sql
   └─ views
      └─ materialized
         └─ .keep

```