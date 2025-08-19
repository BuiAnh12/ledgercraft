import os
import pendulum
from typing import Literal
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Great Expectations (GX 0.18.x)
import great_expectations as gx
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult

DWH_GE_URL = os.getenv("DWH_GE_URL")

# Reusable helpers -------------------------------------------------------------

def _get_validator(schema: str, table: str):
    """
    Create a GX context and a validator bound to stg.<table>.
    Ensures the ExpectationSuite exists, otherwise creates it.
    """
    ctx = gx.get_context(context_root_dir="/opt/airflow/dags/great_expectations")

    # Add or get datasource
    ds = ctx.data_sources.add_sql(
        name="dwh_pg",
        connection_string=DWH_GE_URL,
    )

    # Register a table asset
    asset = ds.add_table_asset(
        name=f"{schema}_{table}",
        table_name=table,
        schema_name=schema,
    )

    # Build a batch request
    batch_req = asset.build_batch_request()

    suite_name = f"{schema}.{table}.suite"

    # Ensure suite exists (create if missing)
    try:
        ctx.suites.get(suite_name)
    except gx.exceptions.DataContextError:
        ctx.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    # Return validator bound to suite
    return ctx, ctx.get_validator(
        batch_request=batch_req,
        expectation_suite_name=suite_name
    )

# Expectation sets per table ---------------------------------------------------

def validate_stg_customers():
    ctx, v = _get_validator("stg", "customers")
    # Basic columns
    v.expect_column_values_to_not_be_null("id")
    v.expect_column_values_to_be_unique("id")
    v.expect_column_values_to_not_be_null("email")
    v.expect_column_values_to_match_regex("country", r"^[A-Z]{2}$")
    v.expect_column_values_to_be_between("kyc_level", min_value=0, max_value=3, mostly=1.0)
    v.expect_column_values_to_be_in_set("risk_band", ["LOW","MEDIUM","HIGH"])
    v.expect_column_values_to_not_be_null("created_at")
    v.expect_column_values_to_not_be_null("updated_at")
    # Save suite updates
    ctx.add_or_update_expectation_suite(v.expectation_suite)
    # Evaluate
    res = v.validate()
    _raise_if_failed(res, "stg.customers")
    return True

def validate_stg_accounts():
    ctx, v = _get_validator("stg", "accounts")
    v.expect_column_values_to_not_be_null("id")
    v.expect_column_values_to_be_unique("id")
    v.expect_column_values_to_not_be_null("customer_id")
    v.expect_column_values_to_match_regex("currency", r"^[A-Z]{3}$")
    v.expect_column_values_to_match_regex("country", r"^[A-Z]{2}$")
    v.expect_column_values_to_be_in_set("status", ["ACTIVE","SUSPENDED","CLOSED"])
    v.expect_column_values_to_not_be_null("created_at")
    v.expect_column_values_to_not_be_null("updated_at")
    # Save suite updates
    ctx.add_or_update_expectation_suite(v.expectation_suite)
    res = v.validate()
    _raise_if_failed(res, "stg.accounts")
    return True

def validate_stg_transactions():
    ctx, v = _get_validator("stg", "transactions")
    v.expect_column_values_to_not_be_null("id")
    v.expect_column_values_to_be_unique("id")
    v.expect_column_values_to_not_be_null("account_id")
    v.expect_column_values_to_be_in_set("type", ["PAYMENT","TRANSFER_IN","TRANSFER_OUT","REFUND","FEE"])
    v.expect_column_values_to_be_in_set("status", ["PENDING","SETTLED","DECLINED","REVERSED","CHARGEBACK"])
    v.expect_column_values_to_match_regex("currency", r"^[A-Z]{3}$")
    v.expect_column_values_to_match_regex("country", r"^[A-Z]{2}$")
    v.expect_column_values_to_be_greater_than("amount", 0)
    v.expect_column_values_to_not_be_null("created_at")
    # created_at should not be in the future (allow tiny clock skew of 2 minutes)
    # We'll approximate by requiring created_at <= now()+120s
    now_plus = pendulum.now("UTC").add(minutes=2)
    v.expect_column_values_to_be_between("created_at", max_value=now_plus)
    # Save suite updates
    ctx.add_or_update_expectation_suite(v.expectation_suite)
    res = v.validate()
    _raise_if_failed(res, "stg.transactions")
    return True

def check_fk_transactions_accounts():
    """
    Simple FK check using SQL (faster & clearer than forcing GX to do it):
    count rows in transactions whose account_id doesn't exist in stg.accounts → must be zero.
    """
    dwh = PostgresHook(postgres_conn_id="dwh_postgres")
    cnt = dwh.get_first("""
        SELECT COUNT(*) FROM stg.transactions t
        LEFT JOIN stg.accounts a ON a.id = t.account_id
        WHERE a.id IS NULL
    """)[0]
    if cnt and cnt > 0:
        raise AssertionError(f"FK check failed: {cnt} transactions reference missing accounts")
    return True
