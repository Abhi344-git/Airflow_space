from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt_project"
PROFILES_DIR = "/opt/airflow/profiles"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# ==============================================================================
# 1. CUSTOMER BRONZE DAG - Daily at 1:00 PM IST (07:30 AM UTC)
# ==============================================================================
with DAG(
    dag_id="citi_customer_bronze_daily",
    default_args=default_args,
    description="Customer bronze run daily at 1:00 PM IST (07:30 UTC)",
    schedule_interval="30 7 * * *",  # 07:30 UTC = 1:00 PM IST
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["citi", "customer", "bronze"],
) as dag_customer:

    check_conn_cust = BashOperator(
        task_id="check_snowflake_connection",
        bash_command=f"cd {DBT_DIR} && dbt debug --profiles-dir {PROFILES_DIR}",
    )

    run_customer_bronze = BashOperator(
        task_id="run_customer_bronze",
        bash_command=f"cd {DBT_DIR} && dbt run --select customer_bronze --profiles-dir {PROFILES_DIR}",
    )

    test_customer = BashOperator(
        task_id="test_customer_bronze",
        bash_command=f"cd {DBT_DIR} && dbt test --select customer_bronze --profiles-dir {PROFILES_DIR}",
    )

    check_conn_cust >> run_customer_bronze >> test_customer


# ==============================================================================
# 2. FACT TRANSACTION DAG - Daily at 1:20 PM IST (07:50 AM UTC)
# ==============================================================================
with DAG(
    dag_id="citi_f_transaction_daily",
    default_args=default_args,
    description="Fact transaction run daily at 1:20 PM IST (07:50 UTC)",
    schedule_interval="50 7 * * *",  # 07:50 UTC = 1:20 PM IST
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["citi", "transaction", "bronze"],
) as dag_trans:

    check_conn_trans = BashOperator(
        task_id="check_snowflake_connection",
        bash_command=f"cd {DBT_DIR} && dbt debug --profiles-dir {PROFILES_DIR}",
    )

    run_f_transaction = BashOperator(
        task_id="run_f_transaction_bronze",
        bash_command=f"cd {DBT_DIR} && dbt run --select f_transaction_bronze --profiles-dir {PROFILES_DIR}",
    )

    test_transaction = BashOperator(
        task_id="test_f_transaction_bronze",
        bash_command=f"cd {DBT_DIR} && dbt test --select f_transaction_bronze --profiles-dir {PROFILES_DIR}",
    )

    check_conn_trans >> run_f_transaction >> test_transaction