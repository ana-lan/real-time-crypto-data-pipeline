import boto3
import logging
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────
AWS_REGION = "us-east-1"
GLUE_CRAWLER_NAME = "crypto-clean-crawler"
GLUE_DATABASE = "crypto_pipeline_db"
ATHENA_TABLE = "clean"
ATHENA_RESULTS_BUCKET = "s3://ana-lan-crypto-athena-results/"
AWS_PROFILE = "crypto-data-pipeline"

# ── Failure callback ───────────────────────────────────────────────
def on_failure_callback(context):
    """
    Called automatically by Airflow when any task in this DAG fails.
    
    'context' is a dict Airflow provides with details about the failure:
    - context['task_instance']: the specific task that failed
    - context['exception']: the actual exception that was raised
    - context['execution_date']: when this DAG run was scheduled for
    
    In production this would send an SNS notification or Slack message.
    For now we log a structured error that CloudWatch can pick up.
    """
    task_instance = context['task_instance']
    exception = context.get('exception')
    logger.error(
        f"DAG FAILURE ALERT | "
        f"dag={task_instance.dag_id} | "
        f"task={task_instance.task_id} | "
        f"execution_date={context['execution_date']} | "
        f"exception={exception}"
    )


# ── Helper: get boto3 session ──────────────────────────────────────
def get_boto3_session():
    """
    Returns a boto3 Session using the named AWS profile.
    
    Why a helper function instead of just calling boto3.client() directly?
    Because we need the same session config in multiple tasks, and
    centralizing it means one place to update if credentials change.
    """
    session = boto3.Session(
        profile_name=AWS_PROFILE
    )
    return session


# ── Task 1: Trigger Glue Crawler ───────────────────────────────────
def trigger_glue_crawler(**context):
    """
    Starts the Glue Crawler. Returns immediately — the crawler
    runs asynchronously, so this task just fires it and exits.
    
    boto3 Glue client method: start_crawler(Name=crawler_name)
    
    If the crawler is already running (RUNNING state), we skip
    triggering it and log a warning — don't error out, since a
    previous DAG run might still be finishing.
    """
    from botocore.exceptions import ClientError
    
    session = get_boto3_session()
    client = session.client('glue', region_name=AWS_REGION)

    try:
        client.start_crawler(Name=GLUE_CRAWLER_NAME)
        logger.info(f"Crawler '{GLUE_CRAWLER_NAME}' started successfully")
    except ClientError as e:
        if e.response['Error']['Code'] == 'CrawlerRunningException':
            logger.warning(f"Crawler '{GLUE_CRAWLER_NAME}' is already running, skipping trigger")
            return
        raise

# ── Task 2: Wait for Crawler ───────────────────────────────────────
def wait_for_crawler(**context):
    """
    Polls the crawler status every 15 seconds until it's no longer
    in RUNNING state. Times out after 10 minutes.
    
    Crawler states: READY → RUNNING → STOPPING → READY
    We wait until state is back to READY.
    
    boto3 method: client.get_crawler(Name=name)
    State lives at: response['Crawler']['State']
    """
    max_wait = 600
    elapsed = 0
    
    session = get_boto3_session()
    client = session.client('glue', region_name=AWS_REGION)

    while elapsed < max_wait:
        response = client.get_crawler(Name=GLUE_CRAWLER_NAME)
        state = response['Crawler']['State']

        if state == 'READY':
            logger.info(f"Crawler finished in {elapsed} seconds")
            return
        logger.info(f"Current State: {state}, elapsed time: {elapsed}")
        time.sleep(15)
        elapsed += 15
    
    raise TimeoutError (f"Crawler did not finish within {max_wait} seconds")



# ── Task 3: Repair Athena Table ────────────────────────────────────
def repair_athena_table(**context):
    """
    Runs MSCK REPAIR TABLE to register new partitions in the
    Glue Catalog after the crawler has updated the S3 metadata.
    
    Why is this needed even though the crawler already ran?
    The crawler updates the table SCHEMA in the catalog, but new
    partition PATHS in S3 need to be explicitly registered.
    MSCK REPAIR TABLE does this scan and registration in one shot.
    
    boto3 Athena client methods:
    - start_query_execution(QueryString, QueryExecutionContext, ResultConfiguration)
    - get_query_execution(QueryExecutionId) → check State
    """
    max_wait = 600
    elapsed = 0

    session = get_boto3_session()
    client = session.client('athena', region_name=AWS_REGION)

    query = f"MSCK REPAIR TABLE {GLUE_DATABASE}.{ATHENA_TABLE};"

    response = client.start_query_execution(
            QueryString = query,
            QueryExecutionContext = {'Database': GLUE_DATABASE},
            ResultConfiguration = {'OutputLocation': ATHENA_RESULTS_BUCKET}
        )
    query_id = response['QueryExecutionId']

    while elapsed < max_wait:
        poll = client.get_query_execution(QueryExecutionId=query_id)
        state = poll['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            logger.info(f"Athena query executed successfully in {elapsed} seconds")
            return
        if state in ['QUEUED', 'RUNNING']:
            logger.info(f"Current State: {state}, elapsed time: {elapsed}")
            time.sleep(5)
            elapsed += 5
        else:
            raise Exception (f"Athena query failed with status: {state}")
    raise TimeoutError (f"Athena query did not execute within {max_wait} seconds")

# ── Task 4: Health Check ───────────────────────────────────────────
def health_check(**context):
    """
    Runs SELECT COUNT(*) against the clean table.
    If count is 0 or query fails, raises an exception which
    triggers the on_failure_callback alert.
    """
    max_wait = 600
    elapsed = 0

    session = get_boto3_session()
    client = session.client('athena', region_name=AWS_REGION)

    query = f"SELECT COUNT(*) FROM {GLUE_DATABASE}.{ATHENA_TABLE}"

    response = client.start_query_execution(
            QueryString = query,
            QueryExecutionContext = {'Database': GLUE_DATABASE},
            ResultConfiguration = {'OutputLocation': ATHENA_RESULTS_BUCKET}
        )
    query_id = response['QueryExecutionId']

    while elapsed < max_wait:
        poll = client.get_query_execution(QueryExecutionId=query_id)
        state = poll['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            break
        if state in ['QUEUED', 'RUNNING']:
            time.sleep(5)
            elapsed += 5
        else:
            raise Exception(f"Athena query failed: {state}")
    else:
        raise TimeoutError(f"Query did not finish within {max_wait} seconds")
    
    results = client.get_query_results(QueryExecutionId=query_id)
    count = results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
    if count == '0':
        raise Exception("Health check failed: table is empty")
    logger.info(f"Health check passed, count: {count}")

# ── DAG definition ─────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # depends_on_past=False means each DAG run is independent —
    # a failed previous run doesn't block the next scheduled run
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    # If a task fails, wait 2 minutes then try once more before
    # marking it as truly failed and triggering the callback
    'on_failure_callback': on_failure_callback,
}

with DAG(
    dag_id='crypto_pipeline_orchestration',
    default_args=default_args,
    description='15-min refresh: crawl S3, repair Athena partitions, health check',
    schedule_interval='*/15 * * * *',
    # Cron expression: every 15 minutes
    # */15 = "every 15th minute" (0, 15, 30, 45)
    start_date=datetime(2026, 6, 19),
    catchup=False,
    # catchup=False means if Airflow was offline for a while,
    # it won't try to run all the missed scheduled intervals —
    # it just starts from now. For a real-time pipeline this is
    # almost always what you want.
    tags=['crypto', 'pipeline'],
) as dag:

    t1 = PythonOperator(
        task_id='trigger_glue_crawler',
        python_callable=trigger_glue_crawler,
    )

    t2 = PythonOperator(
        task_id='wait_for_crawler',
        python_callable=wait_for_crawler,
    )

    t3 = PythonOperator(
        task_id='repair_athena_table',
        python_callable=repair_athena_table,
    )

    t4 = PythonOperator(
        task_id='health_check',
        python_callable=health_check,
    )

    t1 >> t2 >> t3 >> t4