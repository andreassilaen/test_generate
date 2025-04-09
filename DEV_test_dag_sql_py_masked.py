import sys
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

# Menyesuaikan path agar sesuai dengan lokasi file di dalam Airflow
# sys.path.append("/opt/airflow/dags/data_temp")

@dag(dag_id="DEV_test_dag_sql_py",
     schedule_interval=None,
     tags=["genesis.cargo"],
     start_date=datetime(year=2023, month=3, day=28, hour=10, minute=5),
     template_searchpath="/opt/airflow/modules/",
     max_active_runs=1,
     catchup=False)
def testing():
    select_sanggah_job = RedshiftDataOperator(
        task_id='select_sanggah_job',
        cluster_identifier='***REDACTED_CLUSTER***',
        database='***REDACTED_DB***',
        db_user='***REDACTED_USER***',
        sql="""
            SELECT * FROM monitoring.lp_asana_sanggah_job
            WHERE dag_id = 'PROD_LPDWH_Operation_Job_RDS'
            LIMIT 5;
        """
    )
    
    call_sp_summary_sanggah_job = RedshiftDataOperator(
        task_id='call_sp_summary_sanggah_job',
        database='***REDACTED_DB***',
        cluster_identifier='***REDACTED_CLUSTER***',
        db_user='***REDACTED_USER***',
        poll_interval=10,
        sql="sp_summary_sanggah_job.sql"
    )
    
    run_hello_world = BashOperator(
        task_id='run_hello_world_script',
        bash_command='python /opt/airflow/modules/hello_world.py'
    )
    
    select_sanggah_job >> call_sp_summary_sanggah_job >> run_hello_world

dag = testing()
