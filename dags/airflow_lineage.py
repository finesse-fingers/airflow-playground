from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.lineage import AUTO
from airflow.lineage.entities import File
from airflow.models import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

FILE_CATEGORIES = ["CAT1", "CAT2", "CAT3"]

args = {
    'owner': 'airflow',
    'start_date': days_ago(2)
}

dag = DAG(
    dag_id='example_lineage', default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'lineage']
)

f_final = File(url="/tmp/final")
run_this_last = DummyOperator(task_id='run_this_last', dag=dag,
    inlets=AUTO,
    outlets=f_final)

f_in = File(url="/tmp/whole_directory/")
outlets = []
for file in FILE_CATEGORIES:
    f_out = File(url="/tmp/{}/{{{{ execution_date }}}}".format(file))
    outlets.append(f_out)

run_this = BashOperator(
    task_id='run_me_first', bash_command='echo 1', dag=dag,
    inlets=f_in,
    outlets=outlets
)
run_this.set_downstream(run_this_last)
