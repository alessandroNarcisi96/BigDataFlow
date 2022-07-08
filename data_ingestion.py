from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow import DAG
from datetime import datetime, timedelta
import boto3
import json
from cassandra.query import SimpleStatement, BatchStatement
from cassandra.cluster import Cluster
# Parameters
WORFKLOW_DAG_ID = "my_example_dag"
WORFKFLOW_START_DATE = datetime(2022, 1, 1)
WORKFLOW_SCHEDULE_INTERVAL = "* * * * *"
WORKFLOW_EMAIL = ["youremail@example.com"]

WORKFLOW_DEFAULT_ARGS = {
            "owner": "ale",
            "start_date": WORFKFLOW_START_DATE,
            "email": WORKFLOW_EMAIL,
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 0}

# Initialize DAG
dag = DAG(dag_id=WORFKLOW_DAG_ID, schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,default_args=WORKFLOW_DEFAULT_ARGS)




def data_ingestion_from_s3():
    s3 = boto3.resource('s3')
    content_object = s3.Object('pinterest-aicore', 'data.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    row = json_content[6]
    row["follower_count"] = row["follower_count"].replace("k","")
    row["follower_count"] = int(row["follower_count"])

    clstr=Cluster()
    session=clstr.connect('mykeyspace')
    stmt=session.prepare("INSERT INTO pinterest_6 (unique_id,category,title,description,follower_count,tag_list,is_image_or_video,image_src,downloaded,save_location) VALUES (?,?,?,?,?,?,?,?,?,?)")
    qry=stmt.bind([row["unique_id"],row["category"],row["title"],row["description"],row["follower_count"],row["tag_list"],row["is_image_or_video"],row["image_src"],row["downloaded"],row["save_location"]])


job_1_operator = PythonOperator(task_id="task_job_1",python_callable=data_ingestion_from_s3,dag=dag)
