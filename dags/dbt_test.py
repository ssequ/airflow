from airflow import DAG
from airflow.models.variable import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.decorators import task_group
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

dbt_bash_executor_config={
  "KubernetesExecutor": {
    "init_containers": [{
        "name": "init-container",
        "image": "busybox",
        "command": ['sh', '-c', 'chmod -R 777 /data'],
        "volumeMounts": [{
            "mountPath": "/data",
            "name": "dag-storage"
        }]
     }],
    "container_security_context": {
        #"allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]}
    },
    "security_context": {
        #"runAsNonRoot": True,
        "runAsUser": 50000,
        "runAsGroup": 0,
        "fsGroup": 0,
        "seccompProfile": {"type": "RuntimeDefault"}
    },
    "volumes": [
      {
        "name": "dag-storage",
        "persistentVolumeClaim":
          {
            "claimName": "dag-storage"
          }
      }
    ],
    "volume_mounts": [
      {
        "name": "dag-storage",
        "mountPath": "/opt/airflow/dbt"
      }
    ]
  }
}


dag = DAG(
  dag_id="dbt_test_dag",
  schedule_interval="@daily",
  start_date=days_ago(1),
  catchup=False,
  default_args={"owner": "airflow"}
)

dbt_task = BashOperator(
  task_id="test-task",
  bash_command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt/orchestration",
  dag=dag,
  trigger_rule='all_done',
  executor_config=dbt_bash_executor_config
)
