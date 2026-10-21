import os
import sys

import pytest

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DAGS_FOLDER = os.path.join(AIRFLOW_HOME, "dags")
sys.path.insert(0, os.path.join(AIRFLOW_HOME, "plugins"))


@pytest.fixture(scope="session")
def dagbag():
    from airflow.models.dagbag import DagBag

    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
