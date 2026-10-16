"""Testes de integridade da DAG do pipeline ETL do IBGE."""

import pytest
from airflow.models import DagBag

DAG_ID = "ibge_pnadc_etl"
ORDEM_TASKS = [
    "extrair_api",
    "carregar_mongo",
    "extrair_mongo",
    "transformar",
    "carregar_neon",
    "validar_neon",
]


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(include_examples=False)


def test_sem_erros_de_import(dagbag: DagBag) -> None:
    assert not dagbag.import_errors, f"Erros de import: {dagbag.import_errors}"


def test_dag_existe_com_todas_as_tasks(dagbag: DagBag) -> None:
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None
    assert set(dag.task_ids) == set(ORDEM_TASKS)


def test_tasks_em_sequencia(dagbag: DagBag) -> None:
    dag = dagbag.dags.get(DAG_ID)
    for anterior, proxima in zip(ORDEM_TASKS, ORDEM_TASKS[1:]):
        assert dag.get_task(proxima).upstream_task_ids == {anterior}
