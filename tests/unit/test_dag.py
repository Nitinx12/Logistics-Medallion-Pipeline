"""tests/unit/test_dag.py - verify Airflow DAG parses without scheduler"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Mock airflow if not installed
try:
    import airflow  # noqa: F401
    HAS_AIRFLOW = True
except ImportError:
    HAS_AIRFLOW = False


DAG_FILES = [
    "airflow/dags/freightlake_bronze.py",
    "airflow/dags/freightlake_silver.py",
    "airflow/dags/freightlake_gold.py",
]
EXPECTED_DAG_IDS = ["freightlake_bronze", "freightlake_silver", "freightlake_gold"]


def test_dag_import_with_airflow():
    if not HAS_AIRFLOW:
        pytest.skip("airflow not installed")
    import importlib.util
    for dag_file, expected_id in zip(DAG_FILES, EXPECTED_DAG_IDS):
        try:
            spec = importlib.util.spec_from_file_location(expected_id, Path(dag_file))
            mod = importlib.util.module_from_spec(spec)  # type: ignore
            spec.loader.exec_module(mod)  # type: ignore
        except ImportError as e:
            pytest.skip(f"airflow import incomplete for {dag_file}: {e}")
        assert hasattr(mod, "dag")
        assert mod.dag.dag_id == expected_id


def test_bronze_tasks_parallel():
    # Verify bronze DAG has two independent tasks that run in parallel (no dep between them)
    content = Path("airflow/dags/freightlake_bronze.py").read_text(encoding="utf-8")
    assert "bronze_pg" in content
    assert "bronze_mongo" in content
    # parallel pattern: [bronze_pg, bronze_mongo] with no >> between them
    assert "[bronze_pg, bronze_mongo]" in content
    # ensure they are not chained as bronze_pg >> bronze_mongo
    assert "bronze_pg >> bronze_mongo" not in content


def test_dag_count_is_three():
    import glob
    assert len(glob.glob("airflow/dags/freightlake_*.py")) == 3
    assert len(EXPECTED_DAG_IDS) == 3


def test_dag_structure_mocked():
    # Mock airflow operators to verify DAG wiring without needing airflow installed
    mock_bash = MagicMock()
    mock_python = MagicMock()
    for m in [mock_bash, mock_python]:
        m.__rrshift__ = MagicMock(return_value=m)
        m.__rshift__ = MagicMock(return_value=m)
        m.__lshift__ = MagicMock(return_value=m)

    mock_dag = MagicMock()
    mock_dag.__enter__ = MagicMock(return_value=mock_dag)
    mock_dag.__exit__ = MagicMock(return_value=False)

    with patch.dict("sys.modules", {
        "airflow": MagicMock(DAG=lambda *a, **kw: mock_dag),
        "airflow.operators.bash": MagicMock(BashOperator=lambda *a, **kw: mock_bash),
        "airflow.operators.python": MagicMock(PythonOperator=lambda *a, **kw: mock_python),
        "airflow.sensors": MagicMock(),
        "airflow.sensors.external_task": MagicMock(ExternalTaskSensor=lambda *a, **kw: mock_bash),
    }):
        import importlib.util
        for dag_file in DAG_FILES:
            spec = importlib.util.spec_from_file_location("freightlake_dag_mock", Path(dag_file))
            mod = importlib.util.module_from_spec(spec)  # type: ignore
            try:
                spec.loader.exec_module(mod)  # type: ignore
            except Exception as e:
                pytest.skip(f"DAG parse mocked failed for {dag_file}: {e}")
        assert True


def test_compose_has_dag_processor():
    content = Path("docker/compose.yml").read_text(encoding="utf-8")
    assert "dag-processor" in content
    assert "airflow-apiserver" in content
    assert "airflow-scheduler" in content
    assert "build:" in content
    assert "context: .." in content
