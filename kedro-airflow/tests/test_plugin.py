from pathlib import Path

import pytest
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline, node
from kedro_airflow.plugin import commands


def identity(arg):
    return arg


@pytest.mark.parametrize(
    "dag_name,pipeline_name,command",
    [
        # Test normal execution
        ("hello_world_default_dag", "__default__", ["airflow", "create"]),
        # Test execution with alternate pipeline name
        ("hello_world_ds_dag", "ds", ["airflow", "create", "--pipeline", "ds"]),
        # Test execution with different dir and filename for Jinja2 Template
        (
            "hello_world_default_dag",
            "__default__",
            ["airflow", "create", "-j", "airflow_dag.j2"],
        ),
        # Test execution with the env variables option
        (
            "hello_world_default_dag",
            "__default__",
            ["airflow", "create", "-E", "var1=value1"],
        ),
        # Test execution with the
        (
            "hello_world_default_dag",
            "__default__",
            ["airflow", "create", "-w", "/opt/airflow/kedro/proj1"],
        ),
    ],
)
def test_create_airflow_dag(
    dag_name, pipeline_name, command, mocker, cli_runner, metadata
):
    """Check the generation and validity of a simple Airflow DAG."""
    dag_file = Path.cwd() / "airflow_dags" / f"{dag_name}.py"
    mock_pipeline = Pipeline(
        [
            node(identity, ["input"], ["intermediate"], name="node0"),
            node(identity, ["intermediate"], ["output"], name="node1"),
        ],
        tags="pipeline0",
    )
    mocker.patch.dict(pipelines, {pipeline_name: mock_pipeline})
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0
    assert dag_file.exists()

    expected_airflow_dag = 'tasks["node0"] >> tasks["node1"]'
    expected_environment_settings = 'os.environ["var1"] = value1'
    expected_working_dir_settings = "os.chdir('/opt/airflow/kedro/proj1')"
    with open(dag_file, "r", encoding="utf-8") as f:
        dag_code = [line.strip() for line in f.read().splitlines()]
    assert expected_airflow_dag in dag_code
    if "-E" in command:
        assert expected_environment_settings in dag_code
    if "-w" in command:
        assert expected_working_dir_settings in dag_code
    else:
        assert expected_working_dir_settings not in dag_code
