""" Kedro plugin for running a project with Airflow """

from collections import defaultdict
from pathlib import Path

import click
import jinja2
from click import secho
from kedro.framework.project import pipelines
from kedro.framework.startup import ProjectMetadata
from slugify import slugify


@click.group(name="Kedro-Airflow")
def commands():
    """Kedro plugin for running a project with Airflow"""
    pass


@commands.group(name="airflow")
def airflow_commands():
    """Run project with Airflow"""
    pass


@airflow_commands.command()
@click.option("-p", "--pipeline", "pipeline_name", default="__default__")
@click.option("-e", "--env", default="local")
@click.option(
    "-t",
    "--target-dir",
    "target_path",
    type=click.Path(writable=True, resolve_path=True, file_okay=False),
    default="./airflow_dags/",
)
@click.option(
    "-j",
    "--jinja-file",
    type=click.Path(
        exists=True, readable=True, resolve_path=True, file_okay=True, dir_okay=False
    ),
    default=Path(__file__).parent / "airflow_dag_template.j2",
)
@click.option("-E", "--envs", "env_variables")
@click.option("-i", "--custom-id", "id_", type=str, default="")
@click.option(
    "-w",
    "--working-directory",
    "working_dir",
    type=click.Path(writable=True, resolve_path=True, file_okay=False),
)
@click.pass_obj
def create(
    metadata: ProjectMetadata,
    pipeline_name,
    env,
    target_path,
    jinja_file,
    env_variables,
    id_,
    working_dir,
):  # pylint: disable=too-many-locals,too-many-arguments
    """Create an Airflow DAG for a project"""
    jinja_file = Path(jinja_file).resolve()
    loader = jinja2.FileSystemLoader(jinja_file.parent)
    jinja_env = jinja2.Environment(autoescape=True, loader=loader, lstrip_blocks=True)
    jinja_env.filters["slugify"] = slugify
    template = jinja_env.get_template(jinja_file.name)

    package_name = metadata.package_name
    if id_ != "":
        id_ = "_" + id_.lstrip("_")
    dag_name = f"{package_name}_{pipeline_name.strip('_')}{id_}"
    dag_filename = f"{dag_name}_dag.py"

    target_path = Path(target_path)
    target_path = target_path / dag_filename

    target_path.parent.mkdir(parents=True, exist_ok=True)

    pipeline = pipelines.get(pipeline_name)

    dependencies = defaultdict(list)
    for node, parent_nodes in pipeline.node_dependencies.items():
        for parent in parent_nodes:
            dependencies[parent].append(node)

    env_vars = dict()
    if env_variables:
        for assingment in env_variables.split(";"):
            varname, val = assingment.split("=", 1)
            env_vars[varname] = val

    template.stream(
        dag_name=dag_name,
        dependencies=dependencies,
        env=env,
        pipeline_name=pipeline_name,
        package_name=package_name,
        pipeline=pipeline,
        env_vars=env_vars,
        working_dir=working_dir,
    ).dump(str(target_path))

    secho("")
    secho("An Airflow DAG has been generated in:", fg="green")
    secho(str(target_path))
    secho("This file should be copied to your Airflow DAG folder.", fg="yellow")
    secho(
        "The Airflow configuration can be customized by editing this file.", fg="green"
    )
    secho("")
    secho(
        "This file also contains the path to the config directory, this directory will need to "
        "be available to Airflow and any workers.",
        fg="yellow",
    )
    secho("")
    secho(
        "Additionally all data sets must have an entry in the data catalog.",
        fg="yellow",
    )
    secho(
        "And all local paths in both the data catalog and log config must be absolute paths.",
        fg="yellow",
    )
    secho("")
