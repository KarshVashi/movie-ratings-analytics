"""Airflow orchestration for the movie_ratings_analytics project, via Astronomer Cosmos.

Why Cosmos rather than a BashOperator
-------------------------------------
The obvious way to run dbt from Airflow is one task::

    BashOperator(task_id="dbt_build", bash_command="dbt build")

It works, and it is the wrong shape. That task is opaque: Airflow sees a single
success or failure and knows nothing about the sixteen nodes inside it. The
consequences are concrete, not stylistic.

*Retries are all-or-nothing.* A Snowflake timeout on the last mart forces a
retry of the entire DAG, re-running a 20M-row incremental that had already
succeeded.

*There is no lineage in the UI.* When something fails at 03:00, the on-call
engineer gets "dbt_build failed" and has to open the logs and read dbt's stdout
to find out which model, and what depends on it.

*Nothing can run in parallel across DAG boundaries.* The five staging models are
mutually independent, but a downstream DAG waiting on this one has to wait for
all of it, including branches it does not care about.

Cosmos parses the dbt manifest at DAG-render time and emits one Airflow task per
model, with dbt's dependency graph translated into Airflow's. Retries become
per-model, the Airflow graph view shows real lineage, and a failure names the
model that failed. Each model's tests attach to that model's task, so a test
failure blocks its own dependents rather than the whole run.

The cost is real and worth stating: Cosmos has to parse the manifest to build
the DAG, and doing that on every scheduler heartbeat is expensive. That is what
LoadMode.DBT_MANIFEST below avoids -- the manifest is built once in CI and read
as a file here, rather than shelling out to dbt on every parse.
"""

from datetime import datetime, timedelta
from pathlib import Path

from cosmos import (
    DbtDag,
    ExecutionConfig,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Inside the Airflow image the dbt project is mounted here; see the Dockerfile.
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/movie_ratings_analytics")
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"

# Credentials come from an Airflow connection, not from a profiles.yml baked
# into the image. Cosmos translates the connection into a dbt profile at
# runtime, so there is exactly one place secrets live and it is not the repo.
profile_config = ProfileConfig(
    profile_name="movie_ratings_analytics",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "MOVIELENS",
            "schema": "PROD",
            "warehouse": "COMPUTE_WH",
        },
    ),
)

movie_ratings_analytics_dag = DbtDag(
    dag_id="movie_ratings_analytics",
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        # Read the manifest CI produced rather than regenerating it here.
        manifest_path=DBT_PROJECT_PATH / "target" / "manifest.json",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
    render_config=RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        # Source freshness runs as its own task ahead of the models. Unlike in
        # CI -- where a stale source says nothing about the code under review --
        # a stale source here means the upstream load did not happen, and
        # building on top of it would produce confidently wrong numbers.
        # Warnings do not block; errors do, via the thresholds in
        # _movielens__sources.yml.
        test_behavior="after_each",
    ),
    operator_args={
        "install_deps": True,
        # Per-model retries: this is the payoff. A transient Snowflake error on
        # one model retries that model, not the twenty minutes of work that
        # already succeeded upstream of it.
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    # Monthly, matching the load cadence the source freshness thresholds assume.
    # A daily schedule against a source that is refreshed monthly would burn
    # warehouse credits rebuilding identical output twenty-nine times over.
    schedule="@monthly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "analytics-engineering",
        "depends_on_past": False,
        "email_on_failure": False,
    },
    tags=["dbt", "snowflake", "movielens"],
    doc_md=__doc__,
)
