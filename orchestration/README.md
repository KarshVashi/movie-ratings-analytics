# Orchestration

Airflow running the dbt project through [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/), which renders each dbt model as its own Airflow task.

## Running it locally

Requires the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) and Docker.

```bash
cd orchestration
astro dev start
```

Airflow comes up on <http://localhost:8080> (`admin` / `admin`).

## Setup

**1. Mount the dbt project into the image.** The DAG expects it at `/usr/local/airflow/dbt/movie_ratings_analytics`. Add to `.astro/config.yaml`:

```yaml
project:
  name: movie-ratings-analytics-orchestration
mounts:
  - source: ../
    target: /usr/local/airflow/dbt/movie_ratings_analytics
```

**2. Create the Snowflake connection.** Admin → Connections → `snowflake_default`, type Snowflake. Cosmos turns this into a dbt profile at runtime, which is why no `profiles.yml` is baked into the image — the credentials live in exactly one place and it is not the repository.

**3. Make sure `target/manifest.json` exists.** The DAG uses `LoadMode.DBT_MANIFEST`, so it reads a pre-built manifest rather than shelling out to dbt on every scheduler parse. Run `dbt parse` once locally, or pull the artefact the CI workflow uploads.

## What the DAG graph should look like

Sixteen tasks, not one. The five staging models fan out in parallel, `dim_*` and `fct_*` converge behind them, and each model's tests hang off that model's task rather than running in a single block at the end.

If the graph shows one `dbt_build` box, Cosmos is not being used and the whole point has been lost — see the module docstring in `dags/movie_ratings_analytics_dag.py` for why that matters.

## Schedule

`@monthly`, matching the source load cadence and the freshness thresholds in `_movielens__sources.yml`. Running daily against a monthly-refreshed source would rebuild identical output twenty-nine times a month.
