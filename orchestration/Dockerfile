# Local Airflow via the Astro CLI:  cd orchestration && astro dev start
#
# Check https://quay.io/repository/astronomer/astro-runtime for the current tag
# before first use; this pin is deliberate but will age.
FROM quay.io/astronomer/astro-runtime:12.7.1

# dbt goes in its own virtualenv rather than into the Airflow environment.
# Airflow and dbt pin overlapping dependencies -- protobuf, jinja2, packaging --
# and installing them together produces a resolver conflict that surfaces as a
# runtime import error rather than an install failure. Cosmos is built expecting
# this split, which is why ExecutionConfig takes an explicit dbt_executable_path.
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-core==1.12.0 dbt-snowflake==1.9.0 && \
    deactivate
