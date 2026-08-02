{#
    Tears down the throwaway schema a CI run built into.

    Called from the CI workflow's cleanup step, which runs with if: always() so
    that a failed build still cleans up after itself. Without this, every pull
    request would leave a schema behind and the Snowflake account would silently
    accumulate them.

    The target guard is the important part. This macro issues a DROP SCHEMA
    CASCADE, and the difference between running it against `ci` and running it
    against `prod` is the entire warehouse. A workflow misconfiguration, a
    forgotten --target flag or a copy-pasted command should fail loudly rather
    than execute -- so the macro refuses to run anywhere but the ci target.
#}

{% macro drop_ci_schema() %}

    {% if target.name != 'ci' %}
        {{ exceptions.raise_compiler_error(
            "drop_ci_schema refuses to run against target '" ~ target.name ~ "'. "
            ~ "This macro drops a schema with CASCADE and is only ever safe on ci."
        ) }}
    {% endif %}

    {% set schema_to_drop = target.database ~ '.' ~ target.schema %}

    {% call statement('drop_schema', auto_begin=False) %}
        DROP SCHEMA IF EXISTS {{ schema_to_drop }} CASCADE
    {% endcall %}

    {{ log("Dropped CI schema " ~ schema_to_drop, info=True) }}

{% endmacro %}
