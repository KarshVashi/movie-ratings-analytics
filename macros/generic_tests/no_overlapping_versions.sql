{#
    Asserts SCD Type 2 integrity on a snapshot.

    A snapshot's validity windows should tile the timeline for each entity: no
    two versions overlapping, no gaps between them, and exactly one open record
    per entity that still exists in the source. dbt guarantees none of this --
    it maintains the columns, but nothing checks that the result is coherent,
    and the failure modes are silent. A snapshot with a non-deterministic source
    query (a LIMIT with no ORDER BY, a filter on a volatile column) will happily
    produce overlapping windows and duplicate open records, and every built-in
    test will still pass.

    Four failure modes, reported separately so the output says what is wrong
    rather than just that something is:

      overlapping_windows          a version stays valid past the start of the next
      gap_between_versions         a window closes before the next one opens
      invalid_open_version_count   an entity has no current version, or several
      valid_to_before_valid_from   a window closes before it opens

    Usage:

      - name: snap_movies
        tests:
          - no_overlapping_versions:
              entity_key: movie_id

    `hard_deletes='invalidate'` legitimately leaves an entity with zero open
    versions, so set allow_closed_entities: true on snapshots that use it.
#}

{% test no_overlapping_versions(
    model,
    entity_key,
    valid_from='dbt_valid_from',
    valid_to='dbt_valid_to',
    allow_closed_entities=false
) %}

WITH versions AS (

    SELECT
        {{ entity_key }} AS entity_key,
        {{ valid_from }} AS valid_from,
        {{ valid_to }}   AS valid_to,

        LEAD({{ valid_from }}) OVER (
            PARTITION BY {{ entity_key }}
            ORDER BY {{ valid_from }}
        ) AS next_valid_from,

        SUM(CASE WHEN {{ valid_to }} IS NULL THEN 1 ELSE 0 END) OVER (
            PARTITION BY {{ entity_key }}
        ) AS open_version_count

    FROM {{ model }}

),

violations AS (

    SELECT entity_key, valid_from, valid_to, 'overlapping_windows' AS failure_mode
    FROM versions
    WHERE next_valid_from IS NOT NULL
      AND valid_to > next_valid_from

    UNION ALL

    SELECT entity_key, valid_from, valid_to, 'gap_between_versions'
    FROM versions
    WHERE next_valid_from IS NOT NULL
      AND valid_to < next_valid_from

    UNION ALL

    SELECT entity_key, valid_from, valid_to, 'invalid_open_version_count'
    FROM versions
    WHERE open_version_count > 1
       {% if not allow_closed_entities %}
       OR open_version_count = 0
       {% endif %}

    UNION ALL

    SELECT entity_key, valid_from, valid_to, 'valid_to_before_valid_from'
    FROM versions
    WHERE valid_to IS NOT NULL
      AND valid_to < valid_from

)

SELECT * FROM violations

{% endtest %}
