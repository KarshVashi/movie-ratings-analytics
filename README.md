# MovieLens Analytics Engineering Pipeline

A layered dbt project on Snowflake — staging → dimensions/facts → marts — with SCD Type 2 snapshots, incremental models, source freshness monitoring, a custom generic test, CI on pull requests, and Airflow orchestration at per-model task granularity.

Built on the [MovieLens 20M](https://grouplens.org/datasets/movielens/20m/) dataset: 20,000,263 ratings, 465,564 tag applications and an 11.7M-row movie–tag relevance matrix across 27,278 films and 138,493 users.

**Docs and lineage graph:** *(add the GitHub Pages link here once published — `dbt docs generate --static`, then push to `gh-pages`)*

---

## Verified against the source

Every assertion this project makes about the data was checked against the raw CSVs rather than assumed. Claims about data are cheap to make and cheap to check, and the ones that turn out false are the interesting ones.

| Claim | Check | Result |
| --- | --- | --- |
| `fct_ratings` grain is `(user, movie)` | duplicate key pairs in `ratings.csv` | **0** of 20,000,263 — merge is safe |
| `rating` is never null | null ratings | **0** — so it is a test, not a `WHERE` clause |
| `relevance` is strictly positive | `relevance <= 0` | **0** of 11,709,768, min 0.00025 |
| Primary keys are unique | duplicate `movieId` / `tagId` / `tag` | **0**, **0**, **0** |
| `rating` is bounded [0.5, 5.0] | observed range | 0.5 – 5.0 — matches the bounds test |
| `seed_genre_groups` is complete | genre vocabulary in `movies.csv` | 20 distinct, **exact match**, no gaps or extras |
| Second-granularity ties are real | ratings sharing a timestamp | **34.7%** of rows |

Two of these changed the code rather than confirming it. **Every one of the 7,801 tagging users has also rated**, so a drafted `is_tag_only_user` flag would have been `false` for all 138,493 rows — a column constant by construction is noise in a dimension, so it went, while the defensive `UNION` behind it stayed. And **`release_year` is null for 23 films**, a few malformed rather than missing (`"... (1983))"` carries a stray bracket, `"Diplomatic Immunity (2009- )"` an en-dash range) — so the column's documentation names the number and the cause instead of hand-waving at "some".

### A principle this project holds to

**A filter that removes nothing is a test that never runs.** `WHERE rating IS NOT NULL` over this data would remove zero of 20,000,263 rows; `WHERE relevance > 0` would remove zero of 11,709,768. Both look like validation while asserting nothing at all, and both would keep looking like validation on the day the property stopped holding. Neither is in the models. Both are explicit tests instead, and tests can fail.

---

## Design decisions

### Materialisations

Set as a layer-level policy in `dbt_project.yml` rather than per model, so that each default is a stated position. Overrides carry their reasoning in the model file.

| Layer | Default | Why |
| --- | --- | --- |
| `staging` | view | Rename-and-cast only. Nothing worth storing, and no query reads staging directly. |
| `intermediate` | ephemeral | Inlined into its single consumer. Nothing queries it standalone. |
| `dim` / `fct` | table | Joined constantly by marts and by ad-hoc queries. |
| `marts` | table | Expensive to compute, small output, read repeatedly. |

Three models override the default:

**`fct_genome_scores` → view.** It applies a `ROUND()` and nothing else. Making it a table would store a second copy of 11.7M rows to hold a rounded float — storage bought for no gain. The expensive work in that branch is the flatten-and-aggregate in `mart_tag_genre_affinity`, and that is where the table lives. The model still earns its place: it declares, documents and tests the grain.

**`fct_ratings` → incremental.** Honestly: at 20M rows and ~120 MB compressed, **incremental is not justified by volume here**. A measured full rebuild of all 20,000,263 rows takes **3.0 seconds** on an XS warehouse at a single thread. Saying "it's incremental because the table is large" would not survive one follow-up question. It is incremental because of *shape* — an append-mostly event stream is the case incremental exists for — and because the project needed the simple form of the pattern to contrast with the harder one below. The point where it would start paying for itself is roughly two orders of magnitude up from here.

**`fct_movie_rating_daily` → incremental with a lookback window.** This is where incremental is load-bearing. A rolling 28-day measure is not a point event: a rating landing today changes the figure for each of the previous 27 days. A high-water-mark filter would freeze those days at whatever value they held when first computed, and the recent tail of the series would be permanently and silently wrong. So the window reprocesses and merges rather than appending — and it reads twice as far back as it writes, because computing a correct 28-day window for the oldest day being reprocessed needs the 28 days of history behind it. Reading only as far back as you write is the off-by-one-window bug that under-counts the oldest rows of every run.

### Grain

- `fct_ratings` — one row per **(user, movie)**, not per rating event. MovieLens permits one rating per user per film; a re-rating replaces rather than appends. That is what makes `merge` on `(user_id, movie_id)` correct, and it is asserted by `dbt_utils.unique_combination_of_columns` rather than assumed. Snowflake `MERGE` raises a duplicate-row error if the source carries duplicate keys, so that test is the difference between a clear failure and an opaque one on some later run.
- `mart_user_cohort_retention` — one row per **(cohort month, month offset)**, not per user. The question is the shape of the decay curve, and the aggregate is four orders of magnitude smaller than the user-level equivalent.
- `mart_tag_genre_affinity` — one row per **(genre, tag)**. ~22K rows out of a ~23M-row intermediate join.

Two details in the incremental filter on `fct_ratings` are worth spelling out, because the obvious version of each is wrong.

The boundary comparison is `>=`, not `>`. A strict `>` drops any row sharing the exact boundary second with the current maximum. The newest timestamp in this extract happens to be a singleton, so that bug would not fire on a first run — but **34.7% of all ratings share their second with another rating**, and 14.9% of distinct timestamps are ties, so roughly one boundary in seven would silently lose rows. A bug that needs the right boundary to appear is worse than one that always fires, not better.

And `>=` only works paired with `unique_key` and `merge`. Alone it would re-read the boundary second every run and duplicate it. Without it, the model would be append-only: an overlapping re-run duplicates rows, and a corrected rating could never overwrite the original. The two choices are a pair.

### Snapshots (SCD Type 2)

The obvious thing to snapshot in this dataset is the tag stream, and it is the wrong choice. A tag is "user U tagged movie M at time T" — an immutable event. Events do not slowly change, so there would be no history to capture and every row would sit at version one forever.

`snap_movies` versions movie metadata instead: titles get corrected and genres get reclassified, which is exactly the slow change SCD Type 2 exists for. It uses the `check` strategy, because `raw_movies` carries no `updated_at` column to run a timestamp strategy against — the strategy is forced by the source, not chosen from preference.

It snapshots **the source, not `dim_movies`**. If a snapshot reads a model, refactoring that model rewrites history: changing `INITCAP` to `UPPER` in `dim_movies` would record a change to all 27,278 films that never happened upstream. A snapshot has to sit above your own logic or it versions your logic rather than the data. The cost is that pure formatting churn in the source creates versions too — the right trade, because over-capture is filterable and lost history is not.

Because the source is a static 2015 extract, `scripts/02_simulate_source_changes.sql` stages deliberate, reproducible source drift — a genre reclassification, a title correction and a hard delete — so the snapshot has something to demonstrate. It is committed rather than run ad hoc, so the drift is visibly staged rather than mysterious.

### Testing

106 tests. The ones worth pointing at:

- **`no_overlapping_versions`** — a custom generic test written for this project. dbt maintains `dbt_valid_from` / `dbt_valid_to` but never checks that the result is coherent, and every way it can go wrong is silent. A snapshot whose source query is non-deterministic — a `LIMIT` with no `ORDER BY`, a filter on a volatile column — will happily produce overlapping windows and duplicate open records while every built-in test passes. The test asserts that validity windows tile the timeline per entity: no overlaps, no gaps, exactly one open record, no window closing before it opens. It reports which of the four failure modes fired rather than just that something did.
- **`n_ratings_28d >= n_ratings_7d`** — a relationship between columns, not a range on one. A 28-day window must contain at least as much as the 7-day window it encloses. This catches frame-definition errors, the failure mode the lookback design is most exposed to, which no column-level test would surface.
- **Source-level tests**, so key violations are caught at the boundary rather than three models downstream.

### Sources and freshness

Freshness is measured from **warehouse load metadata**, not from an event timestamp. The newest rating in MovieLens is from 2015, so event time would report a permanent multi-year staleness and a genuinely broken loader would look identical to a healthy one. Load recency is the property worth alerting on for a batch-delivered source.

Thresholds are sized to a monthly cadence. The genome tables have freshness **disabled outright** rather than loosened — MovieLens regenerates them on a multi-year cycle, and an alert nobody can act on trains people to ignore alerts.

Staging reads through `{{ source() }}` rather than addressing `MOVIELENS.RAW.*` directly. Hardcoding the three-part name compiles to the same SQL but costs three things: the raw layer disappears from the lineage graph, freshness becomes impossible to declare, and repointing at another environment turns into an edit across five SQL files instead of a config change.

### Environment separation

Three targets — `dev` (DEV), `prod` (PROD), `ci` (a per-pull-request schema, dropped when the run ends). Sharing one schema across all three means an in-progress model on a laptop overwrites the table a dashboard is reading, and a failed run leaves prod half-built with no way to tell which half is stale.

Credentials come from environment variables, so no password lives in a config file. `threads` is 8: the staging layer is five mutually independent branches and there is no reason for the warehouse to sit idle while they run one at a time.

---

## CI

`.github/workflows/dbt_ci.yml` runs `dbt build` on every pull request against a schema named for the PR, and drops it afterwards with `if: always()` so a red build still cleans up after itself.

Two decisions worth naming:

**Source freshness runs but does not block.** A stale source is a data problem, not a problem with the code under review. Blocking a reviewer on it trains people to merge past red CI. It belongs on the orchestration schedule, where someone can act on it.

**`drop_ci_schema` refuses to run against any target but `ci`.** It issues `DROP SCHEMA … CASCADE`, and the difference between running it against `ci` and against `prod` is the entire warehouse. A forgotten `--target` should fail loudly rather than execute.

**Slim CI** (`state:modified+` with deferral) is deliberately deferred rather than forgotten. It needs a production manifest published somewhere CI can fetch it, and at sixteen nodes a full build takes long enough to be worth less than the complexity. The threshold where that flips is a few hundred models.

---

## Orchestration

Airflow via Astronomer Cosmos — see [`orchestration/`](orchestration/). Cosmos parses the dbt manifest and emits **one Airflow task per model**, so retries are per-model, the graph view shows real lineage, and a failure names the model that failed rather than saying `dbt_build failed`.

A single `BashOperator` running `dbt build` also works, and is the wrong shape: a Snowflake timeout on the last mart forces a retry of the whole DAG, re-running a 20M-row incremental that had already succeeded.

Scheduled `@monthly`, matching the source load cadence.

---

## What breaks at 100× volume

At 2B ratings rather than 20M:

- **`fct_ratings` becomes genuinely incremental-dependent** rather than incremental-by-choice, and the merge starts to hurt — merging into a 12 TB table rescans far more than it writes. It would need clustering on `rating_timestamp`, and probably `delete+insert` on a partition boundary rather than `merge` on a two-column key.
- **`dim_users` stops being cheap.** It aggregates the full ratings history on every run to derive `n_ratings` and `first_rating_at`. At 100× that is a full-table scan per build, and it would have to become incremental itself, maintaining running aggregates rather than recomputing them.
- **`mart_user_cohort_retention` is the first thing to fall over.** `COUNT(DISTINCT user_id)` across a 2B-row join does not scale gracefully. It would move to an approximate distinct count, or to a pre-aggregated user-month bridge table.
- **`int_movie_tag_relevance` stays fine** — the genome matrix does not grow with rating volume. The branch that looks most expensive is the one least affected.
- **`threads: 8` stops being the constraint** and warehouse size becomes it.

## What I would do differently

- **`fct_movie_rating_daily`'s lookback is hard-coded at 28 days.** It should be a project variable, so backfills can widen it without editing the model. Left as-is because a `var()` with no second caller is speculative generality.
- **The genre taxonomy in `seed_genre_groups` is mine, and it is arguable.** Putting `Horror` under Speculative rather than in its own group is a judgement call I would want a domain owner to make in a real setting. It is a seed precisely so that argument happens in a pull request.
- **`dim_movies` parses the release year with a regex against a title string.** It works because the format is consistent, but it is fundamentally reconstructing a field that should have been delivered separately. In production I would push back on the source rather than parse.
- **No incremental predicate on `fct_ratings`' merge.** Snowflake would benefit from an explicit `incremental_predicates` clause to prune the target scan. Skipped because at this volume it is unmeasurable, and unmeasurable optimisations are how projects accumulate cargo cult.

## Deliberately out of scope

Model contracts, model versions, and dbt unit tests. All defensible additions; none of them changes what this project demonstrates, and each would have cost time better spent on CI and orchestration.

---

## Running it

```bash
pip install -r requirements.txt
dbt deps

export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
export DBT_PROFILES_DIR=./.dbt

dbt build                    # seeds, models, snapshots, tests
dbt source freshness
dbt docs generate --static
```

Project layout:

```text
models/
  staging/        stg_*      views over sources, rename and cast only
  intermediate/   int_*      ephemeral, single-consumer
  dim/            dim_*      conformed dimensions
  fct/            fct_*      facts, two of them incremental
  marts/          mart_*     analytical outputs
snapshots/        snap_movies (SCD Type 2 against the source)
macros/
  generic_tests/  no_overlapping_versions
  drop_ci_schema.sql
seeds/            seed_genre_groups
scripts/          staged source mutations for the snapshot demonstration
orchestration/    Airflow + Cosmos
.github/workflows/dbt_ci.yml
```
