# Cosmos Proposal 003: Correlate dbt microbatch batches with Airflow asset partitions

**Status:** Draft - request for comments

**Author:** [Tatiana Al-Chueyr](https://github.com/tatiana)

**Created:** 2026-09

**Discussion:** Link to GitHub discussion or issue once opened

**Depends on:** Airflow 3.3 (`partition_date` on the task-SDK `DagRun`) and dbt-core >= 1.9
(microbatch). A prerequisite spike on dbt Fusion is required before the supported range can be
stated - see Open questions.

## Summary

dbt's [microbatch incremental strategy](https://docs.getdbt.com/docs/build/incremental-microbatch)
and [Airflow asset partitions](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html#asset-partitions)
describe the same idea from two sides. A dbt model configured with `event_time` and `batch_size`
(`hour`/`day`/`month`/`year`) is processed in bounded time batches. An Airflow asset partition is a
key attached to asset events, used for partition-aware downstream scheduling.

This proposal correlates them as **one Airflow partition per DAG run, one dbt batch per partition**.
A partitioned timetable produces one DAG run per grain, and Cosmos derives dbt's
`--event-time-start` / `--event-time-end` from that run's partition, so the batch dbt builds and the
partition Airflow records are the same interval.

```
DAG run (partition 2026-03-10T09:00:00, hourly)
  -> dbt run --event-time-start 2026-03-10T09:00:00
             --event-time-end   2026-03-10T10:00:00   (exclusive)
  -> asset event inherits the run's partition_key
```

Asset emission needs no change: Airflow already stamps a task's asset events with the run's
partition key, including through the `AssetAlias` path Cosmos uses. The proposal is therefore
entirely about the dbt input side, plus the validation needed to keep the two halves aligned.

It is opt-in, off by default, and scoped to the execution modes that run one dbt command per model.

## Motivation

Cosmos has no microbatch awareness today: no reference to `event_time`, `batch_size`,
`incremental_strategy` or a run's data interval anywhere in `cosmos/`. The only existing support is a
docs recipe using templated `dbt_cmd_flags`
(`docs/guides/run_dbt/operators/operator-args.rst:160-183`), which is `params`-driven, hardcodes a
single `--select`, and therefore fights the one-task-per-model model that `DbtDag` exists to provide.

More importantly, that recipe cannot be made correct. `dbt_cmd_flags` applies to **every** task in
the DAG, and only `dbt run` and `dbt build` accept the event-time flags, so seed, test, snapshot and
source-freshness tasks would receive an option `click` rejects and fail outright. A user wanting
partition-aligned microbatch runs today must either abandon per-model tasks or hand-maintain a
separate DAG per model.

Meanwhile a user who does get the window right gets no correlation: Airflow records a partition key
and dbt builds a batch, with nothing guaranteeing they describe the same interval.

## Current state (verified against Airflow 3.3.1, dbt-core 1.12.4 and Cosmos 1.15.1)

Read from the installed and tagged sources rather than the published documentation, which is
inaccurate on two of the points below.

### What Airflow provides

- **Asset events already inherit the run's partition key.** `partition_key = ti.dag_run.partition_key`
  (`airflow/models/taskinstance.py:1439` on 3.2, `:1557` on 3.3) is threaded into every
  `register_asset_change` call, **including the alias branch Cosmos uses** (`:1567` / `:1705-1723`).
  So a Cosmos DAG under a partitioned timetable emits partitioned events with no code change.
- **3.3 adds `partition_date`, which 3.2 lacks.** The task-SDK `DagRun` (declared
  `airflow/sdk/api/datamodels/_generated.py:756`) carries `partition_key` (`:778`) and
  `partition_date: AwareDatetime | None` (`:779`). On 3.2 only the string key exists (`:112`), it is
  absent from the public `DagRunProtocol` (`airflow/sdk/types.py:95-108`), and `partition_date`
  appears nowhere but a docstring.
- **3.3 adds the runtime partition API, but Cosmos cannot use it.** `PartitionedAtRuntime`,
  `add_partitions` and the window and mapper classes are exported in 3.3 and absent from 3.2.
  However `add_partitions` "raises TypeError ... if this accessor is for an asset alias, since
  partition keys are only attached to concrete asset events, not alias events"
  (`task-sdk/src/airflow/sdk/execution_time/context.py:979`), restated in
  `taskinstance.py:1547-1557`: *"Per-emission partition keys do not fan out through ... resolved
  asset, all carrying the same dag_run_partition_key."* Cosmos emits only through `AssetAlias`
  (`cosmos/dataset.py:472-479`).
- **A timetable's `partitioned` flag is invisible to a DAG author.** `partitioned: bool = False`
  lives on the scheduler-side timetable (`airflow/timetables/base.py:214`);
  `CronPartitionTimetable(...).partitioned` raises `AttributeError` on the SDK object a DAG file
  holds. `PartitionedAssetTimetable` does set `partitioned = True` scheduler-side
  (`airflow/timetables/simple.py:254`), so its DAG runs carry partition keys.
- **`CronPartitionTimetable` exposes `expression` and `timezone`** on the SDK object, which makes
  cadence and timezone validation possible at DAG-parse time.

### The dbt microbatch contract

Byte-identical in dbt-core 1.9.0 (microbatch's first release), 1.11.8 and 1.12.4, so the contract has
not moved since microbatch shipped. Two facts contradict dbt's own documentation:

- `--event-time-start` is **inclusive** and `--event-time-end` is **exclusive**
  (`dbt/cli/params.py:238,230`). The published docs table says the end is inclusive.
- Both flags are `type=click.DateTime()`, so only `%Y-%m-%d`, `%Y-%m-%dT%H:%M:%S` and
  `%Y-%m-%d %H:%M:%S` parse. An offset, a `Z`, or fractional seconds is rejected, which rules out a
  bare `datetime.isoformat()`.
- dbt **re-labels naive input as UTC**
  (`dbt/materializations/incremental/microbatch.py:36-38`).
- dbt **truncates the start and ceilings the end** to the grain (`microbatch.py:59` and `:44`). An
  off-boundary value silently widens the run: an hourly start of `09:15` with an end of `10:15`
  becomes `[09:00, 11:00)`, two batches rather than one.
- dbt records per-batch outcomes: `run_results.json` v5 carries per-node `batch_results` with
  `successful` and `failed` as `(datetime, datetime)` tuples
  (`dbt/artifacts/schemas/batch_results.py`).
- The four env aliases are `DBT_EVENT_TIME_START`, `DBT_EVENT_TIME_END`,
  `DBT_ENGINE_EVENT_TIME_START` and `DBT_ENGINE_EVENT_TIME_END`. The engine-prefixed pair is derived
  rather than literal: `_create_option_and_track_env_var` "ensures that all options with env vars ...
  have a `DBT_ENGINE_` prefixed env var" (`params.py:55-70`).
- **`click` resolves the command line before the environment.** `consume_value` sets
  `source = COMMANDLINE` and consults `value_from_envvar` only `if value is None`
  (`click/core.py:2280-2296`). An injected flag therefore overrides any env var.

### Only `dbt run` and `dbt build` accept the window

`grep event_time_start dbt/cli/main.py` matches exactly two commands: `build` (`main.py:175`) and
`run` (`:567`).

### What Cosmos knows, and when

- **At render time**, `DbtNode.config` holds the manifest config unfiltered
  (`cosmos/dbt/graph.py:110-127`, assigned at `:490`), so `event_time`, `batch_size`, `begin`,
  `lookback` and `incremental_strategy` are all reachable. Two loaders hide it: `LoadMode.CUSTOM`,
  whose whitelist is `["materialized", "schema", "tags"]` (`cosmos/dbt/parser/project.py:45`,
  consumed `:214-222`), and `pre_dbt_fusion` with `SourceRenderingBehavior.NONE`, where `dbt ls` runs
  without `--output-keys` (`cosmos/dbt/graph.py:828-859`).
- **`LoadMode.AUTOMATIC` can resolve to CUSTOM.** With no manifest available it falls back to
  `load_via_custom_parser()` on a `dbt ls` `FileNotFoundError` or when there is no profile
  (`cosmos/dbt/graph.py:803,812`), which sets `self.load_method = LoadMode.CUSTOM` (`:1105`) while
  `render_config.load_method` stays `AUTOMATIC`.
- **At execute time**, the operator holds both the dbt config, via
  `extra_context["dbt_node_config"]["config"]` (`cosmos/dbt/graph.py:321`, populated
  `cosmos/airflow/graph.py:405-408`, merged `cosmos/operators/base.py:354-356`), and the live
  `dag_run`.
- **`build_cmd` is the flag funnel** (`cosmos/operators/base.py:310-341`) for the modes that run one
  dbt command per model, but not for all modes: see constraint 4.

## Constraints and goals

1. **Opt-in and backwards-compatible.** Off by default via a new `cosmos` setting. Nothing changes for
   any existing DAG, and a DAG that does not opt in is never validated or rejected.
2. **One grain per DAG, enforced.** A timetable has a single grain, so an hourly timetable would run a
   daily model's window every hour and a daily timetable would skip 23 hourly batches. Mixed grains
   raise at conversion rather than producing silently wrong data.
3. **Fail closed, never fail open.** Activation must not be inferred at runtime from the presence of a
   partition key, because `PartitionedAssetTimetable` and any plugin-registered partition timetable
   would then activate without passing validation. Conversion decides eligibility and stamps approval;
   the runtime acts only on the stamp.
4. **Scoped to per-model command execution modes.** LOCAL, VIRTUALENV, DOCKER, KUBERNETES, EKS, GKE,
   ECS, ACI and CLOUD_RUN. Excluded, and rejected at conversion: all three watcher modes
   (`WATCHER`, `WATCHER_KUBERNETES`, `WATCHER_GCP_GKE`, grouped at `cosmos/airflow/graph.py:1246`),
   whose producer runs one combined `dbt build`; and `AIRFLOW_ASYNC` **when**
   `settings.enable_setup_async_task` is true, where the task executes precompiled SQL and never
   builds a dbt command (`cosmos/operators/_asynchronous/bigquery.py:207-224`). With that setting off,
   `AIRFLOW_ASYNC` falls through to `build_and_run_cmd` (`bigquery.py:222`) and is supported.
5. **UTC timetables only.** A timezone-aware key converts cleanly, but the converted instant is not a
   grain boundary: London midnight in July is `23:00Z` on the preceding day, which dbt's daily
   truncation floors to `00:00Z` of *that* day (`microbatch.py:185-188`). `month` and `year` fail the
   same way; `hour` survives only because common offsets are whole hours.
6. **One partition must mean exactly one batch.** Both alignment and cadence are validated, since an
   aligned key under `0 */2 * * *` passes alignment while silently processing alternate batches.
7. **Never two sources for one window.** A user-supplied window and an injected one must not coexist
   silently. Detection is best-effort where the environment is not statically inspectable; see
   Edge cases.
8. **No new Cosmos dependencies.** Cosmos imports neither `croniter` nor `dateutil` today
   (`types-python-dateutil` appears only in the type-check extras, `pyproject.toml:169`), and relying
   on Airflow's transitive dependencies would be an unstated contract.

## Proposed design

### 1. Eligibility decided once at conversion

Validated next to `validate_arguments` (`cosmos/converter.py:395`), which runs after
`dbt_graph.load()`:

```python
def _microbatch_grain(
    self, dag, task_group, render_config, execution_config
) -> str | None:
    """Return the grain to stamp, or None to stamp nothing. Raises only when a microbatch
    model is actually selected AND the DAG is a supported partition producer."""
    if not settings.enable_microbatch_event_time:
        return None
    if AIRFLOW_VERSION < Version("3.3"):
        return None
    # Must precede the `selected` gate: under either loader the config is invisible, so
    # `selected` is falsely empty and any later check would be unreachable. Uses the
    # *resolved* load method, since AUTOMATIC can fall back to CUSTOM.
    if _config_is_invisible(self.dbt_graph.load_method, render_config):
        return None  # warns
    selected = [
        n
        for n in self.dbt_graph.filtered_nodes.values()
        if n.config.get("incremental_strategy") == "microbatch"
    ]
    if not selected:
        return None
    effective_dag = dag or (task_group and task_group.dag)  # as at converter.py:408
    from airflow.sdk import CronPartitionTimetable  # 3.3-guarded import

    tt = getattr(effective_dag, "timetable", None)
    if not isinstance(tt, CronPartitionTimetable):
        return None  # unpartitioned, PartitionedAssetTimetable, or unknown
    _reject_unsupported_execution_mode(execution_config)
    grain = _single_grain(selected)  # raises on mixed or invalid
    _require_utc(tt)
    _require_exact_cadence(tt.expression, grain)
    return grain
```

Both configs are parameters because `DbtToAirflowConverter.__init__` stores only `self.dbt_graph`
(`cosmos/converter.py:352`); the call site has both in scope.

`isinstance` rather than duck typing, so a structurally similar custom timetable is not silently
approved. `effective_dag` matters because `DbtTaskGroup` passes itself as `task_group` and leaves
`dag` as `None` (`cosmos/airflow/task_group.py:23`).

The invisible-config loaders **warn and return `None`** rather than raising, because under them
Cosmos cannot tell whether a microbatch model is present, so raising would penalise DAGs that have
none.

### 2. Cadence validated by canonical shape

Sampling fire times cannot be made deterministic: it needs an arbitrary starting instant and
occurrence count, and a cron can match one-grain intervals over the sampled window and diverge
later. Each grain therefore accepts exactly one expression:

| grain | accepted expression |
|---|---|
| `hour` | `0 * * * *` |
| `day` | `0 0 * * *` |
| `month` | `0 0 1 * *` |
| `year` | `0 0 1 1 *` |

The literals are pinned to zero rather than left as free values. A shape like `M H * * *` would admit
`30 9 * * *`, a valid daily cadence whose every partition is 09:30 UTC and therefore fails alignment
on every run. Pinning them makes cadence and alignment consistent by construction. This is
deliberately strict, rejecting equivalents such as `0 0 1 */12 *` for a yearly grain; widening it
later is additive.

### 3. Threading the grain into the tasks

`build_airflow_graph` (`cosmos/airflow/graph.py:1180-1193`) gains a `microbatch_grain` parameter,
passed from the converter's call at `:406` and forwarded to `create_task_metadata`, which stamps

```python
extra_context["microbatch_event_time"] = {"batch_size": grain}
```

alongside the existing keys (`cosmos/airflow/graph.py:405-408`), only when the grain is set, the
node's `incremental_strategy` is `microbatch`, and the task is a run or build task. That last
condition is already computed there by `resource_suffix_map` (`:410-412`), so seed, test, snapshot and
source-freshness tasks are never stamped at all.

### 4. Runtime injection

A helper on `AbstractDbtBase`, appended in `build_cmd` after the user-supplied block
(`cosmos/operators/base.py:333-337`):

```python
def _microbatch_event_time_flags(self, context: Context) -> list[str]: ...
```

It returns `[]` silently for genuine non-cases: no stamp; a subcommand that is not `run` or `build`
(a backstop for the parse-time condition); or an unpartitioned run.

With a stamp and a partition, it raises `CosmosValueError` rather than degrading, because a silent
skip means dbt processes its own default window while Airflow labels the event with an unrelated
partition key:

- `partition_date` is `None` while `partition_key` is set, i.e. a non-temporal partition such as
  `SegmentWindow(["us", "eu"])`;
- `partition_date` is not aligned to the stamped grain, which per the truncate and ceiling behaviour
  would quietly produce two batches;
- the user already supplied a window, per section 5.

Otherwise it emits both flags together, since dbt rejects one without the other:

```
["--event-time-start", fmt(start), "--event-time-end", fmt(start + one_grain)]
```

with `start` the UTC-converted `partition_date` and `fmt` producing `%Y-%m-%dT%H:%M:%S`. Stepping one
grain from an already-aligned boundary is exact with the standard library, `timedelta` for
`hour`/`day` and month or year increment with rollover otherwise, so constraint 8 holds and cases such
as "31 January plus one month" cannot arise. The exclusive end bound and the naive-to-UTC re-labelling
are the two points worth a code comment, since neither is inferable and one contradicts dbt's docs.

The injected window is logged at info, so the effective interval is visible in task logs.

### 5. Detecting a user-supplied window

Detection covers both token forms in `dbt_cmd_flags`, `--event-time-start value` and
`--event-time-start=value`, since `click` accepts either; all four env names in the `get_env` result
(`cosmos/operators/base.py:214-236`); and each mode's declared environment channels via an overridable
`_declared_env_var_names() -> set[str]`, empty on `AbstractDbtBase`. Names rather than a mapping,
because several Kubernetes channels are `V1EnvVar` objects.

Container modes overlay their own user-controlled fields after `build_cmd` returns, and the channels
split into those Cosmos can read and those it cannot:

**Directly named, detectable**

| mode | channels | overlay site |
|---|---|---|
| Docker | `environment`, `private_environment` | `cosmos/operators/docker.py:120` |
| Kubernetes, GKE, EKS | `env_vars`, `pod_runtime_info_envs`; `secrets` entries with `deploy_type="env"` and a `key`, where `deploy_target` names the variable; each container's `env` inside `full_pod_spec`, `pod_template_file`, `pod_template_dict` | `cosmos/operators/_k8s_common.py:168` |
| ECS, ACI, Cloud Run | `environment_variables` | `aws_ecs.py:137`, `azure_container_instance.py:128`, `gcp_cloud_run_job.py:148` |

**References, not detectable**

| mode | channels | why |
|---|---|---|
| Docker | `env_file` | the path may exist only inside the image |
| Kubernetes, GKE, EKS | `env_from`, `configmaps`, keyless `secrets`, `envFrom` in a pod spec | contents live in the cluster; a keyless `Secret` "will mount all secrets in object" (`airflow/providers/cncf/kubernetes/secret.py:46`) |
| ECS | task-definition environment | lives in AWS |

### 6. The setting

In `cosmos/settings.py` beside the other feature gates (`:41-48`):

```python
enable_microbatch_event_time = conf.getboolean(
    "cosmos", "enable_microbatch_event_time", fallback=False
)
```

Opt-in rather than opt-out because, unlike the dataset settings, it changes the dbt command rather
than caching a result.

### 7. Node plumbing, useful independently

- `event_time` and `batch_size` properties on `DbtNode` reading `self.config`, following
  `has_ephemeral_materialization` (`cosmos/dbt/graph.py:149-152`).
- `incremental_strategy` added to `SUPPORTED_CONFIG` (`cosmos/dbt/selector.py:24`) plus a branch at
  `:305-314`, so `select=["config.incremental_strategy:microbatch"]` stops being rejected.

## Alternatives considered

**A documentation recipe with templated `dbt_cmd_flags`.** Requires no Cosmos code, since
`dbt_cmd_flags` is already a template field. Rejected because it cannot be made correct: the flags
would reach seed, test, snapshot and source-freshness tasks, which `click` rejects, and it cannot
compute an exclusive end bound or per-model grain. It also leaves the user hand-deriving a window
whose upper bound is exclusive.

**Per-batch emission: one run spanning a window, one partitioned event per successful batch.** A
closer match to dbt's own model, and both halves of the data exist: `batch_results.successful`, and
`add_partitions` on Airflow 3.3. Rejected for now because `add_partitions` raises on an alias accessor
and Cosmos emits only through `AssetAlias`, so it would require emitting concrete assets and breaking
every user scheduling on `AssetAlias(...)`. See Roadmap.

**Detecting the dbt engine at runtime to gate Fusion.** Rejected because no signal is trustworthy:
the version import at `cosmos/operators/local.py:554` runs at task execution and only under
`InvocationMode.DBT_RUNNER`; a subprocess `dbt_executable_path` may differ from the scheduler's
importable package; VIRTUALENV installs dbt during execution
(`cosmos/operators/virtualenv.py:113-118`); and `settings.pre_dbt_fusion` is a compatibility switch,
not an engine declaration. A guard on an untrustworthy signal is worse than a stated limitation.

## Changes by module (`astronomer-cosmos`)

| Module | Change |
|---|---|
| `cosmos/settings.py` | new `enable_microbatch_event_time`, default `False` |
| `cosmos/converter.py` | compute the grain next to `validate_arguments` (`:395`); pass `microbatch_grain` to `build_airflow_graph` (`:406`) |
| `cosmos/airflow/graph.py` | `microbatch_grain` parameter on `build_airflow_graph` (`:1180`); stamp `extra_context` in `create_task_metadata` (`:405-408`) |
| `cosmos/operators/base.py` | `_microbatch_event_time_flags`, `_declared_env_var_names`, call in `build_cmd` (`:333-337`) |
| `cosmos/operators/docker.py`, `_k8s_common.py`, `aws_ecs.py`, `azure_container_instance.py`, `gcp_cloud_run_job.py` | `_declared_env_var_names` override per mode |
| `cosmos/dbt/graph.py` | `event_time` and `batch_size` properties on `DbtNode` |
| `cosmos/dbt/selector.py` | `incremental_strategy` in `SUPPORTED_CONFIG` |
| `docs/` | rewrite the microbatch recipe; document the rules, unsupported modes and the Fusion position |
| `dev/dags/dbt/` | a single-grain microbatch project, and a mixed-grain project used only as a rejection fixture |

## Edge cases and risks

1. **`run_offset` shifts the partition off the cron boundary.** `CronPartitionTimetable` accepts it, so
   the runtime alignment check stays as a backstop even though the pinned cron shapes make cadence and
   alignment consistent.
2. **Undetectable environment channels.** Per the table in section 5, a window arriving from a
   ConfigMap, Secret, ECS task definition or in-image `env_file` is not detected. The mitigation is
   `click`'s precedence: the injected flag overrides the env var, so the hidden value is ignored rather
   than silently governing the window. The residual risk is user surprise, addressed by logging the
   injected window.
3. **Invisible-config loaders.** Under `LoadMode.CUSTOM` (including via `AUTOMATIC` fallback) and
   `pre_dbt_fusion` with `SourceRenderingBehavior.NONE`, Cosmos cannot see `incremental_strategy`, so
   the feature is inactive and warns. A test distinguishing "that loader with a microbatch model" from
   "without one" is impossible from the graph data, which is why these warn rather than raise.
4. **No fixtures exist.** No `dev/dags/dbt/` project declares `event_time`, and no
   `tests/sample/manifest*.json` contains it; the newest is dbt 1.8.7, which predates microbatch.
5. **Matrix coverage.** Microbatch needs dbt >= 1.9, and the matrix is
   `["1.8", "1.9", "1.10", "1.11", "1.12", "2.0"]` (`pyproject.toml:183`), so tests must exclude the
   1.8 row and cap at `<2.0`. The `2.0` row is Fusion, which lacks the postgres adapter every
   `dev/dags` project uses (`scripts/test/pre-install-airflow.sh:103-107`).
6. **Local environments.** The primary `venv` is Airflow 3.2.0, so a 3.3 environment must be built
   before any end-to-end check.

## Testing

Run through the hatch scripts named in `AGENTS.md`, not raw pytest.

1. `_microbatch_event_time_flags` units: each silent-skip case; each raising case, including an
   off-boundary `partition_date` such as `09:15` on an hourly model, and a non-temporal partition where
   `partition_date` is `None` but `partition_key` is set; and all four grains, asserting month and year
   land on calendar boundaries and the output carries no offset, `Z` or fractional seconds.
2. User-window detection: both token forms in `dbt_cmd_flags`; all four env names through each
   `get_env` layer, the process environment with `append_env=True`, the operator `env`, and an
   interceptor mutating `self.env`; one test per **directly named** container channel; and mirror tests
   asserting the **reference** channels are not detected while injection still happens.
3. Conversion tests, each for **both `DbtDag` and `DbtTaskGroup`**: mixed grains raise naming both
   models; all three watcher modes raise; `AIRFLOW_ASYNC` raises only when
   `settings.enable_setup_async_task` is true; a non-UTC timetable raises naming the timezone.
4. Cadence: the four accepted expressions; rejections naming the accepted expression for
   `0 */2 * * *` and `0 0 * * *` on `hour`, `30 * * * *` on `hour`, `30 9 * * *` and `0 9 * * *` on
   `day`, `0 0 15 * *` on `month`, `0 0 1 7 *` on `year`, and `0 0 1 */12 *` on `year`.
5. Fail-closed: a DAG on `PartitionedAssetTimetable` is not stamped and its command is unchanged even
   though its runs carry partition keys; likewise an unrecognised timetable.
6. No-op: a DAG with no selected microbatch node is never rejected, even with a watcher mode or a
   non-UTC timetable; an unpartitioned DAG with mixed grains parses cleanly; any DAG on Airflow below
   3.3 is untouched.
7. Loaders: explicitly requested `LoadMode.CUSTOM`, and `AUTOMATIC` falling back to CUSTOM, and
   `pre_dbt_fusion` with `SourceRenderingBehavior.NONE`, each warning and stamping nothing.
8. Regression: a non-microbatch model, a seed or test task, and an unpartitioned run all produce an
   unchanged command.
9. End to end on Airflow 3.3 with `schedule=CronPartitionTimetable("0 0 * * *", timezone="UTC")` over
   the microbatch project. Use a **daily** grain, since an hourly run would pass even with the
   timezone handling broken. Assert the rendered command, then that `batch_results.successful` in
   `run_results.json` has length 1, which is the real proof the alignment worked.
10. Downstream: a DAG on `PartitionedAssetTimetable` with a matching mapper fires once per partition,
    and a plain `schedule=[AssetAlias(...)]` DAG does **not** fire, per Airflow's documented behaviour.

## Roadmap (additive, opt-in slices)

1. **Node plumbing** (section 7). No behaviour change, useful on its own for selection.
2. **The Fusion spike** (Open questions). A prerequisite, since it sets the documented support range.
3. **This proposal**: setting, conversion-time eligibility, stamping, injection, docs, fixtures.
4. **`PartitionedAssetTimetable` support**, so a Cosmos project can consume another partitioned asset.
   Fail-closed today; needs its own grain-mapping story.
5. **Per-batch emission**, once the alias-to-concrete-asset migration has a story. This is also the
   design that would serve the watcher modes, whose producer runs one combined command.

## Open questions (for community input)

1. **dbt Fusion.** dbt-core 2.0 is Fusion, and PyPI has no stable 2.0, only up to `2.0.0rc2`. None of
   the contract facts above are verified for it, since they come from the Python source Fusion does not
   share. The spike needs no warehouse: install `2.0.0rc2` in a throwaway venv and check the two help
   strings for inclusivity, whether an offset-bearing or fractional-seconds value is accepted, and
   whether the flags remain `run`/`build` only. Only truncate and ceiling need a real run. Until it
   runs, the documented range is dbt-core 1.9 to 1.12 on the Python engine, with no detection
   attempted.
2. **Is the pinned cron shape too strict?** It rejects legitimate equivalents. Should the accepted set
   be widened, and if so on what deterministic rule?
3. **Should `lookback` be honoured?** dbt widens an incremental run by `lookback` batches. A partition
   deliberately names one batch, so the two ideas conflict; this proposal ignores `lookback` and does
   not warn when it is set.
4. **Is UTC-only acceptable long term,** or should Cosmos support an offset-bearing `key_format`
   override? Note a fixed offset still cannot express a region's DST rules, so it would not fix
   calendar grains across a DST boundary.
