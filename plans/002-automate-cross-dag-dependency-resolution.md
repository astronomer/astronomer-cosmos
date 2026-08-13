# Cosmos Proposal 002: Automate dependency resolution across a dbt project split into multiple DbtDags or DbtTaskGroups

**Status:** Draft - request for comments

**Author:** [Tatiana Al-Chueyr](https://github.com/tatiana)

**Created:** 2026-06

**Discussion:** [astronomer-cosmos#1321](https://github.com/astronomer/astronomer-cosmos/issues/1321)

**Tracking ticket:** BOSS-269 (internal Linear ticket, "Automate the dependency resolution between DbtDags that represent sub-parts of a dbt project")

**Depends on:** [astronomer-cosmos#2959](https://github.com/astronomer/astronomer-cosmos/issues/2959) (unify Asset/Dataset URI construction across execution modes, including making relation identity available independent of `LoadMode`) and [astronomer-cosmos#2960](https://github.com/astronomer/astronomer-cosmos/issues/2960) (fix the `dbt ls` cache key so it doesn't silently serve stale node data once #2959 changes what Cosmos requests). This proposal is not scoped as manifest-only - see constraint 7.

## Summary

Cosmos maps a dbt project (or a `select`/`exclude` subset of it) 1:1 into one Airflow `DbtDag` or
`DbtTaskGroup` and resolves dependencies *within* that unit from `manifest.json`'s `depends_on`
graph. It does **not** resolve dependencies when a project is split across **multiple** units, so
cross-boundary edges are lost and users hand-wire them today (`dev/dags/cross_project_*.py` shows the
manual pattern).

This proposal adds a mode-independent, parse-time **structural boundary resolver** that computes, for
any selected subset of a dbt project, which of its parent nodes live outside the subset - using only
`unique_id`/`depends_on`/`resource_type`/`config`, fields available under every `LoadMode` today. A
separate, optional **URI-enrichment step** resolves those external parents to Asset/Dataset URIs, which
needs relation identity (manifest today, `dbt ls` once a dependency lands - see F1/constraint 7). Two
consumers build on the structural resolver, only one of which needs URI enrichment:

1. **Separate `DbtDag`s (ticket-primary):** a new `RenderConfig.auto_schedule: DbtUpstreamUpdated`
   option (`AND`/`OR`) that injects the resolved external-upstream Assets into the DAG's `schedule`,
   extending rather than replacing any schedule the user already set. Needs both the structural resolver
   and URI enrichment.
2. **Separate `DbtTaskGroup`s in one DAG (additive):** a `DbtDependencyCoordinator` that wires direct
   `producer_task >> consumer_task` edges across groups. This path is execution-mode-independent, does
   not touch `schedule` at all, and needs only the structural resolver - no URI, no manifest, no
   relation identity, and so no dependency on #2959/#2960.

Five feasibility constraints (F1-F5, below) gate this design and are folded into the roadmap and open
questions: relation identity (needed only for URI enrichment) is only available via a manifest today,
`schedule` is consumed before Cosmos parses the dbt graph, ephemeral/source nodes can silently break
scheduling if not handled explicitly, LOCAL/VIRTUALENV Asset emission is conditional on OpenLineage
rather than guaranteed (and most non-WATCHER, non-LOCAL/VIRTUALENV execution modes cannot emit an Asset
at all), and the resolver cannot verify at parse time that a matching producer exists, will emit, or
resolves to the same URI namespace - a gap that is structurally unfixable for a standalone `DbtDag`
without some form of peer visibility, but does not affect the `DbtTaskGroup` coordinator, which already
receives its peers explicitly.

## Motivation

The ticket's motivating case: one team owns the seeds (`raw_*`, refreshed hourly), another owns the
downstream transforms; the org wants one DAG per team, with the transforms DAG triggered once the
seeds are ready. Today that means hand-writing
`schedule=[Dataset("postgres://0.0.0.0:5432/postgres.public.raw_customers"), ...]` on the downstream
DAG. Secondary goals once cross-DAG dependencies are automatic: assign different schedules, owners,
and credentials per subset of one project, while Cosmos keeps owning the inter-DAG dbt dependency
wiring.

This proposal covers one topology - one dbt project, one manifest, partitioned by `select`/`exclude`
across units (the ticket's example: same `project_config`, different `select`) - split two ways:

- Multiple `DbtTaskGroup`s, within one DAG (direct `>>` task edges can't cross DAG objects, so this is
  never "one or more DAGs"): task dependencies.
- Multiple `DbtDag`s, separate DAGs (ticket-primary): Dataset / data-aware scheduling.

Separate projects / dbt-mesh (dbt-loom) is **out of scope**: `resolve_graph_boundary`
reads one project's node data per unit, and a mesh setup means each unit's data lives in a different
repo/deployment, possibly with different adapters - a materially different, harder problem.

Why the manual approach is fragile: users must understand Cosmos's dataset-URI syntax, the
hand-written `schedule=` drifts whenever `RenderConfig.select` or the dbt project changes, and
dataset URIs are environment-specific (they embed Airflow-connection / dbt-profile properties the
user has to reconstruct by hand).

## Current state (verified against the last released version of Cosmos, 1.15.1)

### Every dbt model already has a location-independent identity

A model, seed, or snapshot maps to a stable Asset/Dataset URI built from its warehouse relation - the
`postgres://0.0.0.0:5432/postgres.public.raw_customers` shape the ticket shows:

- `construct_dataset_uri(namespace, "database.schema.alias")` (`cosmos/dataset.py:206`).
- `compute_model_outlet_uris(manifest_path, namespace)` (`cosmos/dataset.py:279`) reads
  `manifest.json`, filters to model/seed/snapshot, and returns
  `{unique_id: [f"{namespace}/{database}.{schema}.{alias}"]}`.
- `namespace` comes from `get_dataset_namespace(profile_config)` (`cosmos/dataset.py:158`); it is
  `None` for unsupported adapters.
- The dependency graph (`depends_on.nodes`) lives in the same manifest.

A consumer can therefore compute, at parse time, the URIs of its external upstream models - exactly
what `auto_schedule` needs to inject into the DAG's `schedule`. **But only from a manifest** - see F1.

### How the *emitted* URI is produced differs by `ExecutionMode`

For the schedule to fire, the consumer's parse-time-declared URI must equal the producer's
runtime-emitted URI:

Cosmos has 13 `ExecutionMode` values (`cosmos/constants.py`): `WATCHER`, `WATCHER_KUBERNETES`,
`WATCHER_GCP_GKE`, `LOCAL`, `VIRTUALENV`, `AIRFLOW_ASYNC`, `DOCKER`, `KUBERNETES`, `AWS_EKS`, `AWS_ECS`,
`AZURE_CONTAINER_INSTANCE`, `GCP_CLOUD_RUN_JOB`, `GCP_GKE`. Most of them cannot emit an Asset at all - a
distinct, worse category than "conditional emission":

| Mode | Emitted URI source | Matches the manifest URI? |
|---|---|---|
| WATCHER / WATCHER_KUBERNETES / WATCHER_GCP_GKE | Producer parser calls `compute_model_outlet_uris(target/manifest.json, namespace)` (`cosmos/operators/_watcher/base.py`, shared by the Kubernetes/GKE variants via `_k8s_common.py`) and pushes it via XCom; the consumer sensor emits `Asset(uri)` via `register_dataset` (`cosmos/operators/watcher.py`). No runtime OpenLineage events involved. All three share the single-producer architecture discussed in the `DbtTaskGroup` coordinator section below. | Yes - built straight from the manifest. |
| LOCAL / VIRTUALENV | After the run, `get_datasets()` builds URIs via `construct_dataset_uri(output.namespace, output.name)` from `openlineage_events_completes` (`cosmos/operators/local.py`), gated by `emit_datasets`. | In practice yes (same OpenLineage standard the ticket's Cosmos-1.7 venv example relies on), **but emission is conditional** - see F4. |
| AIRFLOW_ASYNC (BigQuery) | `_register_event` hardcodes `f"bigquery://{gcp_project}/{dataset}/{table_name}"` (`cosmos/operators/_asynchronous/bigquery.py`). | No - a divergent scheme that bypasses `construct_dataset_uri` and won't match. |
| `DOCKER`, `KUBERNETES`, `AWS_EKS`, `AWS_ECS`, `AZURE_CONTAINER_INSTANCE`, `GCP_CLOUD_RUN_JOB`, `GCP_GKE` | None. `cosmos/operators/docker.py`, `kubernetes.py`, `aws_eks.py`, `aws_ecs.py`, `azure_container_instance.py`, `gcp_cloud_run_job.py`, and `gcp_gke.py` contain no `emit_datasets`/`get_datasets`/dataset-URI code at all - confirmed by grepping each file. `KUBERNETES` specifically is tracked in [astronomer-cosmos#2329](https://github.com/astronomer/astronomer-cosmos/issues/2329); the other six share the same gap without an existing tracked ticket. | N/A - there is nothing to match. This is a capability gap, not a conditional-emission or wrong-scheme problem, and it cannot be fixed by `emit_datasets=True` or by #2959 alone. |

Two cross-cutting hazards apply regardless of mode: the Airflow 2-vs-3 URI standard
(`construct_dataset_uri`, `settings.use_dataset_airflow3_uri_standard`) must match on both sides of a
dependency, and `get_dataset_namespace` must support the adapter in use.

This divergence is exactly what
[astronomer-cosmos#2959](https://github.com/astronomer/astronomer-cosmos/issues/2959) proposes to
normalize for the three modes that emit at all today (WATCHER family, LOCAL/VIRTUALENV,
AIRFLOW_ASYNC): route their emission through `construct_dataset_uri` instead of three separate
mechanisms, so a URI computed at parse time is guaranteed to match what gets emitted at runtime for
those modes. The seven modes with no emission capability (see the table above) are a separate problem
#2959 doesn't cover - see constraint 5. This proposal depends on #2959 per constraint 5.

### What Cosmos knows at render time

For a unit defined by `(ProjectConfig, RenderConfig.select/exclude, ProfileConfig)`, the nodes it owns
and their external upstreams are computable from the manifest. `RenderConfig.invocation_mode` defaults
to `InvocationMode.DBT_RUNNER` (`cosmos/config.py:90`) and is orthogonal to all of the above.

### Feasibility constraints surfaced during review

- **F1 - relation identity exists only via a manifest.** The `dbt ls` command Cosmos builds requests
  `--output-keys name unique_id resource_type depends_on original_file_path tags config freshness fqn`
  (`cosmos/dbt/graph.py`, around line 831) - **never** `database`/`schema`/`alias`/`relation_name`.
  `DbtNode` (`cosmos/dbt/graph.py:110`) stores no relation identity, and neither the dbt-ls parser nor
  the manifest-graph parser populates one. `LoadMode` defaults to `AUTOMATIC`, which resolves to
  `DBT_LS` whenever no manifest is available but a project path and profile are - so the ticket's own
  `ProjectConfig(jaffle_shop_path)` example (no manifest) runs via `dbt ls` and **cannot** yield
  relation URIs today. A node's captured `config` dict may hold *configured* `schema`/`database`
  values, but these are pre-resolution and usually unset - not the resolved relation.
  [astronomer-cosmos#2959](https://github.com/astronomer/astronomer-cosmos/issues/2959) is the ticket
  covering this: extending the requested `--output-keys` and `DbtNode` to carry relation identity
  independent of `LoadMode`.
- **F2 - `schedule` is consumed before Cosmos parses the graph.** `DbtDag.__init__`
  (`cosmos/airflow/dag.py`) calls `DAG.__init__` first, which normalizes `schedule`, and only then
  calls `DbtToAirflowConverter.__init__`; the converter parses the dbt graph and builds the Airflow
  graph later in its own `__init__` (`cosmos/converter.py`, `self.dbt_graph.load(...)` followed by
  `build_airflow_graph(...)`). A converter-stage mutation of `schedule`/`timetable` is therefore too
  late - confirmed still true against Cosmos 1.15.1.
- **F3 - non-emitting boundary nodes silently break scheduling.** `external = parents-of-selected -
  selected` is too shallow. An **ephemeral** model (`resource_type=model` but
  `materialized=ephemeral`, inlined as a CTE, never written -
  `DbtNode.has_ephemeral_materialization`, `cosmos/dbt/graph.py:150`) passes
  `compute_model_outlet_uris`'s resource-type filter yet **no task ever emits its Asset**, so injecting
  that URI would make the downstream DAG never trigger. **Sources** live in `manifest["sources"]` and
  never get URIs - they are genuinely external inputs, not a dependency to wait on. Silently tolerating
  "missing keys" either drops the dependency (downstream runs too early) or waits forever.
- **F4 - LOCAL/VIRTUALENV emission is conditional, not equivalent to WATCHER.** It depends entirely on
  OpenLineage artifact parsing (`openlineage-integration-common` /
  `DbtLocalArtifactProcessor`). If that package is absent, the completes-calculation short-circuits and
  `openlineage_events_completes` stays empty - zero datasets emitted, silently. If `parse()` raises
  `FileNotFoundError`/`NotImplementedError`/`ValueError`/`KeyError`/`jinja2.UndefinedError` it is
  swallowed at debug level - also empty emission. Any other exception propagates and fails the task. So
  LOCAL emission is "silent-skip or hard-fail" depending on the error, never a guaranteed emit.
- **F5 - the boundary resolver cannot verify that a matching producer exists or will emit, and this
  splits into a structural half and a harder, mode/namespace half.** A node being outside a unit's own
  selection only proves that unit doesn't own it; it proves nothing about whether another
  `DbtDag`/`DbtTaskGroup` actually owns it, or whether exactly one unit claims it (vs. two overlapping
  selections, or zero). This **structural** ownership question - does a rendered Airflow task exist for
  this `unique_id`, and does that task do real work - is answerable today with no manifest and no
  registry *when the peer units are all visible in one place*, which is exactly how
  `DbtDependencyCoordinator` is constructed (`DbtDependencyCoordinator([tg_a, tg_b, ...])` - see the
  coordinator section below). "Does real work" matters because `SeedRenderingBehavior.RENDER_ONLY`
  (`cosmos/airflow/graph.py`) renders an `EmptyOperator` placeholder for the seed instead of a real
  `dbt seed` task - a task exists, so a naive "is there a task" check would treat it as a valid producer,
  but it never actually loads the seed. The structural check must exclude `RENDER_ONLY` placeholders
  specifically, not just confirm a task exists. Ownership is **not** answerable at all for a standalone
  `DbtDag`, which is parsed with zero visibility into sibling `DbtDag`s - that's the harder problem open
  question 5 is about.

  On top of ownership, `auto_schedule` specifically (not the coordinator - see below) also needs the
  owner to actually **emit an Asset**, which fails several ways: the owner's `ExecutionMode` may be
  structurally incapable of emitting one at all (`DOCKER`/`KUBERNETES`/`AWS_EKS`/`AWS_ECS`/
  `AZURE_CONTAINER_INSTANCE`/`GCP_CLOUD_RUN_JOB`/`GCP_GKE` - see the emitted-URI table above); the owner
  may have `emit_datasets=False`; or, for seeds, `SeedRenderingBehavior.NONE` may drop the seed from the
  DAG/TaskGroup altogether (`return None`, no task at all), or `SeedRenderingBehavior.RENDER_ONLY` may
  render it as the `EmptyOperator` placeholder above (a task exists and succeeds, but never runs
  `dbt seed` and never emits) - so a seed that is a graph parent elsewhere can leave `auto_schedule`
  waiting forever with no error. Separately, `get_dataset_namespace(profile_config)`
  (`cosmos/dataset.py:158`) derives the URI namespace from whichever `ProfileConfig` is handed to it;
  `resolve_graph_boundary` only ever has the *calling* unit's `ProfileConfig`, with no visibility into
  the actual producing unit's `ProfileConfig`. Constraint 6 explicitly wants split units to carry
  different credentials, so if two units' profiles resolve to different namespaces, the consumer's
  guessed external-parent URI can silently never match what the producer emits - the schedule never
  fires, with no error anywhere. None of this - mode capability, `emit_datasets`, or namespace - applies
  to `DbtDependencyCoordinator`, which wires tasks directly and never touches a URI.

## Constraints and goals

1. **Opt-in and backwards-compatible.** Off by default; manual wiring keeps working unchanged. This
   could become the default behavior in [Cosmos 2.0](https://github.com/astronomer/astronomer-cosmos/milestone/10),
   where a breaking-change release is an acceptable place to flip the default.
2. **Derive edges from dbt's own `depends_on`**, not hand-maintained lists.
3. **Extend, don't replace, a user's schedule** - append the dbt-dataset condition to whatever the user
   already set, per the compatibility matrix in the Dataset-scheduling section below. Combinations that
   matrix marks unsupported (chiefly: an existing cron/interval/custom `Timetable` combined with
   `auto_schedule`) must raise, not silently drop one side of the intended schedule.
4. Cover the ticket-primary case (multiple `DbtDag`s via Datasets) and the additive case (multiple
   `DbtTaskGroup`s via task dependencies) from one shared resolver.
5. **Unify Asset emission for the modes #2959 actually covers, before building `auto_schedule` for
   them.** Today, only WATCHER/WATCHER_KUBERNETES/WATCHER_GCP_GKE reliably emit a URI that matches the
   manifest. LOCAL/VIRTUALENV emit conditionally and can silently emit nothing (F4). AIRFLOW_ASYNC emits
   a different URI scheme that never matches.
   [astronomer-cosmos#2959](https://github.com/astronomer/astronomer-cosmos/issues/2959) covers exactly
   these three: it routes them through `construct_dataset_uri`. `auto_schedule` for these modes should be
   built after #2959 lands, not against today's fragmented behavior.

   `DOCKER`/`KUBERNETES`/`AWS_EKS`/`AWS_ECS`/`AZURE_CONTAINER_INSTANCE`/`GCP_CLOUD_RUN_JOB`/`GCP_GKE`
   don't emit at all, and **#2959 does not cover them** - it's not a universal fix. `KUBERNETES` has its
   own ticket, [astronomer-cosmos#2329](https://github.com/astronomer/astronomer-cosmos/issues/2329); the
   other six have no tracked ticket. `auto_schedule` must keep these seven gated (raise, mode-specific
   error) regardless of what #2959 delivers - S2's supported-mode matrix is exactly the modes #2959
   covers, not "every mode."

   None of this affects the coordinator (S3), which never uses Assets. Also pin the Airflow 2-vs-3 URI
   standard on both sides of any dependency.
6. Enable per-subset schedule, owner, and credentials - each `DbtDag` already accepts its own
   `schedule`/`default_args`/`profile_config`, so this is mostly a matter of not breaking that.
7. **Not manifest-only (F1), scoped to URI enrichment - `auto_schedule` specifically, not the
   structural resolver or the coordinator.** Relation identity is only available via the manifest
   *today*, but this proposal deliberately does not scope `auto_schedule` as a manifest-only feature
   with `DBT_LS` treated as an optional follow-on: a cross-DAG dependency feature that silently only
   works for one `LoadMode` is a trap for `DBT_LS` users, who would get no error explaining why their
   setup doesn't qualify unless constraint 9 is followed carefully at every call site. This constraint
   does **not** apply to `resolve_graph_boundary` itself or to `DbtDependencyCoordinator` - both need
   only `unique_id`/`depends_on`/`resource_type`/`config`, already available under `DBT_LS` today, and
   ship independent of the dependency below (see Proposed design). For `auto_schedule`, this proposal
   **depends on**
   [astronomer-cosmos#2959](https://github.com/astronomer/astronomer-cosmos/issues/2959) (extending the
   `dbt ls` `--output-keys` Cosmos requests, plus `DbtNode`, to carry relation identity independent of
   `LoadMode`) and [astronomer-cosmos#2960](https://github.com/astronomer/astronomer-cosmos/issues/2960)
   (fixing the `dbt ls` cache key so it doesn't silently keep serving pre-#2959 node data after the
   upgrade) landing first. The URI-enrichment step should be written against relation identity as a
   property of `DbtNode` - however it got there, manifest or `dbt ls` - not against `LoadMode.DBT_MANIFEST`
   specifically, so no `DBT_LS`-specific carve-out is needed once those two land. The one caveat #2959
   itself flags and this proposal inherits: whether `database`/`schema`/`alias`/`relation_name` are valid
   `--output-keys` on every dbt-core version Cosmos supports and on dbt Fusion is unverified; if some
   supported version/engine combination genuinely cannot supply them, `auto_schedule` raises a clear
   error for that combination specifically (constraint 9), rather than the whole feature being
   permanently gated behind manifest availability. S4 in the roadmap below is this dependency, not a
   slice this proposal owns.

   **#2959/#2960 do not fully close the `DBT_LS` gap on their own.** They add fields to nodes `dbt ls`
   already returns; they don't change *which* nodes it returns. `dbt ls --select <x>` only returns
   selected nodes - an external ephemeral ancestor's own `DbtNode` (needed to traverse past it, F3) is
   never in the result. `DBT_MANIFEST` doesn't have this problem (the full project graph is always
   loaded). So even after #2959/#2960 land, `auto_schedule` on a standalone `DbtDag` under `DBT_LS` may
   still need to fail-fast on an external ephemeral ancestor it can't see - see open question 1.
8. **Schedule injection (F2) - resolved: patch `timetable` inside the converter, don't move loading
   before `DAG.__init__`.** Tested directly against Airflow 3.2.0rc1: `dag.schedule = X` after
   construction raises `FrozenAttributeError` (schedule is frozen). `dag.timetable = X` after
   construction works fine, and a serialize/deserialize round-trip confirms the patched timetable is
   what the scheduler actually sees (`schedule` isn't even serialized). So `auto_schedule` can just set
   `self.timetable` at the end of `DbtToAirflowConverter.__init__`, after computing the boundary and
   URIs as usual - no need to load anything before `DAG.__init__`, and no risk of loading the graph
   twice.

   Airflow validates several things against the timetable once, at construction, and none of it re-runs
   automatically when `timetable` is patched afterward: `catchup`/`start_date`, `allowed_run_types`,
   required-params-without-defaults, and `max_active_runs` vs `active_runs_limit`. Confirmed
   empirically - constructing a DAG directly with an asset schedule and `catchup=True` but no
   `start_date` raises; constructing with `schedule=None` (so the check doesn't fire) and then patching
   `timetable` to the same asset condition does not, silently accepting an invalid combination.

   Don't reimplement each check by hand - call `attrs.validate(self)` after patching. `catchup`,
   `allowed_run_types`, and `params` are all `attrs` field validators that read `self.timetable`
   dynamically, so `attrs.validate()` re-runs them against the patched value and correctly raises
   (confirmed empirically for the `catchup` case above). The one thing `attrs.validate()` won't catch is
   `max_active_runs` vs `active_runs_limit`, since that check lives inline in `__attrs_post_init__`, not
   a field validator - check that one explicitly. This also needs confirming on Airflow 2 and on later
   Airflow 3 versions, since Airflow could add more such checks over time.
9. **Fail closed, never silently no-op (F5) - the ownership half splits by consumer.** Both
   `auto_schedule` and `DbtDependencyCoordinator` are opt-in: a user who asks for one is explicitly
   asking Cosmos to guarantee an ordering or a schedule gate. For the **coordinator**, "exactly one
   owner renders a task for this `unique_id`" is verifiable today from the explicit list of peer
   `DbtTaskGroup`s passed into its constructor - no registry needed, because all peers are visible in one
   parse (see the coordinator section below); this must raise on zero or multiple owners among the
   passed-in peers. For **`auto_schedule`** on a standalone `DbtDag`, the same ownership question is
   **not** verifiable without some form of peer visibility a lone `DbtDag`'s parse does not have - see
   open question 5 - and `auto_schedule` additionally needs the owner's `ExecutionMode` to be capable of
   emitting an Asset at all (constraint 5), `emit_datasets=True`, `SeedRenderingBehavior` not to have
   dropped the node, and the owner's `ProfileConfig` namespace to match. Every case where the applicable
   guarantee cannot be verified must raise a clear `CosmosValueError` at parse time, not a silent no-op -
   including the case of a `DbtDag` whose peer-visibility mechanism (open question 5) simply doesn't
   exist yet, which is a reason to gate `auto_schedule`'s release on resolving that question, not a
   reason to relax this constraint.
## Proposed design

### Core: a structural boundary resolver (mode-independent, `LoadMode`-independent)

```python
@dataclass
class GraphBoundary:
    owned: set[str]  # unique_ids owned by this unit
    # (external_parent_unique_id, owned_child_unique_id) - already traversed
    # past ephemeral/non-emitting nodes (F3)
    external_edges: set[tuple[str, str]]
    external_sources: set[str]  # external parent unique_ids that are dbt sources


def resolve_graph_boundary(
    nodes: dict[str, DbtNode], selected_unique_ids: set[str]
) -> GraphBoundary: ...


def resolve_external_uris(
    boundary: GraphBoundary, nodes: dict[str, DbtNode], namespace: str
) -> dict[str, str]:
    # external_parent_unique_id -> Asset URI, built from each DbtNode's relation identity (F1) -
    # wherever that identity came from: the manifest today, or `dbt ls` once #2959/#2960 land.
    # Takes the node map, not a manifest, so it isn't tied to LoadMode.DBT_MANIFEST.
    ...
```

`resolve_graph_boundary` only needs `DbtNode.unique_id`/`depends_on`/`resource_type`/`config` - none of
that is relation identity, so this function has no F1 dependency. But it also needs the `nodes` map
passed in to actually *contain* an entry for each external parent it has to inspect, and that isn't
true under `DBT_LS` today: `dbt ls --select <x>` returns only the selected nodes
(`DbtGraph.run_dbt_ls`/`load_via_dbt_ls`, `cosmos/dbt/graph.py`) - an external parent's `unique_id` is
visible via `depends_on`, but that parent's own `DbtNode` (its `config`, so whether it's ephemeral) is
not, because it was never selected. `DBT_MANIFEST` doesn't have this problem: `_apply_manifest_node_selection`
(`cosmos/dbt/graph.py:1336`) keeps `self.nodes` as the *full, unfiltered* project graph and only
`self.filtered_nodes` as the selection - so every node's `config` is available regardless of selection.

So: under `DBT_MANIFEST`, pass `self.nodes` (full graph) and ephemeral traversal (F3, below) works today.
Under `DBT_LS`, `self.nodes` is selection-scoped, so the caller must supply a `nodes` map wide enough to
cover every external parent up to the nearest non-ephemeral one - source detection alone doesn't need
this (a `unique_id` prefix check works with no data), but ephemeral traversal does. S3 gets this for free
(see below); S2 (`auto_schedule` on a standalone `DbtDag`) does not, and needs a decision - see
constraint 7 and open question 1.

Structural edges and Asset URIs are deliberately kept separate, because the two consumers need different
things: `DbtDependencyCoordinator` (S3, below) only ever needs `owned`/`external_edges`/
`external_sources` - it wires Airflow tasks directly and never touches a URI, so gating it on F1 would
have rejected valid `DBT_LS`-based `DbtTaskGroup` splits for no reason. `auto_schedule` (S2, below) needs
the structural boundary *and* `resolve_external_uris`'s output, since it schedules on Assets. Only the
latter function needs relation identity (F1) - via the manifest today, or via `dbt ls` once
[astronomer-cosmos#2959](https://github.com/astronomer/astronomer-cosmos/issues/2959)/[astronomer-cosmos#2960](https://github.com/astronomer/astronomer-cosmos/issues/2960)
land (constraint 7); it should not be written against `LoadMode.DBT_MANIFEST` specifically.

**Non-emitting-boundary rule (F3, mandatory, structural):** when an external parent cannot itself be
rendered as a task that does real work - ephemeral models (excluded explicitly via
`has_ephemeral_materialization`, not via the resource-type filter) - the resolver must traverse through
it to the nearest such upstream node, updating `external_edges` to point there directly rather than at
the non-emitting node. This traversal needs no relation identity, only `config`/`resource_type`/
`depends_on` - but it does need that ancestor's `DbtNode` to be present in the `nodes` map passed in
(see above). For **S3**, the coordinator already has every peer's own loaded `nodes` map (not just their
`filtered_nodes`), so it passes the union of all peers' `nodes` into `resolve_graph_boundary` - covering
any ephemeral ancestor that at least one peer happens to have loaded, at no extra cost. For **S2**
(a standalone `DbtDag`, `DBT_LS`), there's no such union available; an ephemeral external ancestor whose
`DbtNode` isn't in this unit's own `nodes` can't be traversed, and the resolver must fail with an
unsupported-topology error rather than guess it isn't ephemeral - see open question 1. Sources go into
`external_sources`, keep their `external_edges` entry (they are still real graph parents, just never
schedulable or wireable ones - see the fail-closed exemption discussion in Edge cases), and
`resolve_external_uris` must never assign them a URI. The resolver fails fast with an
unsupported-topology error rather than silently dropping a dependency.

**Whether test nodes contribute edges is a resolver decision, not an afterthought.** A dbt test with
multiple parents can be pulled into `filtered_nodes` purely because one of its parents is selected
(`DbtGraph.update_node_dependency` in `cosmos/dbt/graph.py`), even when the test itself was not
independently selected and even under `TestBehavior.NONE`, where it will not run as part of this unit at
all. If `resolve_graph_boundary` walks `depends_on` over every node in `filtered_nodes` rather than only
over model/seed/snapshot nodes, that auto-attached test's *other* parent - possibly unrelated to
anything the owned models actually read - becomes a spurious external dependency and a spurious
schedule gate. The resolver must compute edges from model/seed/snapshot owned nodes only; test nodes are
not a source of boundary edges.

### Separate `DbtDag`s: Dataset scheduling (ticket-primary)

Adopt the ticket's proposed API - `RenderConfig.auto_schedule` taking a `DbtUpstreamUpdated` enum:

```python
RenderConfig(
    select=["path:models"], auto_schedule=DbtUpstreamUpdated.AND
)  # AND = all upstreams; OR = any
```

`auto_schedule` calls `resolve_graph_boundary` and then `resolve_external_uris`, and only needs
`set(external_uris.values())` - the `DbtDag` schedule gates on the *union* of resolvable external URIs
regardless of which owned node needs which. Cosmos injects the corresponding `Dataset`/`Asset` condition
into `schedule` (`AND` -> `AssetAll`/a list; `OR` -> `AssetAny`, on the Airflow versions that support
it), **extending** rather than replacing a user-set schedule, per this compatibility matrix
(constraint 3):

| User's existing `schedule` | `auto_schedule=AND` / `OR` |
|---|---|
| Not set (`None`) | Use `AssetAll(*uris)` / `AssetAny(*uris)` directly. |
| Already a Dataset/Asset list or expression | Combine into one `AssetAll`/`AssetAny` that wraps both the user's own assets and the dbt-derived ones. Open question 2 covers whether the user's own assets AND or OR against the new set by default. |
| A cron string, interval, or custom `Timetable` | **Unsupported in v1.** Airflow has no native time-AND-asset schedule - `DatasetOrTimeSchedule`/`AssetOrTimeSchedule` are OR-only, and this remains true on Airflow `main` (see Edge cases). Raise a clear `CosmosValueError` pointing at the S8 follow-up (an emulated AND) rather than silently dropping either the time or the asset condition. |

Per constraint 9, an external parent that resolves to no confirmed, emitting owner; an owner whose
`ExecutionMode` cannot emit at all (constraint 5); or a namespace mismatch with the consuming unit's own
`ProfileConfig` (F5) - must raise, not silently omit that URI from the condition. Unlike the coordinator
(below), `auto_schedule` cannot verify "exactly one owner" from information a standalone `DbtDag`'s own
parse has access to - this is open question 5, and it gates `auto_schedule`'s release, not something S2
can work around per-call.

**If every external parent is a source, `external_uris` is empty - raise, don't build an empty
condition.** Sources are exempt from the ownership check, but that means a source-only boundary leaves
nothing to schedule on. Silently applying `schedule=None` here would look like `auto_schedule` worked
when it did nothing; raise instead.

**Where injection happens (F2, resolved, constraint 8):** `DbtToAirflowConverter.__init__` already runs
after `DAG.__init__` and already loads the graph - no change needed there. At the end of its `__init__`,
after computing `external_uris`, it builds the combined timetable and sets `self.timetable` directly.
Confirmed safe on Airflow 3 (`schedule` is frozen, but `timetable` isn't, and the patched value survives
serialization). Airflow 2's `DAG` isn't `attrs`-based, so the same kind of patch should be even simpler
there, but that still needs its own check before shipping.

### `DbtTaskGroup`s in one DAG: direct task edges (additive)

A `DbtDependencyCoordinator([tg_a, tg_b, ...]).wire()` builds a map from each owned node's Airflow task
to its `external_edges` parents (structural `GraphBoundary`, above) and adds
`producer_task >> consumer_task` edges across groups, with `granularity="model"` or `"group"`. It uses
only the structural resolver - no manifest, no relation identity, no URI, no F1/#2959/#2960 dependency -
and works under any `LoadMode` for which the supplied `nodes` union covers every boundary ancestor -
`DBT_MANIFEST` always does; `DBT_LS` does only if the peer union happens to include the relevant
ancestor (see the core resolver section above), and the resolver deliberately raises unsupported
topology otherwise, rather than guessing. It doesn't touch `schedule`, so F2 doesn't apply. Because it wires
tasks directly instead of relying on emitted Assets, it also sidesteps the emitted-URI divergence in
constraint 5 for LOCAL/VIRTUALENV/KUBERNETES/AIRFLOW_ASYNC, where each model has its own executing task
and a task edge genuinely gates execution.

Its ownership check is scoped, not the full F5 check: since the constructor is handed the exact list of
peer `DbtTaskGroup`s, the coordinator can directly confirm that exactly one of them owns a given external
`unique_id` and raise otherwise (zero or multiple owners) - no registry needed. That check must exclude
`SeedRenderingBehavior.RENDER_ONLY` placeholders (an `EmptyOperator` that never runs `dbt seed` - F5), or
the coordinator would wire downstream tasks to a producer that trivially succeeds without loading
anything. It does not need to check
Asset emission or namespace (constraint 9); those only matter for schedules, not direct task wiring.

**WATCHER, WATCHER_KUBERNETES, and WATCHER_GCP_GKE need their own strategy - they are not
execution-mode-independent for this feature.** All three run one shared producer task that executes the
entire selected subset in a single `dbt build` (`_add_watcher_producer_task` in
`cosmos/airflow/graph.py`); per-model tasks are sensors watching that producer, not independent
executors. Wiring `upstream_consumer >> downstream_consumer` at model granularity only delays when the
downstream *sensor* starts watching - it does nothing to the downstream *producer*, which can already be
running that model's SQL against stale upstream data. For these three modes, the coordinator must wire
the upstream signal to the **downstream group's producer task** instead, which caps them at group-level
execution gating regardless of the requested `granularity`. `granularity="model"` under these modes
should either raise or explicitly degrade to group-level with a logged warning - open question 4 covers
which. Tests must assert the producer's actual execution order (e.g. via the producer's own
`upstream_list`), not just that an edge is rendered on a consumer sensor.

## Alternatives considered

| Alternative | Verdict |
|---|---|
| Status quo - hand-written `schedule=[Dataset(...)]` or `ExternalTaskSensor` | Rejected - this is exactly the fragility the ticket targets. |
| `TriggerDagRun` (ticket alternative a) | Rejected - less flexible, forgoes conditional dataset scheduling, and would require Cosmos to understand the user's DAG topology. |
| `DbtDagGroup` / `DbtDagFamily` (ticket alternative b) | Not purely a follow-up: `auto_schedule` on a standalone `DbtDag` can't verify ownership across siblings without something like this (open question 5). Variants: split by dbt tags, per-model Cosmos config, or a Python grouper. |
| Match on runtime-emitted Assets instead of manifest URIs | Rejected - the edge is only knowable after the run, but the consumer needs it at parse time. |
| A producer registry stored in an Airflow Variable, used to *compute* the join key | Rejected as unnecessary for that - the URI is already a deterministic join key. A registry may still be needed to *verify* ownership across `DbtDag`s (open question 5); that's separate from this rejection. |

## Changes by module (`astronomer-cosmos`)

| Module | Change |
|---|---|
| `cosmos/dataset.py` (or a new `cosmos/dependencies.py`) | `resolve_graph_boundary` (structural, `owned`/`external_edges`/`external_sources`, no manifest needed) and a separate `resolve_external_uris` (needs relation identity, F1). Includes the F3 ephemeral traversal, the test-node exclusion rule, and fail-fast behavior. |
| `cosmos/config.py` | New `RenderConfig.auto_schedule: DbtUpstreamUpdated \| None = None`; new `DbtUpstreamUpdated` enum (`AND`/`OR`) exported from `cosmos`. Reject `auto_schedule` on a `DbtTaskGroup`'s `RenderConfig` (it mutates a DAG's `schedule`; a `TaskGroup` isn't one). |
| `cosmos/converter.py` (F2, constraint 8) | At the end of `DbtToAirflowConverter.__init__`, for `auto_schedule`: build the combined timetable, set `self.timetable`, call `attrs.validate(self)` to re-run `catchup`/`allowed_run_types`/`params` validation, and separately re-check `max_active_runs` vs `active_runs_limit`. No change needed to `cosmos/airflow/dag.py`. |
| New `cosmos/airflow/dependencies.py` | `DbtDependencyCoordinator`: cross-`DbtTaskGroup` wiring, `granularity`, cycle/ambiguous-producer detection using its own peer list (no registry), and the WATCHER-family producer-level gating strategy. Uses only the structural resolver. |
| `cosmos/dbt/graph.py` (F1, dependency - not owned by this proposal) | DBT_LS relation metadata: extend `--output-keys` with `database`/`schema`/`alias`, and add the fields to `DbtNode`, the dbt-ls parser, and the ls cache; plus the cache-key fix. Scoped in [astronomer-cosmos#2959](https://github.com/astronomer/astronomer-cosmos/issues/2959) and [astronomer-cosmos#2960](https://github.com/astronomer/astronomer-cosmos/issues/2960) - `auto_schedule` (S2) is sequenced after both land, per constraint 7. |
| Docs | `docs/guides/multi_project/`, the scheduling guide; refit `dev/dags/cross_project_*` examples; document the WATCHER-family group-level-only granularity limitation. |

## Edge cases and risks

- **F1 - relation identity depends on [astronomer-cosmos#2959](https://github.com/astronomer/astronomer-cosmos/issues/2959)/[astronomer-cosmos#2960](https://github.com/astronomer/astronomer-cosmos/issues/2960) landing first.**
  `DBT_LS` has no relation identity today, and this proposal does not scope around that gap with a
  manifest-only carve-out (constraint 7) - it sequences after those two land instead. Even after they
  land, `dbt ls --select <x>` still only returns selected nodes, so an external ephemeral ancestor's own
  data may still be unavailable under `DBT_LS` - see open question 1. If some dbt-core-version/dbt-Fusion
  combination genuinely cannot supply the needed `--output-keys` at all, the resolver must raise a clear
  error for that specific combination (constraint 9 - never a silent no-op).
- **F2 - schedule-injection timing, resolved.** `schedule` itself can't be reassigned after `DAG.__init__`
  (frozen), but `timetable` can - patch it inside the converter (constraint 8).
- **F3 - non-emitting boundary nodes.** Ephemeral parents pass the type filter but never emit, which
  would make the downstream DAG never trigger; sources are genuinely external. The resolver must
  traverse past them or fail fast - never silently drop the dependency.
- **F4/constraint 5 - non-WATCHER emission is unreliable or missing.** LOCAL/VIRTUALENV emit
  conditionally; AIRFLOW_ASYNC emits the wrong scheme - `auto_schedule` depends on #2959 fixing both
  before it's built for those modes. The seven modes that don't emit at all are separate - #2959 doesn't
  cover them, and `auto_schedule` stays gated there regardless (constraint 5).
- **The Airflow 2-vs-3 URI standard** must match on both sides of a dependency, or the schedule
  silently never fires.
- **Unsupported adapter** - `get_dataset_namespace` returns `None`, so there is no dataset to schedule
  on; this must fail with a clear error rather than silently no-op.
- **No Airflow version offers a native time-AND-dataset schedule.** Airflow exposes
  `DatasetOrTimeSchedule` (Airflow 2) / `AssetOrTimeSchedule` (Airflow 3), but there is no AND
  equivalent, including on Airflow `main` as of this writing. Short term: document the "emit a daily
  dataset" workaround and let users control how Cosmos's dependency condition combines (`AND`/`OR`)
  with their own datasets. This is not blocked solely on Airflow shipping a native schedule type -
  Cosmos could emulate one (see the roadmap's follow-up slice below).
- **Source freshness** is not handled by this proposal. **Cycles or ambiguous producers** (overlapping
  selections claiming the same node) must be detected and raised, not silently resolved one way.
- **F5, split by consumer.** For the **coordinator**, ownership (does exactly one peer own this node) is
  checkable today from its own peer list - no gap. For **`auto_schedule`**, a standalone `DbtDag` can't
  see its siblings at all, so it can't check ownership, `emit_datasets`, mode capability, or namespace
  match without the peer-visibility mechanism open question 5 is about. Both must raise, never silently
  omit the dependency, once the applicable check is in place.
- **A source is legitimately ownerless - don't fail-closed on it.** Constraint 9 raises on an "ownerless"
  external parent, but a source has no Cosmos owner by definition (it's external data, e.g. loaded by
  Fivetran). The ownership check must exempt `external_sources` explicitly, not just happen to skip them
  because they have no URI. This proposal doesn't cover source freshness as part of its ordering
  guarantee - test this at the consumer level (a `DbtDag`/`DbtTaskGroup` with a source dependency renders
  and runs without raising), not only via the resolver's internal representation.
- **WATCHER/WATCHER_KUBERNETES/WATCHER_GCP_GKE's single producer breaks model-level
  `DbtDependencyCoordinator` wiring.** A per-model `producer_task >> consumer_task` edge only delays when
  a downstream *sensor* starts watching; the downstream group's actual `dbt build` runs inside one shared
  producer task whose own dependencies are untouched by that wiring, so it can execute against stale
  upstream data regardless of the rendered edge. These modes need producer-level gating instead, which
  caps them at group-level granularity - see the `DbtTaskGroup` coordinator section above.
- **Tests can leak spurious external dependencies.** A multi-parent test pulled into `filtered_nodes` by
  one selected parent (`DbtGraph.update_node_dependency`) can introduce an unrelated external dependency
  through its *other* parent if the resolver isn't scoped to model/seed/snapshot nodes only - including
  under `TestBehavior.NONE`, where the test wouldn't otherwise run as part of this unit at all.

## Testing

- `resolve_graph_boundary` (structural, no manifest fixture needed - test under `DBT_LS` too): `owned`/
  `external_edges`/`external_sources` are correct on a partitioned project; an ephemeral parent's edge
  is rewritten to the nearest real upstream when its `DbtNode` is available, or the resolver raises
  unsupported-topology when it isn't (e.g. a `DBT_LS`-loaded external ephemeral ancestor with no peer
  union supplying it); a multi-parent test's non-selected parent does *not* appear as an external
  dependency; the coordinator's peer-union `nodes` map correctly resolves an ephemeral ancestor owned by
  one peer and referenced by another.
- `resolve_external_uris` (takes a node map, not a manifest - run the same contract test against a
  manifest-loaded node map today and a `DBT_LS`-loaded one once #2959/#2960 land): resolves each
  non-source external parent to a URI; a source never gets one; missing relation identity raises with a
  clear error.
- Sources - two distinct cases, not one: a `DbtDag`/`DbtTaskGroup` with a source dependency *alongside*
  other, resolvable external dependencies renders and runs without raising (the ownership check exempts
  sources); a `DbtDag` using `auto_schedule` whose external dependencies are *only* sources raises,
  since there is nothing left to schedule on. Test both at the consumer level, not just as resolver
  output.
- F2/constraint 8: the DAG's `timetable` reflects the dbt-derived datasets and extends a user-set
  schedule; a serialize/deserialize round-trip confirms the patched timetable survives; a DAG that would
  now fail any of the checks Airflow normally runs at construction - `catchup`/`start_date`,
  `allowed_run_types`, required-params-without-defaults, `max_active_runs`/`active_runs_limit` - raises
  instead of shipping an invalid DAG. Run this across the supported Airflow 2 and Airflow 3 versions.
- `auto_schedule`: `AND`/`OR` combine correctly; Airflow 2 vs Airflow 3 URI parity; each row of the
  schedule-compatibility matrix behaves as specified (cron + `auto_schedule` raises); set on a
  `DbtTaskGroup`'s `RenderConfig` raises; each of the seven non-emitting execution modes raises with a
  mode-specific message (constraint 5); an unsupported adapter raises.
- F5: `auto_schedule` raises when ownership, emission, or namespace can't be verified (once the
  peer-visibility mechanism from open question 5 exists to make that check possible at all).
- End-to-end: WATCHER/WATCHER_KUBERNETES/WATCHER_GCP_GKE producer emits and the consumer triggers
  (seeds-to-models example); ASYNC and LOCAL/VIRTUALENV get the same test once #2959 lands.
- `DbtDependencyCoordinator`: cross-group edges at model and group granularity for
  LOCAL/VIRTUALENV/KUBERNETES/AIRFLOW_ASYNC; its own ownership check raises on zero or multiple owners
  among the peers it's given; for the WATCHER family, a test asserts the *producer* task's execution
  order is actually gated (not just that an edge is rendered on a consumer sensor), and that
  `granularity="model"` either raises or logs a group-level-only degradation (open question 4).

## Roadmap (additive, opt-in slices)

Listed in build order, not by slice number - S9 must land before S2, since S2 can't honestly ship
without it:

- **S1 - `resolve_graph_boundary`**: structural only (`owned`/`external_edges`/`external_sources`), the
  F3 ephemeral traversal, the test-node exclusion rule, the source exemption from ownership checks, and
  fail-fast behavior. No manifest dependency, but ephemeral traversal needs the caller to supply a
  `nodes` map wide enough to cover external ancestors (full graph under `DBT_MANIFEST`; under `DBT_LS`,
  whatever the caller can assemble - see open question 1). Test under both.
- **S3 - `DbtDependencyCoordinator`**: cross-`DbtTaskGroup` wiring, `granularity`, and its own scoped
  ownership check (zero/multiple owners among the peers it's given). Uses only S1's output. No
  dependency on S4/#2959/#2960. WATCHER-family groups get the producer-level gating strategy and are
  capped at group-level granularity (open question 4).
- **S9 - peer-visibility mechanism for `auto_schedule`:** resolve open question 5 - can a standalone
  `DbtDag` verify ownership/emission/namespace some other way, or does it need a `DbtDagGroup`
  (candidate: S6) or a registry? **This must land before S2**, not after - `auto_schedule` cannot honor
  constraint 9's fail-closed guarantee without it.
- **S4 (external dependency, blocking) - relation metadata for `DBT_LS`**: extend `--output-keys`,
  `DbtNode`, the parser, and the cache. Tracked in
  [astronomer-cosmos#2959](https://github.com/astronomer/astronomer-cosmos/issues/2959) and
  [astronomer-cosmos#2960](https://github.com/astronomer/astronomer-cosmos/issues/2960), not owned by
  this proposal.
- **S2 - `auto_schedule` and `DbtUpstreamUpdated`** for `DbtDag`. Depends on S9 (ownership/emission/
  namespace verification), S4 (`DBT_LS` relation identity, constraint 7), and constraint 5 (#2959
  unifying emission across modes) all landing first. Implementation: the `timetable` patch in the
  converter (constraint 8) and the extend-an-existing-schedule compatibility matrix.
- **S5 - ASYNC reconciliation**: part of #2959 (constraint 5), not owned by this proposal.
- **S6 - a `DbtDagGroup` container** (ticket alternative b.iii): the leading candidate for S9.
- **S7 - Docs and refitting the `cross_project_*` examples**, including the WATCHER-family
  group-level-only granularity limitation.
- **S8 (follow-up) - emulate a "dataset AND time" schedule**, so a user can combine a time schedule with
  the dbt-dependency condition without waiting on Airflow to add one natively. Sketch (credit: an Airflow
  maintainer's comment on #1321): a first task/branch operator that sets the upstream Assets as `inlets`,
  checks their asset events over a cutoff window, and skips the DAG body if the condition isn't met.
  Prerequisite: confirm how much asset-event data Airflow exposes to a running task.

## Open questions

1. **Residual `DBT_LS` gaps for `auto_schedule`, after #2959/#2960 land (F1, constraint 7).** Two
   specific items remain, both about `auto_schedule` on a standalone `DbtDag` under `DBT_LS`:
   - If `database`/`schema`/`alias`/`relation_name` turn out not to be valid `--output-keys` for some
     specific dbt-core-version/dbt-Fusion combination, does `auto_schedule` raise only for that
     combination, or does the whole feature need a documented minimum-version floor?
   - `dbt ls --select <x>` only ever returns selected nodes - #2959/#2960 don't change that. An external
     ephemeral ancestor's own data may still be invisible to a standalone `DbtDag` (the coordinator
     doesn't have this problem - it unions its peers' `nodes`). Does `auto_schedule` need an expanded
     `dbt ls` listing to cover this, or does it just fail-fast on that topology under `DBT_LS`
     permanently?
2. **`DbtUpstreamUpdated` semantics:** confirm the intended meaning of `AND`/`OR`, and how the
   dbt-derived condition should combine with a user's own datasets or timetable.
3. **`DbtDagGroup` (S6) internal design:** a Python grouper, a tag-driven mechanism, or per-model Cosmos
   config? (Separate from question 5 - whether `DbtDagGroup` is even the right answer to the
   peer-visibility problem.)
4. **`DbtTaskGroup` coordinator granularity:** should the default be model-level or group-level? And
   specifically for the WATCHER family, where only group-level execution gating is achievable
   (see the coordinator section above): should requesting `granularity="model"` raise outright, or
   degrade to group-level with a logged warning?
5. **Peer visibility for `auto_schedule` (F5, S9) - the biggest open question.** The coordinator (S3)
   already solves this for `DbtTaskGroup`s: its constructor takes the exact list of peers, so ownership
   is directly checkable, no registry needed. A standalone `DbtDag` has no such list - it's parsed
   independently, with zero visibility into sibling `DbtDag`s. So: does `auto_schedule` need a
   `DbtDagGroup`-style construct that takes multiple `DbtDag` configs together (pulling S6 forward as a
   prerequisite, not a follow-up), or a persistent registry (e.g. an Airflow Variable)? **A weaker,
   best-effort guarantee - validate whatever a lone `DbtDag`'s own parse can see, but accept that a
   genuinely unowned or double-owned external node won't be caught - is not on the table**: constraint 9
   requires fail-closed, not a documented weaker guarantee, so this question resolves to one of the two
   real mechanisms above, not a third "ship it anyway" option. This gates S2's release.
