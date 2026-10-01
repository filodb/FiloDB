# Proposal: Time-Split Aggregate Pushdown in MultiPartitionPlanner


## Summary

When ingestion of a shard key (workspace/namespace) moves from partition P1 to partition P2 at time `ts`, queries
whose time range spans `ts` need data from both partitions. Today, with `enable-remote-raw-exports` on,
`MultiPartitionPlanner` sends the instants that can be served by a single partition to that partition, and computes
the instants whose lookback window straddles `ts` on the query service after exporting raw samples from **both**
partitions. The raw export is expensive (heap pressure on the query service, network transfer of raw data), is
capped by `max-time-range-remote-raw-export`, and queries over the cap return no data for the gap.

For a small but very common family of queries, such as `sum(rate(foo[5m]))`, the raw export is unnecessary: the
full query can be pushed down to **each** partition for time ranges that overlap around the split, and the partial
results can be combined per timestamp on the query service. This works because for these queries the temporal
function (over a window split into two disjoint pieces) and the spatial aggregation use the same operator, so the
per-partition partial results can simply be combined with that operator.

This proposal adds that capability behind a feature flag, `query.routing.enable-time-split-aggregate-pushdown`.

## Current Behavior ("Before")

Query `sum(rate(foo[L]))` over `[t1, t3]`, split at `ts`, uncertainty `U`, offset `o`:

```
 time  ──────────────────────────────────────────────────────────────────────────────▶
        t1                        ts          ts+L+o+U                              t3
        |--------- P1 -----------|············|----------------- P2 ----------------|
        [   sum(rate) on P1      ][ raw export ][          sum(rate) on P2           ]
                                  from P1 + P2
                                  rate + sum on
                                  query service
```

Planned by `materializeSplitLeafPlan` → `getAssignmentQueryRanges` + `rewritePlanWithRemoteRawExport` +
`walkLogicalPlanTree(forceInProcess = true)`, and stitched with `StitchRvsExec`, which emits `NaN` when two children
have different non-NaN values at the same timestamp.

## Proposed Behavior ("After")

```
 time  ──────────────────────────────────────────────────────────────────────────────▶
        t1                   ts-U+o   ts   ts+L+o+U                                  t3
        [       sum(rate) on P1          ]
                             [            sum(rate) on P2                            ]
                             [ overlap:   ]
                             [ P1 + P2    ]
        StitchRvsExec(overlapMerge = Sum) adds partial results in the overlap
```

For each partition assignment `i` with data range `[s_i, e_i]`, instants `t` whose window
`[t - o - L, t - o]` can see that data are `[s_i + o, e_i + o + L]`. We extend this by `U` on both sides, clamp it
to the query range (the first and last assignments are open ended towards the query start/end), and snap it to the
original query step grid so that results of all partitions line up. The **entire** aggregate is pushed down to each
partition for its range. Each partition returns small, pre-aggregated results instead of raw series, and there is no
raw export limit.

## Eligibility

The plan must be `Aggregate(op, PeriodicSeriesWithWindowing(RawSeries, fn, window, ...))` with no aggregate params,
no function args, no `@` modifier, and exactly one routing key. The `by`/`without` clauses are allowed.

The requirement: `fn` over a window `W` split into disjoint `W1` (data in P1) and `W2` (data in P2) satisfies
`fn(W) = fn(W1) ⊕ fn(W2)`, and `op` uses the same `⊕`.

| Query | Eligible | Merge | Why |
|---|---|---|---|
| `sum(rate)`, `sum(increase)`, `sum(sum_over_time)`, `sum(count_over_time)` | ✅ | Sum | Additive over split windows |
| `max(max_over_time)` | ✅ | Max | Max of maxes |
| `min(min_over_time)` | ✅ | Min | Min of mins |
| `count(rate)`, `group(...)` | ❌ | | A series in both partitions within the overlap is counted twice |
| `avg`, `avg_over_time`, `quantile`, `topk`, `bottomk`, `stddev` | ❌ | | Not decomposable |
| `sum(foo)` (instant selector), `sum(last_over_time)` | ❌ | | P1 contributes a stale value in the overlap: double count |
| `max(rate)`, `sum(max_over_time)` | ❌ | | Operator mismatch |
| `irate`, `idelta`, `deriv`, `predict_linear` | ❌ | | Not additive over split windows |
| Subqueries, binary joins, `@` modifier | ❌ | | Not supported yet (future work) |

Plans that aren't eligible use the existing raw-export path unchanged.

## Interaction with Remote Raw Export

In production both `enable-remote-raw-exports` and `enable-time-split-aggregate-pushdown` are on, with the pushdown
scoped to specific tenants through `time-split-aggregate-pushdown-tenants`:

```
routing {
  enable-remote-raw-exports = true
  enable-time-split-aggregate-pushdown = true
  time-split-aggregate-pushdown-tenants = ["tenant-a", "tenant-b"]
}
```

The two don't conflict: the planner picks one path per query. For a query whose leaves span a time split,
`MultiPartitionPlanner.materializeSplitLeafPlan` decides in this order:

1. **Pushdown**, if all of these hold (`timeSplitAggregatePushdownMerge`):
   * `enable-time-split-aggregate-pushdown` is on,
   * the query's tenant is not on `disabled-remote-stitch-tenants`,
   * the allow list is empty, or the query filters the tenant label with equality (e.g. `_ws_="tenant-a"`) and
     every such tenant is on the allow list,
   * the whole plan has an eligible shape (see [Eligibility](#eligibility)).

   The whole query is sent to each partition and the results are merged with `StitchRvsExec(overlapMerge = ...)`.
   **Raw export is not used for this query.**
2. **Existing path, unchanged**, otherwise. Each partition answers the time range it can serve alone. The stretch
   after the split is computed on the query service from raw exports if `enable-remote-raw-exports` is on and the
   tenant is not on `disabled-remote-stitch-tenants`. That export is still capped by
   `max-time-range-remote-raw-export`.

Plain range selectors such as `foo[5m]` never reach this decision. `walkMultiPartitionPlan` sends them straight to
raw export.

With the production configuration above:

| Query | Tenant | Path |
|---|---|---|
| `sum(rate(foo[5m]))`, `sum(increase)`, `sum(sum_over_time)`, `sum(count_over_time)`, `max(max_over_time)`, `min(min_over_time)` | on the allow list, filtered with `_ws_="..."` | Pushdown |
| Same queries | not on the allow list, or tenant label filtered with a regex or not at all | Raw export |
| Same queries | on `disabled-remote-stitch-tenants` | Neither: no results from the split until P2 can answer alone (split + lookback + uncertainty), as today |
| Ineligible shapes: `count(rate)`, `avg(...)`, `sum(foo)`, `sum(irate)`, `topk`, subqueries, … | any | Raw export (unless the tenant is denied) |
| An eligible aggregate as only part of the query: `sum(rate(a)) + sum(rate(b))`, `sum(rate(a)) * 60`, `abs(sum(rate(a)))` | any | Raw export (unless the tenant is denied) |
| `foo[5m]` | any | Raw export (unless the tenant is denied) |

Behavior worth knowing:

* **Eligibility is decided on the whole plan.** It's checked on the plan whose leaves span the split, not on its
  parts. So an eligible aggregate inside a binary join, scalar operation or instant function uses raw export.
  Applying the pushdown to parts of a plan is listed under Future Work.
* **The raw export limit doesn't apply to the pushdown.** An eligible query with a long lookback, e.g.
  `sum(rate(foo[7d]))`, returns full results. On the raw-export path it would exceed
  `max-time-range-remote-raw-export` and return nothing around the split.
* **The pushdown doesn't depend on `enable-remote-raw-exports`.** With raw export off, eligible queries still get
  full results; everything else behaves as it does today with raw export off, with no results for the stretch
  after the split.
* **Period of uncertainty.** The raw-export path only uses `period-of-uncertainty-ms` when raw export is enabled.
  The pushdown always widens partition ranges by it.
* **The deny list does two things.** A tenant on `disabled-remote-stitch-tenants` gets neither the pushdown nor raw
  export. To keep a tenant on raw export only, leave it off the allow list instead.

`MultiPartitionPlannerSpec` ("time-split aggregate pushdown") covers the pushdown decisions above: eligible and
ineligible shapes, the allow and deny lists, eligible aggregates inside larger plans, the raw export limit, and
raw export turned off.

## How the Move Is Conducted

The pushdown relies on how a shard key's ingestion moves from P1 to P2:

1. **Scheduled in advance.** The move takes effect at a chosen time `ts` in the future. The new routing
   configuration (P1 for sample timestamps before `ts`, P2 from `ts` onwards) is pushed to all ingestion gateways
   before `ts`, and the partition assignments the query planner sees (`PartitionLocationProvider`) carry the same
   `ts`.
2. **Routed by sample timestamp.** Each gateway picks the partition from the **sample's own timestamp**, not from the
   time the gateway processes it. Every gateway therefore places a given sample in the same partition regardless of
   clock skew between gateways, ingestion lag or late-arriving samples.
3. **No loss, no duplication.** Every sample is ingested into exactly one partition. Nothing is dropped at the switch
   and nothing is written to both partitions.

As a result, for every series, all samples with timestamps before `ts` are in P1 and all samples from `ts` onwards
are in P2, with no gap in between. The planner still widens each partition's range by `period-of-uncertainty-ms`
on both sides. That costs a little extra work and is a safety margin in case the planner's partition boundary and
the gateways' switch time are configured slightly differently.

## Assumptions and Disclaimers

1. **Every sample in exactly one partition** (item 3 above). If samples were written to both partitions, sum-based
   results in the overlap would **double count**. The raw-export path doesn't have this problem because stitching
   raw samples de-duplicates identical timestamps.
2. **Each series split cleanly at `ts`** (item 2 above). If gateways routed by processing time instead, samples near
   `ts` could land out of order across partitions (a later sample in P1, an earlier one in P2). Delta counters stay
   exact in that case, but for cumulative counters the partial rates overlap or leave holes, and the result can be
   **too high or too low**. With 0–20s of random routing delay per sample, `TimeSplitAggregatePushdownSpec`'s setup
   showed errors from +0.5% to −4.1% in the overlap.
3. **`rate`/`increase` of cumulative counters are slightly approximate in the overlap, and always underestimate.**
   Each partition computes the rate on its part of the window with Prometheus-style extrapolation. The increase
   between a series' last P1 sample and its first P2 sample is covered only by extrapolation, and a partition with
   fewer than 2 samples in its part of the window contributes nothing (`extrapolatedRate` returns NaN).

   `TimeSplitAggregatePushdownSpec` measures this with the production pipeline (chunked storage,
   `PeriodicSamplesMapper`, `AggregateMapReduce(Sum)`, `StitchRvsExec(overlapMerge = Sum)`) for 25 series with
   random scrape phases, ±1s jitter and ±20% noise, 30s scrapes and a 5m lookback. It compares against the same
   pipeline over the stitched series, which is what the raw-export path computes:

   | `sum(rate(foo[5m]))` | worst dip in the overlap | outside the overlap |
   |---|---|---|
   | cumulative counter | −0.7% | identical |
   | delta counter | 0% (identical) | identical |

   The dip is small because series scrape at different phases and their errors average out. A sum over **few**
   series, or over series scraped in lockstep, approaches the single-series worst case. 

   **Delta counters are exact.** For delta temporality `rate` is `sum(deltas in window) / window length`
   (`RateOverDeltaChunkedFunctionD`) and `increase` is `sum_over_time`, with no extrapolation, so the window splits
   exactly across partitions. `sum_over_time`, `count_over_time`, `max_over_time` and `min_over_time` are exact too.
4. **Counter resets** across the split aren't an issue, since each partition computes its own portion independently.
5. **Partition assignments** are assumed to be sorted and time-disjoint, like the rest of `MultiPartitionPlanner`.
6. **Why not `MultiPartitionReduceAggregateExec`?** Its row aggregators (`RangeVectorAggregator.mapReduceInternal`)
   zip child rows by position and require all children to share the same time range. Here the children have
   different ranges, so the merge happens in `StitchRvsExec`, which merges by timestamp.

## Changes

### `StitchRvsExec` (query module)

* New `sealed trait StitchOverlapMerge` with `NaNOnConflict` (default, current behavior), `Sum`, `Max`, `Min`.
* New `overlapMerge` param on `StitchRvsExec` and `StitchRvsExec.merge`. When not `NaNOnConflict`, values from more
  than one child at the same timestamp are combined. NaN (or an empty histogram) is treated as "no data" from that
  child. If all values are NaN, the output is NaN.
* Histogram results support `Sum` (via `MutableHistogram.addNoCorrection`). The value column type is taken from the
  child result schemas in `compose`. `Max`/`Min` on histograms throw.

### Protobuf (grpc module)

* New `enum StitchOverlapMerge` and field `overlapMerge = 3` on `message StitchRvsExec`. The proto3 default
  (`NAN_ON_CONFLICT`) keeps the legacy behavior for plans serialized by older nodes.
* `ProtoConverters` converts in both directions.

### `MultiPartitionPlanner` (coordinator module)

* `materializeSplitLeafPlan` first checks `timeSplitAggregatePushdownMerge(plan)`. If the plan is eligible, it
  delegates to `materializeTimeSplitAggregatePushdown`, which computes the overlapping per-assignment ranges,
  materializes the full plan per assignment via `materializeForAssignment` (local planner or remote exec, and
  proportional `PartitionAssignmentV2` handled as today), and wraps the children in
  `StitchRvsExec(overlapMerge = ...)`.
* The tenant check is split out of `supportRemoteRawExport` into `isRemoteStitchDisabledTenant`, so the new feature
  respects `disabled-remote-stitch-tenants` independently of `enable-remote-raw-exports`.

### Config

* `query.routing.enable-time-split-aggregate-pushdown = false` (`filodb-defaults.conf`, `RoutingConfig`).
* `query.routing.time-split-aggregate-pushdown-tenants = []`: an optional allow list. When non-empty, only these
  tenants get the pushdown. Tenants are values of the `disabled-remote-stitch-tenant-column-name` label (`_ws_` by
  default), and the query must filter on that label with equality (`_ws_="my-workspace"`). Queries without such a
  filter, or with a regex on it, keep the existing raw-export path. `disabled-remote-stitch-tenants` still applies on
  top, and also keeps disabling raw export as today. Empty means all tenants.

  To enable for a single tenant:

  ```
  routing {
    # not needed by the pushdown; keeps the raw-export path for queries that aren't eligible
    enable-remote-raw-exports = true
    enable-time-split-aggregate-pushdown = true
    time-split-aggregate-pushdown-tenants = ["my-workspace"]
  }
  ```

## Rollout

1. Deploy with the flag off everywhere. The proto field is backward compatible.
2. Confirm the move procedure above holds: moves are scheduled with configuration pushed ahead, gateways route by
   sample timestamp, and no sample is dropped or written to both partitions (assumptions 1 and 2).
3. Enable for one or a few tenants with `time-split-aggregate-pushdown-tenants`. Compare results against the
   raw-export path around recent splits, and compare query-service heap and latency for queries spanning splits.
4. Enable broadly by emptying the allow list.

## Future Work

* Guard `rate`/`increase` with a minimum lookback (e.g. only push down when `L` ≥ a configured duration such as 10m)
  and fall back to raw export for shorter windows. This matters mostly for sums over few series, given the
  single-series errors measured in assumption 3.
* Apply the rule to each side of a `BinaryJoin` (e.g. `sum(rate(a)) / sum(rate(b))`) and to scalar operations on
  eligible aggregates (e.g. `sum(rate(x)) * 60`).
* Rewrite `avg(fn)` as `sum(fn) / count(fn)` when `fn` is additive and series don't overlap across partitions.
* Support `sum by` over subqueries whose inner step aligns with the split.
