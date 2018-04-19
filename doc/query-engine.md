# FiloDB Query Engine

FiloDB Query Engine allows for distributed orchestration of queries. Queries are processed this way
inside FiloDB:

1. Parse the query string from a syntax such as PromQL into its AST
2. Convert Language specific AST into a FiloDB Logical Plan 
3. Submit the Logical Plan to the Query Engine for execution

The Query Engine internally takes the query through this pipeline for execution:
1. Validate the `LogicalPlan` 
2. Optimize the `LogicalPlan` continuously using configurable rules
3. Materialize the `LogicalPlan` into one or more candidate `ExecPlan`s using shard/cluster health/metadata.
   The load of a FiloDB node, the health of shard replicas etc are all useful information that can be 
   fed to the materializer to enumerate `ExecPlan`s.
4. Apply a cost model for each `ExecPlan`, and pick the one with lowest cost. 
5. Trigger the orchestration of the `ExecPlan`

Note that in the current implementation:
* We do not optimize the logical plan yet.
* We do not enumerate more than one candidate execution plan and cost them. We simply materialize
  one execution plan.
  
Later phases of implementation can enhance the engine to provide the envisioned functionality.

## Logical Plan

The logical plan is a tree of `LogicalPlan` type nodes. The `RawSeriesPlan` node type can be used
for the leaves. It represents extraction of raw data from partitions identified by the filter. 

```
  val raw = RawSeries(from = 1523575055L,
                      to = 1523785055,
                      filters = Seq(ColumnFilter(ColumnFilterType.Equal, "__name__", "http_requests_total"),
                                    ColumnFilter(ColumnFilterType.Equal, "job", "some-service")))
```

Note that column filters need to include the shard key columns for the dataset. This enables FiloDB to 
narrow down on the shards it must access to retrieve the data.

The `RawSeries` node can now be composed to transform and manipulate raw data. For instance `SampledSeriesWithWindowing`
helps in transforming raw data into samples with regular intervals after applying a look-back windowing
range function on the raw data.  

```
  val sampled = PeriodicSeriesWithWindowing(rawSeries = raw,
                                            start = 1523576055L,
                                            step = 5000,
                                            end = 1523785055,
                                            window = 5000,
                                            function = RangeFunction.Rate)

```

The `BinaryJoin` node type helps in joining two child series. Look at the scaladocs for more information
on other node types. We have `Aggregate`, `ScalarVectorBinaryOperation` and `ApplyInstantFunction`

It is important to note that `RawSeriesPlan` type nodes are not composable before they are transformed
into samples with regular interval.       

## Execution Plan Constructs

The execution plan is a tree of `ExecPlan` type nodes. The tree structure represents the sub-query hierarchy.
During materialization, each `ExecPlan` node is assigned a dispatcher, which abstracts the target node on which
the plan will be executed. The Query Engine will co-locate execution of sub-queries as close to the data as
possible and minimize movement of data over the wire.

`ExecPlan` nodes can be executed using the `ActorExecutionStrategy` implementation of the `ExecutionStrategy`
trait. Communication between nodes happen using Akka Actor messages.

Once a node receives an `ExecPlan`, its `execute()` method can be used to orchestrate the execution of the plan
node. 

The `NonLeafExecPlan` trait helps its subclasses by orchestrating (scatter-gather) the execution of the child 
`ExecPlan`s before allowing the implementations to compose the sub-query results in their own way in
the `compose` abstract method.

Subclasses of `LeafExecPlan` will implement the `doExecute` method to perform leaf level operations that involve
fetching of data. 

Here are the various `ExecPlan` implementations we have. The convention is for concrete ExecPlan names
to end with 'Exec'. 
* `SelectRawPartitionsExec`: This is the only lead node execution plan to extract raw data from the MemStore
* `ReduceAggregateExec`: This node takes intermediate aggregates and reduces them 
* `BinaryJoinExec`: This node performs binary operations using results returned from child plans
* `DistConcatExec`: This node simply concatenates data from child plans. Typically used to accumulate data from multiple nodes. 

### Range Vector Transformers

While the `ExecPlan` nodes deal with distribution of sub-queries across nodes, they have another important 
capability. Data transformation operations that do not involve data from multiple machines can be applied 
directly at source without needing to move data across machines. This is done by adding `RangeVectorTransformer`
implementations to the `ExecPlan` objects.

We have the following transformers:
* `PeriodicSamplesMapper`: Sample raw data to intervals optionally applying a range vector function on time windows
* `InstantVectorFunctionMapper`: Apply an instant vector function
* `ScalarOperationMapper`: Perform a binary operation with a scalar
* `AggregateCombiner`: Performs aggregation operation across instants of the Range Vectors
* `AverageMapper`: Calculates average from "sum" and "count" columns in each range vector

Each `ExecPlan` node in the tree can be associated with zero or more of such transformers. The `ExecPlan.execute`
method will first  perform its designated operation via the `doExecute` and `compose` methods and then
apply the `RangeVectorTransformer` transformations one-by-one. 

## Conversion of `LogicalPlan` to `ExecPlan`

Conversion of `LogicalPlan` to `ExecPlan` is done by walking the logical plan tree in a depth first manner.
Each sub-tree in the logical plan will result in a one or more `ExecPlan` nodes.

Whenever possible, data transformation operations are co-located with data source by appending `RangeVectorTransformer`
constructs to child execution plans.

### Dispatcher Assignment

During materialization, each ExecPlan node will be assigned a dispatcher that abstracts the delivery of the plan
to the node it will be executed on. 

For the leaf nodes that involve data extraction from memstore, the target is decided by calculating
the shard key hash from the filters and then the shard number. We then look up a healthy node from
the `ShardMapper` which is kept up-to-date with the changes in the cluster.
 
For the non-leaf `ExecPlan` nodes, we randomly select one of the child nodes' target as the target
of the parent node. This is to minimize data movement over the wire. 

## Example

The following PromQL statement calculates the fraction of requests with latency below 300ms for each 5 minute window
```
sum(rate(http_request_duration_seconds_bucket{job="myService",le="0.3"}[5m])) by (job)
 /
sum(rate(http_request_duration_seconds_count{job="myService"}[5m])) by (job)
```

The `LogicalPlan` tree for the above PromQL would be 
* `BinaryJoin` with operation=DIV
  * `Aggregate` with operation=SUM 
    * `PeriodicSeriesWithWindowing` with function=Rate and window=5000 
       * `RawSeries` with filter __name__==http_request_duration_seconds_bucket && job==myService && le==0.3             
  * `Aggregate` with operation=SUM
    * `PeriodicSeriesWithWindowing` with function=Rate and window=5000 
       * `RawSeries` with filter __name__==http_request_duration_seconds_count && job==myService            

A candidate `ExecPlan` tree materialized for the above `LogicalPlan` could be

* `BinaryJoinExec` *on Host2*
  * `ReduceAggregateExec` *on Host1*
       * `SelectRawPartitionsExec` with filter __name__=http_request_duration_seconds_bucket && job==myService && le==0.3 and shard 1 *on Host1*
       * `SelectRawPartitionsExec` with filter __name__=http_request_duration_seconds_bucket && job==myService && le==0.3 and shard 2 *on Host2*
  * `ReduceAggregateExec`  *on Host2*   
       * `SelectRawPartitionsExec` with filter __name__=http_request_duration_seconds_count && job==myService and shard 1 *on Host2*
       * `SelectRawPartitionsExec` with filter __name__=http_request_duration_seconds_count && job==myService and shard 2 *on Host1*

Note that each of the `SelectRawPartitionsExec` nodes above would be associated with the following
`RangeVectorTransformer`s in that order and would run on the same host as the `SelectRawPartitionsExec`
* `PeriodicSamplesMapper`
* `AggregateCombiner`

See [QueryEngineSpec](../coordinator/src/test/scala/filodb.coordinator/queryengine2/QueryEngineSpec.scala) for
test code on this example. 
