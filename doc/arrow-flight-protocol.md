# Arrow Flight RPC Protocol in FiloDB

This document describes the Apache Arrow Flight transport layer in FiloDB: its wire protocol, request/response schema, versioning mechanism, and the advantages it provides over the Akka Actor and gRPC dispatchers it augments or replaces.

## Pre-requsite Reading

* [Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html)
* [Arrow Flight Java Cookbook](https://arrow.apache.org/cookbook/java/flight.html)

## Background and Motivation

FiloDB is a distributed time-series store.  Query execution is a tree of `ExecPlan` nodes dispatched across nodes in one or more cells (partitions).  Two existing transports carry these plans:

| Transport | Scope | Mechanism |
|-----------|-------|-----------|
| **Akka Actor** (`ActorPlanDispatcher`) | Within a single cell (intra-partition) | Akka `ask` with Kryo or Protobuf serialization of the full `ExecPlan` object |
| **gRPC** (`GrpcPlanDispatcher` / `PromQLGrpcRemoteExec`) | Across cells (inter-partition) | Protobuf-serialized `RemoteExecPlan` streamed over HTTP/2 |

Both transports serialize query *result rows* as Protobuf messages on the heap.  For large fan-outs that return millions of time-series samples this adds per-message object overhead, GC pressure, and an extra copy between native memory and the JVM heap.

**Arrow Flight** solves these problems by:
1. Keeping row data in Arrow `VectorSchemaRoot` (VSR) buffers allocated off-heap by an explicit `BufferAllocator`.
2. Streaming metadata (schema, stats, errors) as side-channel `putMetadata` messages so data frames carry pure payload.
3. Reusing the Apache Arrow ecosystem's zero-copy record-batch transfer between sender and receiver.

The Flight transport is implemented in `coordinator/src/main/scala/filodb/coordinator/flight/` and covers **both** single-partition and multi-partition scenarios with an identical response schema.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  Single-partition sub-plan                                       │
│    └─ FlightPlanDispatcher ──► Ticket(Kryo(ExecPlan))            │
│                                                                  │
│  Multi-partition sub-plan                                        │
│    └─ FlightPlanDispatcher ──► Ticket(Proto(Request))            │
│              ▲  (PromQLFlightRemoteExec.grpcRequest)             │
└──────────────┼───────────────────────────────────────────────────┘
               │  HTTP/2  (Arrow Flight getStream RPC)
               ▼
┌─────────────────────────────────────────────────────────────────┐
│  FiloDB Flight Server  (Netty, port = akkaPort + 5000)          │
│                                                                 │
│  FiloDBSinglePartitionFlightProducer                            │
│    getStream(Ticket)                                            │
│      deserialize Kryo ticket → ExecPlan                         │
│      executePlan(q, querySession) → QueryResponse               │
│      FlightQueryResultStreaming.streamResults(…)                │
│                                                                 │
│  FiloDBMultiPartitionFlightProducer                             │
│    getStream(Ticket)                                            │
│      parse Proto Request → PromQL string                        │
│      queryPlanner.materialize(logicalPlan, qContext)            │
│      → ExecPlan                                                 │
│      executePhysicalPlanAndRespond(…)                           │
│        FlightQueryResultStreaming.streamResults(…)              │
└─────────────────────────────────────────────────────────────────┘
               │  Arrow Flight response stream
               ▼  (same schema for both server types)
┌─────────────────────────────────────────────────────────────────┐
│  Client: FlightPlanDispatcher.executeFlightRequest              │
│    putMetadata → Header (ResultSchema)                          │
│    putNext     → VSR batch 0 … N                                │
│    putMetadata → Footer (QueryStats, optional Throwable)        │
│    → ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs     │
│    → QueryResult(id, schema, List[ArrowSerializedRangeVector])  │
└─────────────────────────────────────────────────────────────────┘
```
Here is a potential deployment diagram

```

┌───────────────────────────────┐                   ┌───────────────────────────────┐
│     Partition/Cell 1          │                   │     Partition/Cell 2          │ 
│                               │                   │                               │
│   ┌───────────────────────┐   │                   │   ┌───────────────────────┐   │
│   │                       │   │  Multi Partition  │   │                       │   │
│   │ Partition Query Facade│───│────Flight RPC────>│   │ Partition Query Facade│   │
│   │                       │   │                   │   │                       │   │
│   └───────────────────────┘   │                   │   └───────────────────────┘   │
│               │               │                   │               │               │
│     Single Partition Flight   │                   │     Single Partition Flight   │
│              RPC              │                   │              RPC              │
│               │               │                   │               │               │
│               V               │                   │               V               │
│   ┌───────────────────────┐   │                   │   ┌───────────────────────┐   │
│   │                       │   │                   │   │                       │   │
│   │       FiloDB          │   │                   │   │       FiloDB          │   │
│   │                       │   │                   │   │                       │   │
│   └───────────────────────┘   │                   │   └───────────────────────┘   │
│                               │                   │                               │
└───────────────────────────────┘                   └───────────────────────────────┘

```
---

## Server Implementations

### Single-Partition Flight Server

**Class:** `FiloDBSinglePartitionFlightProducer`  
**Ticket payload:** Kryo-serialized `ExecPlan`  
**Port:** `akkaPort + 5000` (This will change later when akka and flight are fully decoupled)
**Execution:** calls `execPlan.execute(memStore, querySession)` directly—no re-planning

```scala
// FiloDBSinglePartitionFlightProducer.scala
override def getStream(context, ticket, listener) = {
  FlightKryoSerDeser.deserialize(ticket.getBytes) match {
    case execPlan: ExecPlan =>
      executePhysicalPlanAndRespond(context, execPlan, listener)
    ...
  }
}

def executePlan(q: ExecPlan, querySession: QuerySession): Task[QueryResponse] =
  q.execute(memStore, querySession)(queryScheduler)
```

The server is started with:
```scala
FiloDBSinglePartitionFlightProducer.start(memStore, allConfig)
// host from akka.remote.netty.tcp.hostname, port = akkaPort + 5000
```

### Multi-Partition Flight Server

**Class:** `FiloDBMultiPartitionFlightProducer`  
**Ticket payload:** Protobuf-serialized `GrpcMultiPartitionQueryService.Request`  
**Execution:** parses PromQL, materializes a logical plan, then dispatches the resulting `ExecPlan` tree  

```scala
// FiloDBMultiPartitionFlightProducer.scala
override def getStream(context, ticket, listener) = {
  val request = GrpcMultiPartitionQueryService.Request.parseFrom(ticket.getBytes)
  // version gate (see Response Versioning section)
  require(request.getFlightResponseAcceptVersion == ACCEPT_RESPONSE_VERSION1)
  val logicalPlan = Parser.queryRangeToLogicalPlan(request.getQueryParams.getPromQL, ...)
  val execPlan    = queryPlannerSelector(request.getPlannerSelector).materialize(logicalPlan, qContext)
  executePhysicalPlanAndRespond(context, execPlan, listener)
}

def executePlan(execPlan: ExecPlan, querySession: QuerySession): Task[QueryResponse] =
  // dispatch with UnsupportedChunkSource – leaf plans run on remote nodes, not in-process
  execPlan.dispatcher.dispatch(ExecPlanWithClientParams(execPlan, ...), UnsupportedChunkSource())
```

The `processMultiPartition = false` flag is set in the Request by the client (`PromQLFlightRemoteExec`) to prevent the receiving node from infinitely re-planning multi-partition queries.

---

## Request Differences: Single-Partition vs Multi-Partition

| Aspect | Single-Partition | Multi-Partition |
|--------|-----------------|-----------------|
| **Who constructs the ticket** | `FlightPlanDispatcher` | `PromQLFlightRemoteExec.grpcRequest` |
| **Ticket encoding** | Kryo bytes of a concrete `ExecPlan` subclass | Protobuf `Request` message |
| **What the server receives** | A physical plan, ready to run | A PromQL expression + planner params |
| **Server action** | Execute plan directly against `TimeSeriesStore` | Parse PromQL → materialize plan → dispatch |
| **Re-planning guard** | N/A (already a physical plan) | `processMultiPartition = false` in `PlannerParams` |
| **Transport negotiation** | No version field | `flightResponseAcceptVersion = "1.0"` (required) |
| **Server class** | `FiloDBSinglePartitionFlightProducer` | `FiloDBMultiPartitionFlightProducer` |

Both paths converge in `FlightQueryResultStreaming.streamResults`, so the **response protocol is identical**.

---

## Unified Response Protocol

Every Flight response—regardless of whether the query was single-partition or multi-partition—uses the same three-phase message sequence.

### Arrow VSR Schema (`arrowSrvSchema`)

All `VectorSchemaRoot` batches in the data phase share one Arrow schema with two columns:

```
Schema {
  isRvk : Bool   (not-nullable)   // 1 = RangeVector key row, 0 = data row
  rvkBr : Binary (nullable)       // payload bytes (see below)
}
```

Defined in `ArrowSerializedRangeVectorOps.arrowSrvSchema`.

| `isRvk` | `rvkBr` contents |
|---------|-----------------|
| `1` | Protobuf `RvMetadata` — either an `RvKey` (binary-record partition key + optional `RvRange`) or a full `SerializableRangeVector` (for scalar / formulated-row results) |
| `0` | Binary-encoded `BinaryRecord` row (timestamp + value columns); `null` for empty-histogram / NaN rows that are intentionally omitted |

The VSR capacity limits are:

```scala
val maxVecLen  = 1_048_576   // 1 MB data buffer per VSR
val maxNumRows = maxVecLen / 15  // ≈ 69 813 rows per VSR
```

When a RangeVector's rows exceed one VSR, it spills into the next one transparently; `ArrowSerializedRangeVector` tracks `startVsrIndex` and `rvkRowIndex` to reassemble the cursor across VSR boundaries.

### Message Order on the Wire

```
Flight response stream
  ┌───────────────────────────────────────┐
  │  putMetadata(FlightMetadata.header)   │  ← ResultSchema (Protobuf, side-channel)
  ├───────────────────────────────────────┤
  │  putNext()   [VSR batch 0]            │  ┐
  │  putNext()   [VSR batch 1]            │  │ Zero or more VectorSchemaRoot batches.
  │    …                                  │  │ Each batch ≤ 1 MB / ≤ 69k rows.
  │  putNext()   [VSR batch N]            │  ┘
  ├───────────────────────────────────────┤
  │  putMetadata(FlightMetadata.footer)   │  ← QueryStats + optional Throwable
  │  completed()                          │
  └───────────────────────────────────────┘
```

The side-channel metadata (header and footer) are Arrow `ArrowBuf` slices carrying serialized `FlightMetadata` Protobuf messages; they never appear as data rows.  Data rows are in the `putNext` Arrow record batches and are never mixed with metadata.

### Header: `FlightResultHeader`

Sent as the **first** `putMetadata` call before any data batch:

```protobuf
// query_service.proto
message FlightResultHeader {
  ResultSchema resultSchema = 1;
}

message FlightMetadata {
  oneof content {
    FlightResultHeader header = 1;
    FlightResultFooter footer = 2;
  }
}
```

The client reads this before processing any VSR:
```scala
// FlightPlanDispatcher.scala
if (meta.hasHeader) {
  resultSchema = Some(meta.getHeader.getResultSchema.fromProto)
}
```

### Data Frames: `VectorSchemaRoot` batches

Each batch is an Arrow record batch transferred using `VectorUnloader` / `VectorLoader` (zero-copy transfer of the Arrow IPC record-batch).

On the **server**, two streaming paths exist inside `FlightQueryResultStreaming.streamResults`:

**Path A – Direct VSR pass-through (fast path)**  
Used when all result `RangeVector`s are already `ArrowSerializedRangeVector` instances (i.e., the plan node received Arrow data from a child node without re-serializing).  The server calls `VectorUnloader.getRecordBatch` and `VectorLoader.load` to copy the batch into the shared `flightVsr` and calls `listener.putNext()`.  No per-row deserialization occurs.

**Path B – On-demand serialization (slow path)**  
Used when result `RangeVector`s are `SerializableRangeVector` (regular rows from a leaf scan or aggregation node).  Row iteration is offloaded to the `QueryScheduler` (CPU-bound work); the resulting finished VSRs are then transferred to the `FlightIoScheduler` to call `listener.putNext()`.

On the **client**, each `putNext` batch is moved into a per-request `BufferAllocator` via `VectorUnloader` / `VectorLoader` so it can be released independently after query result processing completes.

### Footer: `FlightResultFooter`

Sent as the **last** `putMetadata` call after all data batches:

```protobuf
message FlightResultFooter {
  QueryResultStats queryStats  = 1;
  optional Throwable throwable = 2;   // present only on error
}
```

If `throwable` is present the client constructs a `QueryError`; otherwise it constructs a `QueryResult`.  The footer is **always** sent—even on error—so the client always receives `QueryStats`.

---

## ArrowSerializedRangeVector

`ArrowSerializedRangeVector` (in `core/src/main/scala/filodb.core/query/RangeVector.scala`) is the in-process representation on the client side.  It wraps a slice of the received VSRs without copying row data.

```scala
class ArrowSerializedRangeVector(
  val key:              RangeVectorKey,
  val vsrs:             Seq[VectorSchemaRoot],  // shared reference to all received VSRs
  val schema:           RecordSchema,
  val startVsrIndex:    Int,                    // first VSR that contains rows for this RV
  val rvkRowIndex:      Int,                    // row index of the RV key row within startVsrIndex
  val numRowsSerialized: Int,
  val outputRange:      Option[RvRange]         // (startMs, stepMs, endMs) for the time axis
) extends RangeVector with SerializableRangeVector
```

Its `rows()` cursor iterates across VSR boundaries transparently:
- Skips the `isRvk=1` key row at `rvkRowIndex`.
- For each subsequent `isRvk=0` row reads the binary record address via the VarBinary offset buffer (no copy, no object creation).
- Handles null rows (filtered NaN / empty histogram) by returning a synthetic `TransientRow` at the correct timestamp using `outputRange.stepMs`.
- Advances to the next VSR automatically when the current one is exhausted.

Multiple `ArrowSerializedRangeVector` instances from one response share the same `Seq[VectorSchemaRoot]`—only one copy of the raw bytes exists in the `FlightAllocator`-owned off-heap memory.

`ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs` rebuilds the list of `ArrowSerializedRangeVector` objects on the client by scanning the `isRvk` column to locate RV boundaries without re-deserializing any data row.

---

## Serialization Stack

### Kryo – ExecPlan tickets (single-partition)

`FlightKryoSerDeser` maintains a CPU-sized pool of `Kryo` instances (thread-safe borrow/return via `MpmcArrayQueue`) to avoid lock contention.  Each `Kryo` context is pre-registered with all known `ExecPlan` subclasses and standard Scala collection types.

```scala
// FlightPlanDispatcher.scala
val ticket = plan.execPlan match {
  case remoteExec: PromQLFlightRemoteExec => new Ticket(remoteExec.grpcRequest.toByteArray)
  case otherExec                          => new Ticket(FlightKryoSerDeser.serializeToBytes(otherExec))
}
```

### Protobuf – Request tickets (multi-partition)

`PromQLFlightRemoteExec.grpcRequest` builds a `GrpcMultiPartitionQueryService.Request` Protobuf message containing:

```protobuf
message Request {
  QueryParams   queryParams                      = 1;  // PromQL, start, end, step
  PlannerParams plannerParams                    = 2;  // timeout, limits (processMultiPartition=false)
  string        dataset                          = 3;
  string        plannerSelector                  = 4;  // selects the query planner on the server
  optional string flightResponseAcceptVersion    = 5;  // version negotiation (currently "1.0")
}
```

### Direct-write row encoding (data rows)

BinaryRecord rows are written **directly** into the VarBinary data buffer of the VSR without an intermediate heap array:

```scala
// ArrowSerializedRangeVectorOps.scala
srb.reset(UnsafeUtils.ZeroPointer,
          currentRvkBrVec.getDataBufferAddress + writeOffset,
          bytesRemaining)
srb.addFromReader(row, recordSchema, 0)
```

The `commitRow` helper updates the Arrow offset buffer manually rather than calling `VarBinaryVector.set()`, because `set()` calls `fillHoles()` which would overwrite existing valid bytes.  The invariant is: `offsetBuffer[i+1] = offsetBuffer[i] + bytesWritten` for non-null rows, and `offsetBuffer[i+1] = offsetBuffer[i]` + validity-bit cleared for null rows.

---

## Response Versioning

Version negotiation exists **only for multi-partition requests** because a single partition clients are typically deployed concurrently with the server and can be upgraded together.  Multi-partition requests may be sent to a remote server that is running an older version server. This is precisely why multi-partition ticket is a structured Protobuf `Request` with an explicit version field.  Single-partition tickets carry an opaque Kryo blob and do not version the response format.

### Current version: `"1.0"`

```scala
// FlightQueryResultStreaming.scala
object FlightQueryResultStreaming {
  val ACCEPT_RESPONSE_VERSION1 = "1.0"
}
```

The client sets it unconditionally:
```scala
// PromQLFlightRemoteExec.scala
Request.newBuilder()
  ...
  .setFlightResponseAcceptVersion(FlightQueryResultStreaming.ACCEPT_RESPONSE_VERSION1)
  .build()
```

The server rejects requests with an unsupported or missing version:
```scala
// FiloDBMultiPartitionFlightProducer.scala
require(request.hasFlightResponseAcceptVersion &&
        request.getFlightResponseAcceptVersion == ACCEPT_RESPONSE_VERSION1,
  s"Unsupported FlightResponse Accept Version …")
```

### How future versions would work

To introduce `"2.0"`:

1. Define `ACCEPT_RESPONSE_VERSION2 = "2.0"` in `FlightQueryResultStreaming`.
2. Extend `FiloDBMultiPartitionFlightProducer.getStream` to branch on the accepted version:
   ```scala
   val version = request.getFlightResponseAcceptVersion
   if (version == ACCEPT_RESPONSE_VERSION1) streamV1(...)
   else if (version == ACCEPT_RESPONSE_VERSION2) streamV2(...)
   else listener.error(new UnsupportedOperationException(...))
   ```
3. Old clients sending `"1.0"` continue to work against a server that understands both versions.
4. Old servers reject new clients sending `"2.0"` with a clear error rather than silent corruption.

The `optional string flightResponseAcceptVersion` field (field 5 in `Request`) was added precisely to allow this negotiation without breaking the existing field layout.

---

## Client: FlightPlanDispatcher and FlightClientManager

### Connection pooling and health-checking

`FlightClientManager` maintains a singleton `ConcurrentHashMap<location → FlightClientEntry>`.  Each entry holds:
- A `FlightClient` (backed by a Netty HTTP/2 channel).
- An `AtomicBoolean isHealthy` flag.
- An `AtomicLong lastHealthCheck` timestamp.

A single-thread Monix scheduler (`flight-client-health-checker`) probes every known client at a configurable interval via `client.listActions()`.  If the probe fails, `isHealthy` is set to false; the next `getClient` call then calls `reconnectClient`, which closes the old channel and opens a new one.

```scala
// FlightClientManager.scala
def getClient(location: Location, forceRebuild: Boolean = false): FlightClient = {
  val entry = clientMap.computeIfAbsent(locationKey, l => createNewClientEntry(location))
  if (forceRebuild || (entry.lastHealthCheck.get > 0 && !entry.isHealthy.get))
    reconnectClient(entry, location)
  else
    entry.client
}
```

Connection errors in `FlightPlanDispatcher` (`ConnectException`, `IOException`) trigger `forceRebuild = true`; `FlightRuntimeException` with `TIMED_OUT` status is converted to `QueryTimeoutException` for the circuit breaker.

### Memory isolation with `FlightAllocator`

#### Allocator hierarchy

Arrow memory in FiloDB is managed through a tree of `BufferAllocator` instances rooted at a single `RootAllocator`:

```
RootAllocator  (filodb.flight.root-allocator-max-memory)
  ├── FilodbFlightServer  (filodb.flight.server.allocator-limit)
  │     └── query-flight-producer-req-<planId>   ← one child per in-flight server request
  │           └── VSR buffers, header/footer ArrowBufs, …
  └── FilodbFlightClient  (filodb.flight.client.allocator-limit)
        └── per-request child allocator           ← one child per in-flight client request
              └── received VSR buffers
```

`FlightAllocator.serverAllocator` and `FlightAllocator.clientAllocator` are lazy `val`s created once from the shared root, each capped by its own config limit.  Metrics (`flight-allocated-memory`, `flight-used-memory`) are incremented/decremented via `AllocationListener` hooks on both the server and client allocators.

#### Server-side: per-request child allocator

At the start of every `executePhysicalPlanAndRespond` call, `FlightQueryResultStreaming` creates a fresh child allocator bounded by `perReqAllocatorLimit` (default 256 MB) and wraps it in a `FlightAllocator`:

```scala
// FlightQueryResultStreaming.scala – executePhysicalPlanAndRespond
val reqAllocator = serverAllocator.newChildAllocator(
  s"query-flight-producer-req-${q.planId}", 0, perReqAllocatorLimit)
val flightAllocator = new FlightAllocator(reqAllocator)
```

`FlightAllocator` guards the underlying `BufferAllocator` with a `ReentrantReadWriteLock`:
- **`withRequestAllocator(f)(ifClosed)`** acquires the **read lock** before calling `f(allocator)`.  Multiple concurrent threads (e.g., the query scheduler serializing rows and the Flight I/O thread sending VSRs) can hold read locks simultaneously.
- **`close()`** acquires the **write lock**, drains the registered `closeables` queue, then calls `allocator.close()`.  If any bytes remain allocated at close time the JVM is halted (`Shutdown.haltAndCatchFire`) to surface memory leaks immediately in testing and staging.
- **`registerCloseable(c)`** enqueues a resource (e.g., a `VectorSchemaRoot`) to be closed when `FlightAllocator.close()` is called.  It requires the read lock to already be held so that registration and close are mutually exclusive.

The `FlightAllocator` is then placed in the `QuerySession` and a `preventRangeVectorSerialization = true` flag is set, which suppresses the normal Kryo-based `SerializedRangeVector` materialization in `ExecPlan`:

```scala
// FlightQueryResultStreaming.scala
val querySession = QuerySession(
  q.queryContext,
  queryConfig,
  flightAllocator              = Some(flightAllocator),
  preventRangeVectorSerialization = true,  // skip Kryo SRV; Flight does its own Arrow encoding
  catchMultipleLockSetErrors   = true)
```

#### QuerySession as the carrier through the ExecPlan tree

`QuerySession` is the single object that threads execution-wide state across the entire `ExecPlan` tree on one node.  It is passed by reference (not copied) through every `execute` / `doExecute` / `doExecuteStreaming` call:

```
executePhysicalPlanAndRespond(execPlan, listener)
  └── executePlan(q, querySession)                  // implemented by each producer subclass
        └── q.execute(memStore, querySession)        // single-partition path
            q.dispatcher.dispatch(…, querySession)   // multi-partition path
              └── child.doExecute(source, querySession)
                    └── grandchild.doExecute(source, querySession)
                          └── leaf scan / aggregation …
```

Every node in the tree can therefore read `querySession.flightAllocator` to allocate Arrow buffers, accumulate `querySession.queryStats`, check `querySession.preventRangeVectorSerialization` to decide whether to materialize rows into Kryo `SerializedRangeVector`s or leave them as raw `RangeVector`s for the Flight serialization path.

When a non-leaf plan dispatches a child to a **remote** node (via `FlightPlanDispatcher` or `ActorPlanDispatcher`), the `QuerySession` is **not** serialized and sent over the wire—it lives only on the originating node.  The remote node creates its own `QuerySession` for its local execution subtree.

#### Lifecycle and guaranteed close

The `QuerySession` (and through it, the `FlightAllocator` and its child `BufferAllocator`) is closed in the Monix `guarantee` block of the request task, which runs **regardless** of whether the task succeeded, failed, or was cancelled:

```scala
// FlightQueryResultStreaming.scala
circuitBreaker.protect(execTask)
  .onErrorRecover { case t => sendRespFooterAndComplete(…, Some(t)) }
  .guarantee(Task.eval {
    SerializedRangeVector.queryCpuTime.increment(querySession.queryStats.totalCpuNanos)
    execPlanLatency.record(…)
    querySpan.finish()
    querySession.close()   // ← closes FlightAllocator → closes reqAllocator → frees all VSR buffers
  })
  .runToFuture(QueryScheduler.flightIoScheduler)
```

`QuerySession.close()` in turn calls `flightAllocator.foreach(_.close())`, which:
1. Acquires the write lock (blocking until all concurrent `withRequestAllocator` read locks are released).
2. Closes every `AutoCloseable` registered via `registerCloseable` (e.g., VSRs held by the client).
3. Calls `allocator.close()` on the per-request child `BufferAllocator`, returning all Arrow off-heap memory to the parent `FilodbFlightServer` allocator.
4. Sets `closed = true` so any subsequent `withRequestAllocator` call takes the `ifClosed` branch rather than allocating into a dead allocator.

The `guarantee` placement means VSR memory is reclaimed promptly after each query completes, with no dependency on GC.

#### Client-side allocator and close

On the client (`FlightPlanDispatcher.executeFlightRequest`), the `FlightAllocator` is read from the `QuerySession` that was passed in from the caller:

```scala
// FlightPlanDispatcher.scala – executeFlightRequest
require(querySession.flightAllocator.isDefined, "FlightAllocator must be provided …")
val flightAllocator = querySession.flightAllocator.get

// For each putNext batch received from the server:
flightAllocator.withRequestAllocator { requestAllocator =>
  flightAllocator.checkAllocatorLimits(plan.queryContext)
  val reqVsr = VectorSchemaRoot.create(arrowSrvSchema, requestAllocator)
  flightAllocator.registerCloseable(reqVsr)          // owned by this allocator
  val loader = new VectorLoader(reqVsr)
  Using.resource(new VectorUnloader(stream.getRoot).getRecordBatch) { rb =>
    loader.load(rb)                                   // copy from stream's buffer to reqVsr
  }
  vsrs += reqVsr
} {
  // ifClosed branch: allocator already gone → cancel the stream
  stream.cancel("Cancelling due to closed FlightAllocator", null)
  canceled = true
}
```

The received VSRs are registered as closeables on the caller's `FlightAllocator`.  When the outer `QuerySession.close()` fires (in the caller's `guarantee` block), all client-side VSRs are closed together with the server-side ones—there is no separate lifecycle management required.

If `Monix.mapParallelUnordered` fails one task early (e.g., circuit-breaker rejection) while others are still receiving VSR batches, those concurrent tasks encounter a closed allocator and take the `ifClosed` branch rather than allocating into freed memory.  This is the main reason `FlightAllocator` uses a read-write lock rather than a simple `AtomicBoolean`.

---

## Compression

`ZstdGrpc.scala` provides a `ZstdCompressor` / `ZstdDecompressor` pair registered in gRPC's codec registry.  When `filodb.flight.compression-enabled = true`:

- **Server**: `ZstdServerInterceptor` inspects `grpc-accept-encoding` on each call and calls `call.setCompression("zstd")` if the client declares support.
- **Client**: `ZstdClientInterceptor` injects `grpc-accept-encoding: zstd` into every outbound call and sets `callOptions.withCompression("zstd")`.

Compression is applied at the HTTP/2 frame level (gRPC message framing), so both Arrow data batches and metadata buffers are compressed transparently.

---
## Flight RPC Query Planning

Enable Flight RPC using FlightPlanDispatcher on query planners to use Flight using `SingleClusterPlanner.flightEnabled` or `MultiPartitionPlanner.fightEnabled` constructor argument.

For remote partitions, MultiPartitionPlanner will generate `PromQLFlightRemoteExec` if `queryConfig.flightPartitionsDenyList` allows for it. 
The `PromQLFlightRemoteExec`'s execution will delegate to a FlightPlanDispatcher.  FlightPlanDispatcher will use the correct request payload based on the exec plan.
If `PromQLFlightRemoteExec` needs to be dispatched, then it will use the multi-partition request payload.  Otherwise it will use the single-partition flight request payload.

The Flight server port for single cluster client is always `akkaPort + 5000` (see `FiloDBSinglePartitionFlightProducer.akkaPortToFlightPort`).
This is a temporary measure until the Akka and Flight servers are fully decoupled.

---
## Protocol Comparison: Arrow Flight vs Akka vs gRPC

### Single-partition: Arrow Flight vs Akka

| Dimension | Akka Actor Dispatcher                                                                  | Arrow Flight                                                                                               |
|-----------|----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| **Protocol** | Akka remoting over TCP                                                                 | HTTP/2 (Flight over gRPC)                                                                                  |
| **Request encoding** | Kryo or Protobuf `ExecPlan` via Akka `ask`                                             | Kryo `ExecPlan` in Arrow Flight `Ticket`                                                                   |
| **Response encoding** | Kryo-serialized `QueryResult` / `QueryResponse` heap objects                           |  Arrow VSR batches (off-heap) + Protobuf metadata                                                          |
| **Data layout** | Row-oriented `BinaryRecord`s copied to a heap `Array[Byte]` in `SerializedRangeVector` | Row-oriented `BinaryRecord`s written directly into the VarBinary data buffer of a VSR—no intermediate copy |
| **Streaming** | Single `Future[QueryResponse]`; all rows materialized before reply                     | True streaming: rows flow as VSR batches as soon as they are produced                                      |
| **Back-pressure** | None (Akka ask returns one message)                                                    | HTTP/2 flow control per stream                                                                             |
| **Memory accounting** | On-heap allocation                                                                     | Per-request child `BufferAllocator` with configurable hard limit                                           |
| **GC pressure** | High: each row produces a `BinaryRecord` heap wrapper in the result                    | Low: results kept in off-heap Arrow buffers until consumed                                                 |
| **Circuit breaker** | Must be added manually around `ask`                                                    | Built-in via Monix `CircuitBreaker` in `FlightQueryResultStreaming`                                        |
| **Connection reuse** | Akka persistent TCP                                                                    | Long-lived HTTP/2 channel per (host, port)                                                                 |

**Key improvement**: Akka places the entire `QueryResult` (all rows) one Akka message before serializing (with RecordBuilder) and returning it to the caller.  Flight streams VSR batches as soon as they are ready, enabling the client to begin processing earlier and bounding the maximum in-flight memory per batch. Off-heap memory is used for queries and is not subject to GC pressure.

### Multi-partition: Arrow Flight vs gRPC

| Dimension                           | gRPC Dispatcher                                                                | Arrow Flight                                                                                                                                   |
|-------------------------------------|--------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| **Protocol**                        | HTTP/2 gRPC streaming                                                          | HTTP/2 Arrow Flight (`getStream`)                                                                                                              |
| **Request encoding**                | Protobuf `RemoteExecPlan` (physical plan + QueryContext)                       | Protobuf `Request` (PromQL string + PlannerParams)                                                                                             |
| **Server action on request**        | Execute pre-built physical plan                                                | Re-plan from PromQL then execute                                                                                                               |
| **Response encoding**               | `StreamingBodyResponse` with `SerializableRangeVector` repeated Protobuf field | Arrow VSR batches (off-heap)                                                                                                                   |
| **Data copy**                       | Protobuf bytes → JVM heap `ByteString` → on-heap result objects                | Binary records written directly into off-heap Arrow data buffer; received VSRs moved into per-request allocator via `VectorLoader` (zero-copy) |
| **Compression**                     | gRPC built-in gzip/deflate                                                     | Pluggable; Zstandard available via `ZstdGrpc.scala`                                                                                            |
| **Version negotiation**             | None                                                                           | `flightResponseAcceptVersion` field in `Request`                                                                                               |

**Key improvement**: gRPC serializes each `RangeVector` as a full Protobuf message on the heap.  Flight writes BinaryRecord bytes directly into the off-heap VarBinary buffer and sends the buffer as an Arrow record batch, eliminating two heap allocations per row (Protobuf byte array + result object).  For a typical 12-byte double-column row and a query returning 10 M samples, this reduces heap allocations from ~240 MB to near zero on the hot data path. Off-heap memory is used for queries and is not subject to GC pressure.

---

## Configuration Reference

Look in `filodb-defaults.conf` for the following config:

```hocon
filodb {
  flight {
    // several configs documented in filodb-defaults.conf
  }
  grpc {
    flight-multi-partition-service-enabled = false
  }
  query {
    grpc {
      flight {
        // several configs documented in filodb-defaults.conf
      }
    }
  }
  memstore {
    memory-alloc {
      flight-rpc-memory-percent = 20
    }
  }
}
```
---

## Related Code

| File | Purpose |
|------|---------|
| `coordinator/src/main/scala/filodb/coordinator/flight/FiloDBSinglePartitionFlightProducer.scala` | Flight server for single-partition queries; deserializes Kryo tickets and executes `ExecPlan`s directly |
| `coordinator/src/main/scala/filodb/coordinator/flight/FiloDBMultiPartitionFlightProducer.scala` | Flight server for multi-partition queries; parses PromQL, materializes and dispatches `ExecPlan`s |
| `coordinator/src/main/scala/filodb/coordinator/flight/FlightQueryResultStreaming.scala` | Shared streaming logic: circuit breaker, header/footer protocol, VSR pass-through and serialization paths |
| `coordinator/src/main/scala/filodb/coordinator/flight/FlightPlanDispatcher.scala` | Client-side dispatcher; creates tickets, streams VSRs, assembles `QueryResult` |
| `coordinator/src/main/scala/filodb/coordinator/flight/FlightClientManager.scala` | Connection pool with background health-checking and automatic reconnection |
| `coordinator/src/main/scala/filodb/coordinator/flight/ArrowSerializedRangeVectorOps.scala` | Arrow VSR schema definition, row encoding (`populateRvContentsIntoVsrs`), and client-side reassembly (`convertVsrsIntoArrowSrvs`) |
| `coordinator/src/main/scala/filodb/coordinator/flight/FlightProtoSerDeser.scala` | Protobuf serialization of `RvKey`, `RvMetadata`, header, and footer into Arrow buffers |
| `coordinator/src/main/scala/filodb/coordinator/flight/FlightKryoSerDeser.scala` | Thread-safe Kryo pool for `ExecPlan` serialization in single-partition tickets |
| `coordinator/src/main/scala/filodb/coordinator/flight/PromQLFlightRemoteExec.scala` | `RemoteExec` leaf plan that builds a `Request` Protobuf ticket for multi-partition Flight RPC |
| `coordinator/src/main/scala/filodb/coordinator/flight/ZstdGrpc.scala` | Zstandard codec and gRPC interceptors for Flight channel compression |
| `core/src/main/scala/filodb.core/query/RangeVector.scala` (class `ArrowSerializedRangeVector`) | Zero-copy client-side `RangeVector` backed by Arrow VSR slices |
| `grpc/src/main/protobuf/query_service.proto` | Protobuf definitions for `Request`, `FlightResultHeader`, `FlightResultFooter`, `FlightMetadata`, `RvMetadata` |


---

## Future Improvements Enabled by Versioning

The versioning mechanism (`flightResponseAcceptVersion`) allows the response format to evolve without breaking deployed clients:

1. **Native label dictionary encoding (v2.0)**  
   Replace repeated UTF-8 label bytes in `RvKey` with Arrow dictionary-encoded `Utf8` vectors, reducing per-RV key overhead for high-cardinality queries.
