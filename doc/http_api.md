<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [FiloDB HTTP API](#filodb-http-api)
  - [FiloDB Specific APIs](#filodb-specific-apis)
    - [GET /__members](#get-__members)
    - [GET /admin/health](#get-adminhealth)
    - [POST /admin/loglevel/{loggerName}](#post-adminloglevelloggername)
    - [GET /api/v1/cluster](#get-apiv1cluster)
    - [GET /api/v1/cluster/{dataset}/status](#get-apiv1clusterdatasetstatus)
    - [GET /api/v1/cluster/{dataset}/statusByAddress](#get-apiv1clusterdatasetstatusbyaddress)
    - [POST /api/v1/cluster/{dataset}](#post-apiv1clusterdataset)
    - [POST /api/v1/cluster/{dataset}/stopshards](#post-apiv1clusterdatasetstopshards)
    - [POST /api/v1/cluster/{dataset}/startshards](#post-apiv1clusterdatasetstartshards)
  - [Prometheus-compatible APIs](#prometheus-compatible-apis)
    - [GET /promql/{dataset}/api/v1/query_range?query={promQLString}&start={startTime}&step={step}&end={endTime}](#get-promqldatasetapiv1query_rangequerypromqlstringstartstarttimestepstependendtime)
    - [GET /promql/{dataset}/api/v1/query?query={promQLString}&time={timestamp}](#get-promqldatasetapiv1queryquerypromqlstringtimetimestamp)
    - [POST /promql/{dataset}/api/v1/read](#post-promqldatasetapiv1read)
    - [GET /api/v1/label/{label_name}/values](#get-apiv1labellabel_namevalues)
  - [Prometheus APIs not supported](#prometheus-apis-not-supported)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## FiloDB HTTP API

### FiloDB Specific APIs

#### GET /__members

Internal API used to return seed nodes for FiloDB Cluster initialization.  See the [akka-bootstrapper docs](akka-bootstrapper.md).

#### GET /admin/health

Currently returns `All good` if the node is up.

TODO: expose more detailed health, status, etc.?

#### POST /admin/loglevel/{loggerName}

Post the new loglevel (debug,info,warn,error,trace) with the logger name in dot notation.  Allows dynamically changing the log level for the node in question only.

Example to turn on TRACE logging for Kryo serialization:

    curl -d 'trace' http://localhost:8080/admin/loglevel/com.esotericsoftware.minlog

Returns text explaining what got changed (sorry not JSON).

#### GET /api/v1/cluster

* Returns a JSON list of the datasets currently set up in the cluster for ingestion

```json
{
    "status": "success",   
    "data": ["prometheus"]
}
```

#### GET /api/v1/cluster/{dataset}/status

* Returns the shard status of the given dataset
* Returns 404 if the dataset is not currently setup for ingestion

```json
{
    "status": "success",   
    "data": [
        { "shard": 0,
          "status": "ShardStatusActive",
          "address": "akka://filo-server@127.0.0.1:2552" },
        { "shard": 1,
          "status": "ShardStatusActive",
          "address": "akka://filo-server@127.0.0.1:2552" }
    ]
}
```

#### GET /api/v1/cluster/{dataset}/statusByAddress

* Returns the shard status grouped by node for the given dataset
* Returns 404 if the dataset is not currently setup for ingestion

```json
{
    "status": "success",   
    "data": [
        { "address": "akka://filo-server@127.0.0.1:2552",
          "shardList": [
                        { "shard": 0,
                          "status": "ShardStatusActive" },
                        { "shard": 1,
                          "status": "ShardStatusRecovery(94)"}
          ]
        },
        { "address": "akka://filo-server@127.0.0.1:53532",
          "shardList": [
                        { "shard": 2,
                          "status": "ShardStatusActive" },
                        { "shard": 3,
                          "status": "ShardStatusActive"}
          ]
        }
    ]
}
```

#### POST /api/v1/cluster/{dataset}

Initializes streaming ingestion of a dataset across the whole FiloDB cluster. 
The POST body describes the ingestion source and parameters, such as Kafka configuration.  Only needs to be done one time, as the configuration is persisted to the MetaStore and automatically restored on restarts.

* POST body should be an [ingestion source configuration](ingestion#basic-configuration), such as the one in `conf/timeseries-dev-source.conf`.  It could be in Typesafe Config format, or JSON.
* A successful POST results in something like `{"status": "success", "data": []}`
* 400 is returned if the POST body cannot be parsed or does not contain all the necessary configuration keys
* If the dataset has already been set up, the response will be a 409 ("Resource conflict") with

```json
{
    "status": "error",   
    "errorType": "DatasetExists"
    "error": "The dataset timeseries has already been setup for ingestion"
}
```

#### POST /api/v1/cluster/{dataset}/stopshards

Stop all the given shards.
The POST body describes the stop shard config that should have the list of shards to be stopped.

* POST body should be a UnassignShardConfig in JSON format as follows:
```json
{
    "shardList": [
       2, 3
    ]
}
```
* A successful POST results in something like `{"status": "success", "data": []}`
* 400 is returned if the POST body cannot be parsed or any of the following validation fails:
     1. If the given dataset does not exist
     3. Check if all the given shards are valid
        1. Shard number should be >= 0 and < maxAllowedShard
        2. Shard should be assigned to a node

#### POST /api/v1/cluster/{dataset}/startshards

Start the shards on the given node.
The POST body describes the start shard config that should have both destination node address and the list of shards to be started.

* POST body should be a AssignShardConfig in JSON format as follows:
```json
{
    "address": "akka.tcp://filo-standalone@127.0.0.1:2552",
    "shardList": [
       2, 3
    ]
}
```
* A successful POST results in something like `{"status": "success", "data": []}`
* 400 is returned if the POST body cannot be parsed or any of the following validation fails:
     1. If the given dataset does not exist
     2. If the given node doesn not exist
     3. Check if all the given shards are valid
        1. Shard number should be >= 0 and < maxAllowedShard
        2. Shard should not be assigned to any node
     4. Verify whether there are enough capacity to add new shards on the node

### Prometheus-compatible APIs

* Compatible with Grafana Prometheus Plugin

#### GET /promql/{dataset}/api/v1/query_range?query={promQLString}&start={startTime}&step={step}&end={endTime}

Used to issue a promQL query for a time range with a `start` and `end` timestamp and at regular `step` intervals.
For more details, see Prometheus HTTP API Documentation
[Range Queries](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries)


#### GET /promql/{dataset}/api/v1/query?query={promQLString}&time={timestamp}

Used to issue a promQL query for a single time instant `time`.  Can also be used to query raw data by issuing a PromQL
range expression. For more details, see Prometheus HTTP API Documentation
[Instant Queries](https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries)

#### POST /promql/{dataset}/api/v1/read

Used to extract raw data for integration with other TSDB systems.
* Input: ReadRequest Protobuf
* Output: ReadResponse Protobuf
See [Prometheus Remote Proto definition](https://github.com/prometheus/prometheus/blob/master/prompb/remote.proto) for more details

Important Note: The Prometheus API should not be used for extracting raw data out from FiloDB at scale. Current
implementation includes the same 'limit' settings that apply in the Akka Actor interface.   

#### GET /api/v1/label/{label_name}/values

* Returns the values (up to a limit) for a given label or tag in the internal index.  NOTE: it only searches the local node, this is not a distributed query.
* Returns 404 if there is no such label indexed

```json
{
   "status" : "success",
   "data" : [
      "node",
      "prometheus"
   ]
}
```

### Prometheus APIs not supported

Anything not listed above. Especially:

* GET /api/v1/targets
* GET /api/v1/alertmanagers