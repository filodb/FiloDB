<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [FiloDB HTTP API](#filodb-http-api)
  - [FiloDB Specific APIs](#filodb-specific-apis)
    - [GET /__health](#get-__health)
    - [GET /__seeds](#get-__seeds)
    - [GET /api/v1/cluster](#get-apiv1cluster)
    - [GET /api/v1/cluster/{dataset}/status](#get-apiv1clusterdatasetstatus)
  - [Prometheus-compatible APIs](#prometheus-compatible-apis)
    - [GET /api/v1/label/{label_name}/values](#get-apiv1labellabel_namevalues)
    - [GET /api/v1/query](#get-apiv1query)
    - [GET /api/v1/query_range](#get-apiv1query_range)
    - [GET /api/v1/series](#get-apiv1series)
  - [Prometheus APIs not supported](#prometheus-apis-not-supported)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## FiloDB HTTP API

### FiloDB Specific APIs

#### GET /__health

Currently returns `All good` if the node is up.

TODO: expose more detailed health, status, etc.?

#### GET /__members

Internal API used to return seed nodes for FiloDB Cluster initialization.  See the [akka-bootstrapper docs](akka-bootstrapper.md).

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

It is not really an error, but it means the dataset is already ingesting and there is no need to set it up.

### Prometheus-compatible APIs

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

#### GET /api/v1/query

#### GET /api/v1/query_range

#### GET /api/v1/series

### Prometheus APIs not supported

Anything not listed above.  Especially:

* GET /api/v1/targets
* GET /api/v1/alertmanagers