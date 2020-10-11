<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Downsampling](#downsampling)
  - [Configuration](#configuration)
  - [Downsample Data Generation](#downsample-data-generation)
  - [Best Practices](#best-practices)
  - [Querying of Downsample Data](#querying-of-downsample-data)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Downsampling

Downsampling is a process of reducing the number of samples stored for a time series. 
This is necessary for the following reasons:

* To control storage requirements at scale 
* Because most of ingested data is never queried and we would like to reduce storage
* Even if we are able to store data at highest granularity for very long periods, 
  presenting the data on a screen with limited pixel count requires it to be downsampled 
  when larger time range trends are to be presented.
* Processing large amount of samples over long periods (months/year) with query engine is
  inefficient anyway. Working with downsampled data is much faster.

Note that downsampling process does not reduce the number of time series stored. It reduces
the number of samples stored for a time series per unit time.

FiloDB supports downsampling of ingested time series data at configurable resolutions. 
Multiple downsampling algorithms can be configured. 

## Configuration

A dataset can be configured with one or more downsamplers. Following downsamplers are supported:

* __dMin(col):__ Calculates minimum of values in column id 'col' in the downsample period.
* __dMax(col):__ Calculates maximum of values in column id 'col' in the downsample period.
* __dSum(col):__ Calculates sum of values in column id 'col' in the downsample period.
* __dCount(col):__ Calculates count of values in column id 'col' in the downsample period.
* __dAvg(col):__ Calculates average of values in column id 'col' in the downsample period.
* __dAvgAc(avgCol,countCol):__ Calculates average using averages and counts in column ids 'avgCol' and 'countCol' respectively in the downsample period.
* __dAvgSc(sumCol,countCol):__ Calculates average using sums and counts in column ids 'sumCol' and 'countCol' respectively in the downsample period.
* __tTime(col):__ Chooses last timestamp of ingested sample for the downsample period 

Downsamplers beginning with 'd' emit a double value in the downsampled record. Downsamplers beginning with 't'
emit timestamp value. One could later have downsamplers that emit histogram or long columns too.

Downsampling for prometheus counters will come soon.

Downsampling is configured from the Schema.

For example, a schema will contain downsampler config as follows:

```
  schemas {
    gauge {
      # Each column def is of name:type format.  Type may be ts,long,double,string,int
      # The first column must be ts or long
      columns = ["timestamp:ts", "value:double:detectDrops=false"]

      # Default column to query using PromQL
      value-column = "value"

      # Downsampling configuration.  See doc/downsampling.md
      downsamplers = [ "tTime(0)", "dMin(1)", "dMax(1)", "dSum(1)", "dCount(1)", "dAvg(1)" ]

      # If downsamplers are defined, then the downsample schema must also be defined
      downsample-schema = "ds-gauge"
    }
  }
```

In the above example, the data column `value` with index 1 is configured with the dMin, dMax, sSum, dCount and dAvg downsamplers.

Additional configuration like downsample resolutions, how/where data is to be published etc.
is supplied via the ingestion config file for the dataset. See 
[timeseries-dev-source.conf](../conf/timeseries-dev-source.conf) for more details.

## Downsampled Data Computation

This happens in a spark job that runs every 6 hours. There are two jobs:
1. First Spark job downsamples the data ingested in last 6 hours by reading the raw data cassandra keyspace,
   compresses it into one chunkset for each time series partition and writes them to the Cassandra 
   tables of downsample keyspace. See [DownsamplerMain](spark-jobs/src/main/scala/filodb/downsampler/chunk/DownsamplerMain.scala)
   for more details on how it works.
2. Second Spark job copies the partition keys table updates from raw data keyspace, and copies any modified entries into the
   downsample keyspace tables.

## Querying of Downsample Data

A separate FiloDB cluster setup using [DonwsampledTimeSeriesStore](core/src/main/scala/filodb.core/downsample/DownsampledTimeSeriesStore.scala)
configuration allows queries of downsampled data written by the downsampler jobs to the downsample cassandra keyspace.

The FiloDB Chunk scans automatically translates queries to select the right column under the hood when querying downsampled
data. For example `min_over_time(heap_usage{_ws_="demo",_ns_="myApp"})` is roughly converted to something like
`heap_usage::min{_ws_="demo",_ns_="myApp"}` so the min column is chosen.

