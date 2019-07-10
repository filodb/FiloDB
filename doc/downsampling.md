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

Downsampling is configured at the time of dataset creation. For example:

```
./filo-cli -Dconfig.file=conf/timeseries-filodb-server.conf  --command create --dataset prometheus --dataColumns timestamp:ts,value:double --partitionColumns tags:map --shardKeyColumns __name__,_ns --downsamplers "tTime(0),dMin(1),dMax(1),dSum(1),dCount(1),dAvg(1)"
```

In the above example, the data column `value` with index 1 is configured with the dMin, dMax, sSum, dCount and dAvg downsamplers.

Additional configuration like downsample resolutions, how/where data is to be published etc.
is supplied via the ingestion config file for the dataset. See 
[timeseries-dev-source.conf](../conf/timeseries-dev-source.conf) for more details.


The downsample dataset needs to be created as follows with the downsample columns in the same
order as the downsample type configuration. For the above example, we would create the downsample
dataset as:

```
./filo-cli -Dconfig.file=conf/timeseries-filodb-server.conf  --command create --dataset prometheus_ds_1m --dataColumns timestamp:ts,min:double,max:double,sum:double,count:double,avg:double --partitionColumns tags:map --shardKeyColumns __name__,_ns
```

Note that there is no downsampling configuration here in the above dataset. Note that partition
key for the downsample dataset is the same as the raw dataset's.  

## Downsample Data Generation
Downsampling is done at chunk flush time, when data is moved out of write buffers into block memory and cassandra.
At this time, we iterate through all of the assigned downsampler algorithms and resolution
combinations and do a query on the chunk for each period.

For example, if the chunk spans from 10:07am to 11:08am with 10-secondly data 
and we were to downsample (min, max, sum, count) at 15m interval, we'd
have the following data

* One row each corresponding to the 10:15am, 10:30am, 10:45am, 11am, 11:15am periods.
* Timestamp on those rows would be the last timestamp found in the raw data for those
periods.
* One column for each downsampler chosen. In this case we would have 4 more data columns in
the downsample dataset besides the timestamp.
* Each row would be assigned the same partition key as the time series parition the chunk
belonged to.

## Best Practices

* As a result of the downsample data being generated at chunk flush time, there may be a
delay in the generation of downsample data, and it may extend up to the dataset's flush
interval. Hence if you are thinking of cascading the downsample resolutions, you need to
accept the delay in generation.
* Cascading of downsampling can be done, but the implications should be understood clearly. For example,
5m downsample is calculated from 1m data, 15m is calculated from 5m, 1hr is calculated from 15m etc.
This is possible, but remember to choose the right downsamplers. For example, `dAvg` downsampling will
not be accurate when done in a cascaded fashion since average of averages is not the correct average.
Instead you would choose to calculate average using average and count columns by using the `dAvgAc`
downsampler on the downsampled dataset.   

## Querying of Downsample Data
 
Downsampled data for individual time series can be queried from the downsampled dataset. The PromQL
filters in the query needs to include the `__col__` tag with the value of the downsample column name
chosen in the downsample dataset. For example `heap_usage{_ns="myApp" __col__="avg"}`

Coming soon in subsequent PR: Automatic selection of column based on the time window function applied in the query.

## Validation of Downsample Results

Run main class filodb.prom.downsample.GaugeDownsampleValidator with following system property arguments:

```
-Dquery-endpoint=https://myFiloDbEndpoint.com
-Draw-data-promql=jvm_threads{_ns=\"myApplication\",measure=\"daemon\",__col__=\"value\"}[@@@@s]
-Dflush-interval=12h
-Dquery-range=6h

raw-data-promql property value should end with ',__col__="value"}[@@@@s]'.
The lookback window is replaced by validation tool when running the query.
```

This will perform validation of min, max,, sum and count downsamplers by issuing same query to both datasets
and making sure results are consistent.
