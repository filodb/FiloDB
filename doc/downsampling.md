# Downampling

Downsampling is a process of reducing the number of samples stored for a time series. 
This is necessary for the following reasons:

* to control storage requirements at scale 
* because most of ingested data is never queried and we would like to reduce storage
* Even if we are able to store data at highest granularity for very long periods, 
  presenting the data on a screen with limited pixel count requires it to be downsampled 
  when larger time range trends are to be presented.
* Processing large amount of samples over long periods (months/year) with query engine is
  inefficient anyway. Working with data downsampled over time is much faster.

FiloDB supports downsampling of ingested time series data at configurable resolutions. 
Multiple downsampling algorithms can be configured. 

## Configuration

Each data column of the dataset can be configured with one or more downsamplers. Following downsamplers are supported:

* min
* max
* sum
* count
* average
* timestamp (used for the row-key timestamp column)

Downsampling is configured at the time of dataset creation. For example:

```
./filo-cli -Dconfig.file=conf/timeseries-filodb-server.conf  --command create --dataset prometheus --dataColumns timestamp:ts:timestamp,value:double:min%max%sum%count --partitionColumns tags:map --shardKeyColumns __name__,app
```

The column `value` is configured with the min, max, sum and count downsamplers.
The downsamplers are separated with the `%` character.

Additional configuration is supplied via the ingestion config file for the dataset.

```
    dataset = "prometheus"
    // other config ommitted
    sourceconfig {
      // other config ommitted
      downsample {
        # can be disabled by setting this flag to false
        enabled = true
        # array of integers representing one or more downsample intervals in millisecond
        resolutions-ms = [ 60000 ]
        # class implementing the dispatch of downsample metrics to another dataset
        publisher-class = "filodb.kafka.KafkaDownsamplePublisher"
        publisher-config {
          # kafka properties that will be used for the producer
          kafka {
            bootstrap.servers = "localhost:9092"
            group.id = "filo-db-timeseries-downsample"
          }
          # map of millisecond resolution to the kafka topic for publishing downsample data
          # should have one topic per defined resolution above
          topics {
            60000 = "timeseries-dev-ds-1m"
          }
          # maximum size of in-memory queue of record containers to dispatch to kafka
          max-queue-size = 5000
          # minimum size of in-memory queue of record containers to dispatch to kafka
          min-queue-size = 100
          # maximum number of containers to consume from in-memory queue at a time
          consume-batch-size = 100
        }
      }
    }
```

The downsample dataset needs to be created as follows with the downsample columns in the same
order as the downsample type configuration. For the above example, we would create the downsample
dataset as:

```
./filo-cli -Dconfig.file=conf/timeseries-filodb-server.conf  --command create --dataset prometheus_ds_1m --dataColumns timestamp:ts,min:double,max:double,sum:double,count:double --partitionColumns tags:map --shardKeyColumns __name__,app
```

Note that there is no downsampling configuration here in this dataset. Cascading the downsampling
can be done, but its implications should be understood. Please read further.  

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
* One column for each downsample algorithm type chosen. In this case we would have 4
more columns besides the timestamps.
* Each row would be assigned the same partition key as the time series parition the chunk
belonged to.

IMPORTANT: As a result of the downsample data being generated at chunk flush time, there may be a
delay in the generation of downsample data, and it may extend up to the dataset's flush
interval. Hence if you are thinking of cascading the downsample resolutions, you need to
accept the delay in generation. Also remember to choose associative algorithms. For example,
`avg` downsampling will not be accurate when done in a cascaded fashion since average of averages
is not the correct average.   

## Querying of Downsample Data

Downsampled data can be queried from the downsampled dataset. The  PromQL tag filter needs to
include the `__col__` tag with the value of the column name chosen in the downsample dataset. 