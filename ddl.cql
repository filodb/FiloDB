
CREATE KEYSPACE IF NOT EXISTS filodb_admin WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE KEYSPACE IF NOT EXISTS filodb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE KEYSPACE IF NOT EXISTS filodb_downsample WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE IF NOT EXISTS filodb_admin.checkpoints (
    databasename text,
    datasetname text,
    shardnum int,
    groupnum int,
    offset bigint,
    PRIMARY KEY ((databasename, datasetname, shardnum), groupnum)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};

CREATE TABLE IF NOT EXISTS filodb.prometheus_tschunks (
    partition blob,
    chunkid bigint,
    chunks frozen<list<blob>>,
    info blob,
    PRIMARY KEY (partition, chunkid)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};

CREATE TABLE IF NOT EXISTS filodb.prometheus_ingestion_time_index (
    partition blob,
    ingestion_time bigint,
    start_time bigint,
    info blob,
    PRIMARY KEY (partition, ingestion_time, start_time)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
CREATE TABLE IF NOT EXISTS filodb.prometheus_partitionkeys_0 (
    partKey blob,
    startTime bigint,
    endTime bigint,
    PRIMARY KEY (partKey)
) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
CREATE TABLE IF NOT EXISTS filodb.prometheus_partitionkeys_1 (
    partKey blob,
    startTime bigint,
    endTime bigint,
    PRIMARY KEY (partKey)
) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
CREATE TABLE IF NOT EXISTS filodb.prometheus_partitionkeys_2 (
    partKey blob,
    startTime bigint,
    endTime bigint,
    PRIMARY KEY (partKey)
) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
CREATE TABLE IF NOT EXISTS filodb.prometheus_partitionkeys_3 (
    partKey blob,
    startTime bigint,
    endTime bigint,
    PRIMARY KEY (partKey)
) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};

CREATE TABLE IF NOT EXISTS filodb.prometheus_pks_by_update_time (
    shard int,
    epochHour bigint,
    split int,
    partKey blob,
    startTime bigint,
    endTime bigint,
    PRIMARY KEY ((shard, epochHour, split), partKey)
) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};

CREATE TABLE IF NOT EXISTS filodb_downsample.prometheus_ds_1_tschunks (
    partition blob,
    chunkid bigint,
    chunks frozen<list<blob>>,
    info blob,
    PRIMARY KEY (partition, chunkid)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};

CREATE TABLE IF NOT EXISTS filodb_downsample.prometheus_ds_1_ingestion_time_index (
    partition blob,
    ingestion_time bigint,
    start_time bigint,
    info blob,
    PRIMARY KEY (partition, ingestion_time, start_time)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};

CREATE TABLE IF NOT EXISTS filodb_downsample.prometheus_ds_5_tschunks (
    partition blob,
    chunkid bigint,
    chunks frozen<list<blob>>,
    info blob,
    PRIMARY KEY (partition, chunkid)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};

CREATE TABLE IF NOT EXISTS filodb_downsample.prometheus_ds_5_ingestion_time_index (
    partition blob,
    ingestion_time bigint,
    start_time bigint,
    info blob,
    PRIMARY KEY (partition, ingestion_time, start_time)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
CREATE TABLE IF NOT EXISTS filodb_downsample.prometheus_ds__partitionkeys_0 (
    partKey blob,
    startTime bigint,
    endTime bigint,
    PRIMARY KEY (partKey)
) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
CREATE TABLE IF NOT EXISTS filodb_downsample.prometheus_ds__partitionkeys_1 (
    partKey blob,
    startTime bigint,
    endTime bigint,
    PRIMARY KEY (partKey)
) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
CREATE TABLE IF NOT EXISTS filodb_downsample.prometheus_ds__partitionkeys_2 (
    partKey blob,
    startTime bigint,
    endTime bigint,
    PRIMARY KEY (partKey)
) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
CREATE TABLE IF NOT EXISTS filodb_downsample.prometheus_ds__partitionkeys_3 (
    partKey blob,
    startTime bigint,
    endTime bigint,
    PRIMARY KEY (partKey)
) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
