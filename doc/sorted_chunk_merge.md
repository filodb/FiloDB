<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Fast sorted columnar chunk merging](#fast-sorted-columnar-chunk-merging)
  - [Columnar Chunking Defined](#columnar-chunking-defined)
  - [An Interface for Columnar Chunk Merging](#an-interface-for-columnar-chunk-merging)
  - [Index Writes Within a Segment](#index-writes-within-a-segment)
  - [Optimizations and Compactions](#optimizations-and-compactions)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Fast sorted columnar chunk merging

One challenge in FiloDB is how to continuously insert fresh data into a column store and keep the data in that columnar projection sorted at the same time, yet relatively read-optimized.  Suppose that one has N columns, and in a projection data for each column is divided into relatively evenly spaced chunks.  If one had to merge fresh data into each chunk, and keep the data within each chunk sorted, it would make for a very expensive merging process -- one would need to read chunks for every column, merge in fresh data such that the old and new data is sorted together, and rewrite all the chunks.  This describes a faster mechanism for sorted columnar chunk merging.

### Columnar Chunking Defined

* The dataset is divided into partitions.  A single partition must fit entirely into one node.  One node will probably contain many partitions.
* There is a **sort key**, which must be linearizable and divisible into equal **segments**.  For example, if the sort key is timestamp, one segment might be 10 seconds of data.
* Within a single partition, the storage engine (say Cassandra) is responsible for keeping the segments in sort key sort order.
* Within a single segment, for a single column, the data is grouped into columnar chunks.  Each chunk is not aware of the sort key, it just knows it has data for a single column as a binary vector.  The chunk is the smallest unit of data read or written.

### An Interface for Columnar Chunk Merging

The ColumnStore by itself does not know how to merge chunks.  The primitives of a column store implementation, at the low level are:

* writeChunks
* writeChunkRowMap (see below)
* readChunks
* readChunkRowMaps
* scanChunkRowMaps

Instead, the higher level ColumnStore relies on a `ChunkMergingStrategy`, which knows how to do the following:

* read a segment or partial segment into memory (for a cache)
* merge the segment with a new partial segment of data to be inserted.  The combined segment is now ready to be read or written to disk.
* prune the combined segment for saving to a cache

### Index Writes Within a Segment

The key to this algorithm is that, even though segments are ordered within a partition, rows of data within a segment do not need to be written in sorted order. Chunks of data are appended, and a `ChunkRowMap`, one per segment per partition, is updated for all columns.  This `ChunkRowMap` is a sorted map from the sort key of a projection to a (chunkID, rowNum).  Basically it tells the scanning engine, in order to read the data in sorted key order, what chunks and what row # within the chunk to read from.

In terms of its `ChunkMergingStrategy`:

* Only the ChunkRowMap, and the primary key chunks, from existing segments are read
* New chunks are added to the end of a segment, and the ChunkRowMap is updated to point at the new segments.  Existing chunks are not modified.
* Only the ChunkRowMap and the primary key chunks are saved to the cache.

Implications:

* All chunks for all columns within a segment must have the same number of rows.  This guarantees the `ChunkRowMap` is column-independent.  This implies:
    - The Memtable / tuple mover must fill in NA values if not all columns in a row are being written, and make sure the same number of rows are written for every column chunk
* The `ChunkRowMap` is referenced by a `SegmentID`, within a partition, which is basically the first sort key of the sort key range of a segment
* A single chunk of columnar data is referenced by (SegmentID, ChunkID).  The ChunkID must be the same for all chunks that are flushed from the same set of rows.  The ChunkID should be monotonically increasing with successive writes. With a single writer, using a counter would lead to more efficient reads.
* Chunks are sorted by (SegmentID, ChunkID).  This guarantees that chunks can be read in update order, in case the `ChunkRowMap` needs to be reconstructed.
* The `ChunkRowMap` will need to be updated every time more chunks are flushed out.  It needs to be computed at write time to keep reads fast
* It is entirely possible that certain sets of chunks might be deprecated entirely as they will be replaced by data from newer chunks.
* Since the `ChunkRowMap` and `ChunkID` are stateful across chunks, and due to the nature of blob storage, the columnar store should only be written to by a single writer.
* The `ChunkRowMap` could be entirely reconstructed from all the columnar chunks in a segment.
* The `ChunkRowMap` needs to be read for every query, in order to account for row updates and deletes.... unless the data is append-only, there are never updates (probably impossible for at-least-once systems), and you don't care about reading in sorted order from a single segment

### Optimizations and Compactions

From time to time, all the chunks within a segment should be compacted.  After compaction, there should only be one chunk per column, with all data stored in sorted order.  In this case the `ChunkRowMap` isn't necessary anymore, leading to optimal read speeds.

The sort key does not need to be stored with the `ChunkRowMap`, if the sort key is just one of the columns, since the `ChunkRowMap` can be used to read out the sort key chunks in sorted order.   What is needed is just a compacted representation of the UUID and row numbers for each PK.
