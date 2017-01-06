# Overview of BinaryRecord

BinaryRecord is designed to be an extremely high performance binary storage format for individual event records for the memtable and for sorting / transferring records between nodes.  Characteristics that make it unique:

* Tight integration with FiloDB Column types and RowReader
* Supports any FiloDB table schemas / flexible schemas
* Support for off heap memory/storage and memory mapped files (eg memtables)
* Support for null values
* Using RowReader, access to fields with no deserialization.  This is unique and extremely important for high performance sorting, rowkey searching for row replacement/delete functionality, etc.
    - Access to binary UTF8 string representation to avoid string encoding/decoding

Essentially, BinaryRecord is a record type that supports flexible schemas and reads/writes/usage with no serialization at all for extreme performance and low latency with minimal GC pressure.

We try to adhere to these principles for [low latency design](https://github.com/real-logic/simple-binary-encoding/wiki/Design-Principles).

## Format

* First n 32-bit words: used for representing null bit field (1=null)
* All other fields are word (32-bit) aligned
* Bool, int, float all take up 32 bits
* Double, Long 64 bits
* Strings and other blobs that don't fit in 64 bits:
    - 32 bit fixed field that points to other storage
    - If MSB (bit 31) HIGH, then bits 16-30 contain offset, bits 0-15 length
    - If MSB (bit 31) LOW, then it contains byte offset into a 32-bit length field, with the blob following the length field
    - The blob itself is stored word-aligned and padded with 00's at end if needed. This helps with efficient comparisons
    - Note the offset is relative to the start of the binaryRecord


