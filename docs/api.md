# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [maintenance.proto](#maintenance-proto)
    - [BackupRequest](#maintenance-v1-BackupRequest)
    - [ResetRequest](#maintenance-v1-ResetRequest)
    - [ResetResponse](#maintenance-v1-ResetResponse)
    - [RestoreInfo](#maintenance-v1-RestoreInfo)
    - [RestoreMessage](#maintenance-v1-RestoreMessage)
    - [RestoreResponse](#maintenance-v1-RestoreResponse)
  
    - [Maintenance](#maintenance-v1-Maintenance)
  
- [mvcc.proto](#mvcc-proto)
    - [Command](#mvcc-v1-Command)
    - [CommandResult](#mvcc-v1-CommandResult)
    - [Compare](#mvcc-v1-Compare)
    - [KeyValue](#mvcc-v1-KeyValue)
    - [RequestOp](#mvcc-v1-RequestOp)
    - [RequestOp.DeleteRange](#mvcc-v1-RequestOp-DeleteRange)
    - [RequestOp.Put](#mvcc-v1-RequestOp-Put)
    - [RequestOp.Range](#mvcc-v1-RequestOp-Range)
    - [ResponseOp](#mvcc-v1-ResponseOp)
    - [ResponseOp.DeleteRange](#mvcc-v1-ResponseOp-DeleteRange)
    - [ResponseOp.Put](#mvcc-v1-ResponseOp-Put)
    - [ResponseOp.Range](#mvcc-v1-ResponseOp-Range)
    - [Txn](#mvcc-v1-Txn)
  
    - [Command.CommandType](#mvcc-v1-Command-CommandType)
    - [Compare.CompareResult](#mvcc-v1-Compare-CompareResult)
    - [Compare.CompareTarget](#mvcc-v1-Compare-CompareTarget)
  
- [regatta.proto](#regatta-proto)
    - [DeleteRangeRequest](#regatta-v1-DeleteRangeRequest)
    - [DeleteRangeResponse](#regatta-v1-DeleteRangeResponse)
    - [PutRequest](#regatta-v1-PutRequest)
    - [PutResponse](#regatta-v1-PutResponse)
    - [RangeRequest](#regatta-v1-RangeRequest)
    - [RangeResponse](#regatta-v1-RangeResponse)
    - [ResponseHeader](#regatta-v1-ResponseHeader)
    - [TxnRequest](#regatta-v1-TxnRequest)
    - [TxnResponse](#regatta-v1-TxnResponse)
  
    - [KV](#regatta-v1-KV)
  
- [replication.proto](#replication-proto)
    - [MetadataRequest](#replication-v1-MetadataRequest)
    - [MetadataResponse](#replication-v1-MetadataResponse)
    - [ReplicateCommand](#replication-v1-ReplicateCommand)
    - [ReplicateCommandsResponse](#replication-v1-ReplicateCommandsResponse)
    - [ReplicateErrResponse](#replication-v1-ReplicateErrResponse)
    - [ReplicateRequest](#replication-v1-ReplicateRequest)
    - [ReplicateResponse](#replication-v1-ReplicateResponse)
    - [SnapshotChunk](#replication-v1-SnapshotChunk)
    - [SnapshotRequest](#replication-v1-SnapshotRequest)
    - [Table](#replication-v1-Table)
  
    - [ReplicateError](#replication-v1-ReplicateError)
    - [Table.Type](#replication-v1-Table-Type)
  
    - [Log](#replication-v1-Log)
    - [Metadata](#replication-v1-Metadata)
    - [Snapshot](#replication-v1-Snapshot)
  
- [Scalar Value Types](#scalar-value-types)



<a name="maintenance-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## maintenance.proto



<a name="maintenance-v1-BackupRequest"></a>

### BackupRequest
BackupRequest requests and opens a stream with backup data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [bytes](#bytes) |  | table is name of the table to stream. |






<a name="maintenance-v1-ResetRequest"></a>

### ResetRequest
ResetRequest resets either a single or multiple tables in the cluster, meaning that their data will be repopulated from the Leader.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [bytes](#bytes) |  | table is a table name to reset. |
| reset_all | [bool](#bool) |  | reset_all if true all the tables will be reset, use with caution. |






<a name="maintenance-v1-ResetResponse"></a>

### ResetResponse







<a name="maintenance-v1-RestoreInfo"></a>

### RestoreInfo
RestoreInfo metadata of restore snapshot that is going to be uploaded.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [bytes](#bytes) |  | table is name of the table in the stream. |






<a name="maintenance-v1-RestoreMessage"></a>

### RestoreMessage
RestoreMessage contains either info of the table being restored or chunk of a backup data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| info | [RestoreInfo](#maintenance-v1-RestoreInfo) |  |  |
| chunk | [replication.v1.SnapshotChunk](#replication-v1-SnapshotChunk) |  |  |






<a name="maintenance-v1-RestoreResponse"></a>

### RestoreResponse






 

 

 


<a name="maintenance-v1-Maintenance"></a>

### Maintenance
Maintenance service provides methods for maintenance purposes.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Backup | [BackupRequest](#maintenance-v1-BackupRequest) | [.replication.v1.SnapshotChunk](#replication-v1-SnapshotChunk) stream |  |
| Restore | [RestoreMessage](#maintenance-v1-RestoreMessage) stream | [RestoreResponse](#maintenance-v1-RestoreResponse) |  |
| Reset | [ResetRequest](#maintenance-v1-ResetRequest) | [ResetResponse](#maintenance-v1-ResetResponse) |  |

 



<a name="mvcc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## mvcc.proto



<a name="mvcc-v1-Command"></a>

### Command



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [bytes](#bytes) |  | table name of the table |
| type | [Command.CommandType](#mvcc-v1-Command-CommandType) |  | type is the kind of event. If type is a PUT, it indicates new data has been stored to the key. If type is a DELETE, it indicates the key was deleted. |
| kv | [KeyValue](#mvcc-v1-KeyValue) |  | kv holds the KeyValue for the event. A PUT event contains current kv pair. A PUT event with kv.Version=1 indicates the creation of a key. A DELETE/EXPIRE event contains the deleted key with its modification revision set to the revision of deletion. |
| leader_index | [uint64](#uint64) | optional | leader_index holds the value of the log index of a leader cluster from which this command was replicated from. |
| batch | [KeyValue](#mvcc-v1-KeyValue) | repeated | batch is an atomic batch of KVs to either PUT or DELETE. (faster, no read, no mix of types, no conditions). |
| txn | [Txn](#mvcc-v1-Txn) | optional | txn is an atomic transaction (slow, supports reads and conditions). |
| range_end | [bytes](#bytes) | optional | range_end is the key following the last key to affect for the range [kv.key, range_end). If range_end is not given, the range is defined to contain only the kv.key argument. If range_end is one bit larger than the given kv.key, then the range is all the keys with the prefix (the given key). If range_end is &#39;\0&#39;, the range is all keys greater than or equal to the key argument. |
| prev_kvs | [bool](#bool) |  | prev_kvs if to fetch previous KVs. |






<a name="mvcc-v1-CommandResult"></a>

### CommandResult



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| responses | [ResponseOp](#mvcc-v1-ResponseOp) | repeated | responses are the responses (if any) in order of application. |
| revision | [uint64](#uint64) |  | revision is the key-value store revision when the request was applied. |






<a name="mvcc-v1-Compare"></a>

### Compare
Compare property `target` for every KV from DB in [key, range_end) with target_union using the operation `result`. e.g. `DB[key].target result target_union.target`,
that means that for asymmetric operations LESS and GREATER the target property of the key from the DB is the left-hand side of the comparison.
Examples:
* `DB[key][value] EQUAL target_union.value`
* `DB[key][value] GREATER target_union.value`
* `DB[key...range_end][value] GREATER target_union.value`
* `DB[key][value] LESS target_union.value`


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [Compare.CompareResult](#mvcc-v1-Compare-CompareResult) |  | result is logical comparison operation for this comparison. |
| target | [Compare.CompareTarget](#mvcc-v1-Compare-CompareTarget) |  | target is the key-value field to inspect for the comparison. |
| key | [bytes](#bytes) |  | key is the subject key for the comparison operation. |
| value | [bytes](#bytes) |  | value is the value of the given key, in bytes. |
| range_end | [bytes](#bytes) |  | range_end compares the given target to all keys in the range [key, range_end). See RangeRequest for more details on key ranges.

TODO: fill out with most of the rest of RangeRequest fields when needed. |






<a name="mvcc-v1-KeyValue"></a>

### KeyValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key is the key in bytes. An empty key is not allowed. |
| create_revision | [int64](#int64) |  | create_revision is the revision of last creation on this key. |
| mod_revision | [int64](#int64) |  | mod_revision is the revision of last modification on this key. |
| value | [bytes](#bytes) |  | value is the value held by the key, in bytes. |






<a name="mvcc-v1-RequestOp"></a>

### RequestOp



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| request_range | [RequestOp.Range](#mvcc-v1-RequestOp-Range) |  |  |
| request_put | [RequestOp.Put](#mvcc-v1-RequestOp-Put) |  |  |
| request_delete_range | [RequestOp.DeleteRange](#mvcc-v1-RequestOp-DeleteRange) |  |  |






<a name="mvcc-v1-RequestOp-DeleteRange"></a>

### RequestOp.DeleteRange



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key is the first key to delete in the range. |
| range_end | [bytes](#bytes) |  | range_end is the key following the last key to delete for the range [key, range_end). If range_end is not given, the range is defined to contain only the key argument. If range_end is one bit larger than the given key, then the range is all the keys with the prefix (the given key). If range_end is &#39;\0&#39;, the range is all keys greater than or equal to the key argument. |
| prev_kv | [bool](#bool) |  | If prev_kv is set, etcd gets the previous key-value pairs before deleting it. The previous key-value pairs will be returned in the delete response. |






<a name="mvcc-v1-RequestOp-Put"></a>

### RequestOp.Put



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key is the key, in bytes, to put into the key-value store. |
| value | [bytes](#bytes) |  | value is the value, in bytes, to associate with the key in the key-value store. |
| prev_kv | [bool](#bool) |  | prev_kv if true the previous key-value pair will be returned in the put response. |






<a name="mvcc-v1-RequestOp-Range"></a>

### RequestOp.Range



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key is the first key for the range. If range_end is not given, the request only looks up key. |
| range_end | [bytes](#bytes) |  | range_end is the upper bound on the requested range [key, range_end). If range_end is &#39;\0&#39;, the range is all keys &gt;= key. If range_end is key plus one (e.g., &#34;aa&#34;&#43;1 == &#34;ab&#34;, &#34;a\xff&#34;&#43;1 == &#34;b&#34;), then the range request gets all keys prefixed with key. If both key and range_end are &#39;\0&#39;, then the range request returns all keys. |
| limit | [int64](#int64) |  | limit is a limit on the number of keys returned for the request. When limit is set to 0, it is treated as no limit. |
| keys_only | [bool](#bool) |  | keys_only when set returns only the keys and not the values. |
| count_only | [bool](#bool) |  | count_only when set returns only the count of the keys in the range. |






<a name="mvcc-v1-ResponseOp"></a>

### ResponseOp



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| response_range | [ResponseOp.Range](#mvcc-v1-ResponseOp-Range) |  |  |
| response_put | [ResponseOp.Put](#mvcc-v1-ResponseOp-Put) |  |  |
| response_delete_range | [ResponseOp.DeleteRange](#mvcc-v1-ResponseOp-DeleteRange) |  |  |






<a name="mvcc-v1-ResponseOp-DeleteRange"></a>

### ResponseOp.DeleteRange



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [int64](#int64) |  | deleted is the number of keys deleted by the delete range request. |
| prev_kvs | [KeyValue](#mvcc-v1-KeyValue) | repeated | if prev_kv is set in the request, the previous key-value pairs will be returned. |






<a name="mvcc-v1-ResponseOp-Put"></a>

### ResponseOp.Put



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| prev_kv | [KeyValue](#mvcc-v1-KeyValue) |  | if prev_kv is set in the request, the previous key-value pair will be returned. |






<a name="mvcc-v1-ResponseOp-Range"></a>

### ResponseOp.Range



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kvs | [KeyValue](#mvcc-v1-KeyValue) | repeated | kvs is the list of key-value pairs matched by the range request. kvs is empty when count is requested. |
| more | [bool](#bool) |  | more indicates if there are more keys to return in the requested range. |
| count | [int64](#int64) |  | count is set to the number of keys within the range when requested. |






<a name="mvcc-v1-Txn"></a>

### Txn



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| compare | [Compare](#mvcc-v1-Compare) | repeated | compare is a list of predicates representing a conjunction of terms. If the comparisons succeed, then the success requests will be processed in order, and the response will contain their respective responses in order. If the comparisons fail, then the failure requests will be processed in order, and the response will contain their respective responses in order. |
| success | [RequestOp](#mvcc-v1-RequestOp) | repeated | success is a list of requests which will be applied when compare evaluates to true. |
| failure | [RequestOp](#mvcc-v1-RequestOp) | repeated | failure is a list of requests which will be applied when compare evaluates to false. |





 


<a name="mvcc-v1-Command-CommandType"></a>

### Command.CommandType


| Name | Number | Description |
| ---- | ------ | ----------- |
| PUT | 0 |  |
| DELETE | 1 |  |
| DUMMY | 2 |  |
| PUT_BATCH | 3 |  |
| DELETE_BATCH | 4 |  |
| TXN | 5 |  |



<a name="mvcc-v1-Compare-CompareResult"></a>

### Compare.CompareResult


| Name | Number | Description |
| ---- | ------ | ----------- |
| EQUAL | 0 |  |
| GREATER | 1 |  |
| LESS | 2 |  |
| NOT_EQUAL | 3 |  |



<a name="mvcc-v1-Compare-CompareTarget"></a>

### Compare.CompareTarget


| Name | Number | Description |
| ---- | ------ | ----------- |
| VALUE | 0 |  |


 

 

 



<a name="regatta-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## regatta.proto



<a name="regatta-v1-DeleteRangeRequest"></a>

### DeleteRangeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [bytes](#bytes) |  | table name of the table |
| key | [bytes](#bytes) |  | key is the first key to delete in the range. |
| range_end | [bytes](#bytes) |  | range_end is the key following the last key to delete for the range [key, range_end). If range_end is not given, the range is defined to contain only the key argument. If range_end is one bit larger than the given key, then the range is all the keys with the prefix (the given key). If range_end is &#39;\0&#39;, the range is all keys greater than or equal to the key argument. |
| prev_kv | [bool](#bool) |  | If prev_kv is set, etcd gets the previous key-value pairs before deleting it. The previous key-value pairs will be returned in the delete response. |






<a name="regatta-v1-DeleteRangeResponse"></a>

### DeleteRangeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [ResponseHeader](#regatta-v1-ResponseHeader) |  |  |
| deleted | [int64](#int64) |  | deleted is the number of keys deleted by the delete range request. |
| prev_kvs | [mvcc.v1.KeyValue](#mvcc-v1-KeyValue) | repeated | if prev_kv is set in the request, the previous key-value pairs will be returned. |






<a name="regatta-v1-PutRequest"></a>

### PutRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [bytes](#bytes) |  | table name of the table |
| key | [bytes](#bytes) |  | key is the key, in bytes, to put into the key-value store. |
| value | [bytes](#bytes) |  | value is the value, in bytes, to associate with the key in the key-value store. |
| prev_kv | [bool](#bool) |  | prev_kv if true the previous key-value pair will be returned in the put response. |






<a name="regatta-v1-PutResponse"></a>

### PutResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [ResponseHeader](#regatta-v1-ResponseHeader) |  |  |
| prev_kv | [mvcc.v1.KeyValue](#mvcc-v1-KeyValue) |  | if prev_kv is set in the request, the previous key-value pair will be returned. |






<a name="regatta-v1-RangeRequest"></a>

### RangeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [bytes](#bytes) |  | table name of the table |
| key | [bytes](#bytes) |  | key is the first key for the range. If range_end is not given, the request only looks up key. |
| range_end | [bytes](#bytes) |  | range_end is the upper bound on the requested range [key, range_end). If range_end is &#39;\0&#39;, the range is all keys &gt;= key. If range_end is key plus one (e.g., &#34;aa&#34;&#43;1 == &#34;ab&#34;, &#34;a\xff&#34;&#43;1 == &#34;b&#34;), then the range request gets all keys prefixed with key. If both key and range_end are &#39;\0&#39;, then the range request returns all keys. |
| limit | [int64](#int64) |  | limit is a limit on the number of keys returned for the request. When limit is set to 0, it is treated as no limit. |
| linearizable | [bool](#bool) |  | linearizable sets the range request to use linearizable read. Linearizable requests have higher latency and lower throughput than serializable requests but reflect the current consensus of the cluster. For better performance, in exchange for possible stale reads, a serializable range request is served locally without needing to reach consensus with other nodes in the cluster. The serializable request is default option. |
| keys_only | [bool](#bool) |  | keys_only when set returns only the keys and not the values. |
| count_only | [bool](#bool) |  | count_only when set returns only the count of the keys in the range. |
| min_mod_revision | [int64](#int64) |  | min_mod_revision is the lower bound for returned key mod revisions; all keys with lesser mod revisions will be filtered away. |
| max_mod_revision | [int64](#int64) |  | max_mod_revision is the upper bound for returned key mod revisions; all keys with greater mod revisions will be filtered away. |
| min_create_revision | [int64](#int64) |  | min_create_revision is the lower bound for returned key create revisions; all keys with lesser create revisions will be filtered away. |
| max_create_revision | [int64](#int64) |  | max_create_revision is the upper bound for returned key create revisions; all keys with greater create revisions will be filtered away. |






<a name="regatta-v1-RangeResponse"></a>

### RangeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [ResponseHeader](#regatta-v1-ResponseHeader) |  |  |
| kvs | [mvcc.v1.KeyValue](#mvcc-v1-KeyValue) | repeated | kvs is the list of key-value pairs matched by the range request. kvs is empty when count is requested. |
| more | [bool](#bool) |  | more indicates if there are more keys to return in the requested range. |
| count | [int64](#int64) |  | count is set to the number of keys within the range when requested. |






<a name="regatta-v1-ResponseHeader"></a>

### ResponseHeader



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard_id | [uint64](#uint64) |  | shard_id is the ID of the shard which sent the response. |
| replica_id | [uint64](#uint64) |  | replica_id is the ID of the member which sent the response. |
| revision | [uint64](#uint64) |  | revision is the key-value store revision when the request was applied. |
| raft_term | [uint64](#uint64) |  | raft_term is the raft term when the request was applied. |
| raft_leader_id | [uint64](#uint64) |  | raft_leader_id is the ID of the actual raft quorum leader. |






<a name="regatta-v1-TxnRequest"></a>

### TxnRequest
From google paxosdb paper:
Our implementation hinges around a powerful primitive which we call MultiOp. All other database
operations except for iteration are implemented as a single call to MultiOp. A MultiOp is applied atomically
and consists of three components:
1. A list of tests called guard. Each test in guard checks a single entry in the database. It may check
for the absence or presence of a value, or compare with a given value. Two different tests in the guard
may apply to the same or different entries in the database. All tests in the guard are applied and
MultiOp returns the results. If all tests are true, MultiOp executes t op (see item 2 below), otherwise
it executes f op (see item 3 below).
2. A list of database operations called t op. Each operation in the list is either an insert, delete, or
lookup operation, and applies to a database entry(ies). Two different operations in the list may apply
to the same or different entries in the database. These operations are executed
if guard evaluates to true.
3. A list of database operations called f op. Like t op, but executed if guard evaluates to false.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [bytes](#bytes) |  | table name of the table |
| compare | [mvcc.v1.Compare](#mvcc-v1-Compare) | repeated | compare is a list of predicates representing a conjunction of terms. If the comparisons succeed, then the success requests will be processed in order, and the response will contain their respective responses in order. If the comparisons fail, then the failure requests will be processed in order, and the response will contain their respective responses in order. |
| success | [mvcc.v1.RequestOp](#mvcc-v1-RequestOp) | repeated | success is a list of requests which will be applied when compare evaluates to true. |
| failure | [mvcc.v1.RequestOp](#mvcc-v1-RequestOp) | repeated | failure is a list of requests which will be applied when compare evaluates to false. |






<a name="regatta-v1-TxnResponse"></a>

### TxnResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [ResponseHeader](#regatta-v1-ResponseHeader) |  |  |
| succeeded | [bool](#bool) |  | succeeded is set to true if the compare evaluated to true or false otherwise. |
| responses | [mvcc.v1.ResponseOp](#mvcc-v1-ResponseOp) | repeated | responses is a list of responses corresponding to the results from applying success if succeeded is true or failure if succeeded is false. |





 

 

 


<a name="regatta-v1-KV"></a>

### KV
KV for handling the read/put requests

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Range | [RangeRequest](#regatta-v1-RangeRequest) | [RangeResponse](#regatta-v1-RangeResponse) | Range gets the keys in the range from the key-value store. |
| Put | [PutRequest](#regatta-v1-PutRequest) | [PutResponse](#regatta-v1-PutResponse) | Put puts the given key into the key-value store. |
| DeleteRange | [DeleteRangeRequest](#regatta-v1-DeleteRangeRequest) | [DeleteRangeResponse](#regatta-v1-DeleteRangeResponse) | DeleteRange deletes the given range from the key-value store. |
| Txn | [TxnRequest](#regatta-v1-TxnRequest) | [TxnResponse](#regatta-v1-TxnResponse) | Txn processes multiple requests in a single transaction. A txn request increments the revision of the key-value store and generates events with the same revision for every completed request. It is allowed to modify the same key several times within one txn (the result will be the last Op that modified the key). |

 



<a name="replication-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## replication.proto



<a name="replication-v1-MetadataRequest"></a>

### MetadataRequest







<a name="replication-v1-MetadataResponse"></a>

### MetadataResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tables | [Table](#replication-v1-Table) | repeated |  |






<a name="replication-v1-ReplicateCommand"></a>

### ReplicateCommand



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| leader_index | [uint64](#uint64) |  | leaderIndex represents the leader raft index of the given command |
| command | [mvcc.v1.Command](#mvcc-v1-Command) |  | command holds the leader raft log command at leaderIndex |






<a name="replication-v1-ReplicateCommandsResponse"></a>

### ReplicateCommandsResponse
ReplicateCommandsResponse sequence of replication commands


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commands | [ReplicateCommand](#replication-v1-ReplicateCommand) | repeated | commands represent the |






<a name="replication-v1-ReplicateErrResponse"></a>

### ReplicateErrResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [ReplicateError](#replication-v1-ReplicateError) |  |  |






<a name="replication-v1-ReplicateRequest"></a>

### ReplicateRequest
ReplicateRequest request of the replication data at given leader_index


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [bytes](#bytes) |  | table is name of the table to replicate |
| leader_index | [uint64](#uint64) |  | leader_index is the index in the leader raft log of the last stored item in the follower |






<a name="replication-v1-ReplicateResponse"></a>

### ReplicateResponse
ReplicateResponse response to the ReplicateRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commands_response | [ReplicateCommandsResponse](#replication-v1-ReplicateCommandsResponse) |  |  |
| error_response | [ReplicateErrResponse](#replication-v1-ReplicateErrResponse) |  |  |
| leader_index | [uint64](#uint64) |  | leader_index is the largest applied leader index at the time of the client RPC. |






<a name="replication-v1-SnapshotChunk"></a>

### SnapshotChunk



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | data is chunk of snapshot |
| len | [uint64](#uint64) |  | len is a length of data bytes |
| index | [uint64](#uint64) |  | index the index for which the snapshot was created |






<a name="replication-v1-SnapshotRequest"></a>

### SnapshotRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [bytes](#bytes) |  | table is name of the table to stream |






<a name="replication-v1-Table"></a>

### Table



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| type | [Table.Type](#replication-v1-Table-Type) |  |  |





 


<a name="replication-v1-ReplicateError"></a>

### ReplicateError


| Name | Number | Description |
| ---- | ------ | ----------- |
| USE_SNAPSHOT | 0 | USE_SNAPSHOT occurs when leader has no longer the specified `leader_index` in the log. Follower must use `GetSnapshot` to catch up. |
| LEADER_BEHIND | 1 | LEADER_BEHIND occurs when the index of the leader is smaller than requested `leader_index`. This should never happen. Manual intervention needed. |



<a name="replication-v1-Table-Type"></a>

### Table.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| REPLICATED | 0 |  |
| LOCAL | 1 |  |


 

 


<a name="replication-v1-Log"></a>

### Log
Log service provides methods to replicate data from Regatta leader&#39;s log to Regatta followers&#39; logs.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Replicate | [ReplicateRequest](#replication-v1-ReplicateRequest) | [ReplicateResponse](#replication-v1-ReplicateResponse) stream | Replicate is method to ask for data of specified table from the specified index. |


<a name="replication-v1-Metadata"></a>

### Metadata
Metadata service provides method to get Regatta metadata, e.g. tables.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Get | [MetadataRequest](#replication-v1-MetadataRequest) | [MetadataResponse](#replication-v1-MetadataResponse) |  |


<a name="replication-v1-Snapshot"></a>

### Snapshot


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Stream | [SnapshotRequest](#replication-v1-SnapshotRequest) | [SnapshotChunk](#replication-v1-SnapshotChunk) stream |  |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

