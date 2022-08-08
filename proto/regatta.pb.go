//
// Regatta protobuffer specification
//

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.21.4
// source: regatta.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ResponseHeader struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// cluster_id is the ID of the cluster which sent the response.
	ClusterId uint64 `protobuf:"varint,1,opt,name=cluster_id,json=clusterId,proto3" json:"cluster_id,omitempty"`
	// member_id is the ID of the member which sent the response.
	MemberId uint64 `protobuf:"varint,2,opt,name=member_id,json=memberId,proto3" json:"member_id,omitempty"`
	// revision is the key-value store revision when the request was applied.
	Revision int64 `protobuf:"varint,3,opt,name=revision,proto3" json:"revision,omitempty"`
	// raft_term is the raft term when the request was applied.
	RaftTerm uint64 `protobuf:"varint,4,opt,name=raft_term,json=raftTerm,proto3" json:"raft_term,omitempty"`
	// raft_leader_id is the ID of the actual raft quorum leader.
	RaftLeaderId uint64 `protobuf:"varint,5,opt,name=raft_leader_id,json=raftLeaderId,proto3" json:"raft_leader_id,omitempty"`
}

func (x *ResponseHeader) Reset() {
	*x = ResponseHeader{}
	if protoimpl.UnsafeEnabled {
		mi := &file_regatta_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ResponseHeader) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ResponseHeader) ProtoMessage() {}

func (x *ResponseHeader) ProtoReflect() protoreflect.Message {
	mi := &file_regatta_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ResponseHeader.ProtoReflect.Descriptor instead.
func (*ResponseHeader) Descriptor() ([]byte, []int) {
	return file_regatta_proto_rawDescGZIP(), []int{0}
}

func (x *ResponseHeader) GetClusterId() uint64 {
	if x != nil {
		return x.ClusterId
	}
	return 0
}

func (x *ResponseHeader) GetMemberId() uint64 {
	if x != nil {
		return x.MemberId
	}
	return 0
}

func (x *ResponseHeader) GetRevision() int64 {
	if x != nil {
		return x.Revision
	}
	return 0
}

func (x *ResponseHeader) GetRaftTerm() uint64 {
	if x != nil {
		return x.RaftTerm
	}
	return 0
}

func (x *ResponseHeader) GetRaftLeaderId() uint64 {
	if x != nil {
		return x.RaftLeaderId
	}
	return 0
}

type RangeRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// table name of the table
	Table []byte `protobuf:"bytes,1,opt,name=table,proto3" json:"table,omitempty"`
	// key is the first key for the range. If range_end is not given, the request only looks up key.
	Key []byte `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	// range_end is the upper bound on the requested range [key, range_end).
	// If range_end is '\0', the range is all keys >= key.
	// If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"),
	// then the range request gets all keys prefixed with key.
	// If both key and range_end are '\0', then the range request returns all keys.
	RangeEnd []byte `protobuf:"bytes,3,opt,name=range_end,json=rangeEnd,proto3" json:"range_end,omitempty"`
	// limit is a limit on the number of keys returned for the request. When limit is set to 0,
	// it is treated as no limit.
	Limit int64 `protobuf:"varint,4,opt,name=limit,proto3" json:"limit,omitempty"`
	// linearizable sets the range request to use linearizable read. Linearizable requests
	// have higher latency and lower throughput than serializable requests but reflect the current
	// consensus of the cluster. For better performance, in exchange for possible stale reads,
	// a serializable range request is served locally without needing to reach consensus
	// with other nodes in the cluster. The serializable request is default option.
	Linearizable bool `protobuf:"varint,5,opt,name=linearizable,proto3" json:"linearizable,omitempty"`
	// keys_only when set returns only the keys and not the values.
	KeysOnly bool `protobuf:"varint,6,opt,name=keys_only,json=keysOnly,proto3" json:"keys_only,omitempty"`
	// count_only when set returns only the count of the keys in the range.
	CountOnly bool `protobuf:"varint,7,opt,name=count_only,json=countOnly,proto3" json:"count_only,omitempty"`
	// min_mod_revision is the lower bound for returned key mod revisions; all keys with
	// lesser mod revisions will be filtered away.
	MinModRevision int64 `protobuf:"varint,8,opt,name=min_mod_revision,json=minModRevision,proto3" json:"min_mod_revision,omitempty"`
	// max_mod_revision is the upper bound for returned key mod revisions; all keys with
	// greater mod revisions will be filtered away.
	MaxModRevision int64 `protobuf:"varint,9,opt,name=max_mod_revision,json=maxModRevision,proto3" json:"max_mod_revision,omitempty"`
	// min_create_revision is the lower bound for returned key create revisions; all keys with
	// lesser create revisions will be filtered away.
	MinCreateRevision int64 `protobuf:"varint,10,opt,name=min_create_revision,json=minCreateRevision,proto3" json:"min_create_revision,omitempty"`
	// max_create_revision is the upper bound for returned key create revisions; all keys with
	// greater create revisions will be filtered away.
	MaxCreateRevision int64 `protobuf:"varint,11,opt,name=max_create_revision,json=maxCreateRevision,proto3" json:"max_create_revision,omitempty"`
}

func (x *RangeRequest) Reset() {
	*x = RangeRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_regatta_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RangeRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RangeRequest) ProtoMessage() {}

func (x *RangeRequest) ProtoReflect() protoreflect.Message {
	mi := &file_regatta_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RangeRequest.ProtoReflect.Descriptor instead.
func (*RangeRequest) Descriptor() ([]byte, []int) {
	return file_regatta_proto_rawDescGZIP(), []int{1}
}

func (x *RangeRequest) GetTable() []byte {
	if x != nil {
		return x.Table
	}
	return nil
}

func (x *RangeRequest) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *RangeRequest) GetRangeEnd() []byte {
	if x != nil {
		return x.RangeEnd
	}
	return nil
}

func (x *RangeRequest) GetLimit() int64 {
	if x != nil {
		return x.Limit
	}
	return 0
}

func (x *RangeRequest) GetLinearizable() bool {
	if x != nil {
		return x.Linearizable
	}
	return false
}

func (x *RangeRequest) GetKeysOnly() bool {
	if x != nil {
		return x.KeysOnly
	}
	return false
}

func (x *RangeRequest) GetCountOnly() bool {
	if x != nil {
		return x.CountOnly
	}
	return false
}

func (x *RangeRequest) GetMinModRevision() int64 {
	if x != nil {
		return x.MinModRevision
	}
	return 0
}

func (x *RangeRequest) GetMaxModRevision() int64 {
	if x != nil {
		return x.MaxModRevision
	}
	return 0
}

func (x *RangeRequest) GetMinCreateRevision() int64 {
	if x != nil {
		return x.MinCreateRevision
	}
	return 0
}

func (x *RangeRequest) GetMaxCreateRevision() int64 {
	if x != nil {
		return x.MaxCreateRevision
	}
	return 0
}

type RangeResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// kvs is the list of key-value pairs matched by the range request.
	// kvs is empty when count is requested.
	Kvs []*KeyValue `protobuf:"bytes,3,rep,name=kvs,proto3" json:"kvs,omitempty"`
	// more indicates if there are more keys to return in the requested range.
	More bool `protobuf:"varint,4,opt,name=more,proto3" json:"more,omitempty"`
	// count is set to the number of keys within the range when requested.
	Count int64 `protobuf:"varint,5,opt,name=count,proto3" json:"count,omitempty"`
}

func (x *RangeResponse) Reset() {
	*x = RangeResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_regatta_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RangeResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RangeResponse) ProtoMessage() {}

func (x *RangeResponse) ProtoReflect() protoreflect.Message {
	mi := &file_regatta_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RangeResponse.ProtoReflect.Descriptor instead.
func (*RangeResponse) Descriptor() ([]byte, []int) {
	return file_regatta_proto_rawDescGZIP(), []int{2}
}

func (x *RangeResponse) GetHeader() *ResponseHeader {
	if x != nil {
		return x.Header
	}
	return nil
}

func (x *RangeResponse) GetKvs() []*KeyValue {
	if x != nil {
		return x.Kvs
	}
	return nil
}

func (x *RangeResponse) GetMore() bool {
	if x != nil {
		return x.More
	}
	return false
}

func (x *RangeResponse) GetCount() int64 {
	if x != nil {
		return x.Count
	}
	return 0
}

type PutRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// table name of the table
	Table []byte `protobuf:"bytes,1,opt,name=table,proto3" json:"table,omitempty"`
	// key is the key, in bytes, to put into the key-value store.
	Key []byte `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	// value is the value, in bytes, to associate with the key in the key-value store.
	Value []byte `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
	// prev_kv if true the previous key-value pair will be returned in the put response.
	PrevKv bool `protobuf:"varint,4,opt,name=prev_kv,json=prevKv,proto3" json:"prev_kv,omitempty"`
}

func (x *PutRequest) Reset() {
	*x = PutRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_regatta_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PutRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutRequest) ProtoMessage() {}

func (x *PutRequest) ProtoReflect() protoreflect.Message {
	mi := &file_regatta_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PutRequest.ProtoReflect.Descriptor instead.
func (*PutRequest) Descriptor() ([]byte, []int) {
	return file_regatta_proto_rawDescGZIP(), []int{3}
}

func (x *PutRequest) GetTable() []byte {
	if x != nil {
		return x.Table
	}
	return nil
}

func (x *PutRequest) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *PutRequest) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *PutRequest) GetPrevKv() bool {
	if x != nil {
		return x.PrevKv
	}
	return false
}

type PutResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// if prev_kv is set in the request, the previous key-value pair will be returned.
	PrevKv *KeyValue `protobuf:"bytes,2,opt,name=prev_kv,json=prevKv,proto3" json:"prev_kv,omitempty"`
}

func (x *PutResponse) Reset() {
	*x = PutResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_regatta_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PutResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutResponse) ProtoMessage() {}

func (x *PutResponse) ProtoReflect() protoreflect.Message {
	mi := &file_regatta_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PutResponse.ProtoReflect.Descriptor instead.
func (*PutResponse) Descriptor() ([]byte, []int) {
	return file_regatta_proto_rawDescGZIP(), []int{4}
}

func (x *PutResponse) GetHeader() *ResponseHeader {
	if x != nil {
		return x.Header
	}
	return nil
}

func (x *PutResponse) GetPrevKv() *KeyValue {
	if x != nil {
		return x.PrevKv
	}
	return nil
}

type DeleteRangeRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// table name of the table
	Table []byte `protobuf:"bytes,1,opt,name=table,proto3" json:"table,omitempty"`
	// key is the first key to delete in the range.
	Key []byte `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	// range_end is the key following the last key to delete for the range [key, range_end).
	// If range_end is not given, the range is defined to contain only the key argument.
	// If range_end is one bit larger than the given key, then the range is all the keys
	// with the prefix (the given key).
	// If range_end is '\0', the range is all keys greater than or equal to the key argument.
	RangeEnd []byte `protobuf:"bytes,3,opt,name=range_end,json=rangeEnd,proto3" json:"range_end,omitempty"`
	// If prev_kv is set, etcd gets the previous key-value pairs before deleting it.
	// The previous key-value pairs will be returned in the delete response.
	PrevKv bool `protobuf:"varint,4,opt,name=prev_kv,json=prevKv,proto3" json:"prev_kv,omitempty"`
}

func (x *DeleteRangeRequest) Reset() {
	*x = DeleteRangeRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_regatta_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteRangeRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteRangeRequest) ProtoMessage() {}

func (x *DeleteRangeRequest) ProtoReflect() protoreflect.Message {
	mi := &file_regatta_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteRangeRequest.ProtoReflect.Descriptor instead.
func (*DeleteRangeRequest) Descriptor() ([]byte, []int) {
	return file_regatta_proto_rawDescGZIP(), []int{5}
}

func (x *DeleteRangeRequest) GetTable() []byte {
	if x != nil {
		return x.Table
	}
	return nil
}

func (x *DeleteRangeRequest) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *DeleteRangeRequest) GetRangeEnd() []byte {
	if x != nil {
		return x.RangeEnd
	}
	return nil
}

func (x *DeleteRangeRequest) GetPrevKv() bool {
	if x != nil {
		return x.PrevKv
	}
	return false
}

type DeleteRangeResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// deleted is the number of keys deleted by the delete range request.
	Deleted int64 `protobuf:"varint,2,opt,name=deleted,proto3" json:"deleted,omitempty"`
	// if prev_kv is set in the request, the previous key-value pairs will be returned.
	PrevKvs []*KeyValue `protobuf:"bytes,3,rep,name=prev_kvs,json=prevKvs,proto3" json:"prev_kvs,omitempty"`
}

func (x *DeleteRangeResponse) Reset() {
	*x = DeleteRangeResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_regatta_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteRangeResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteRangeResponse) ProtoMessage() {}

func (x *DeleteRangeResponse) ProtoReflect() protoreflect.Message {
	mi := &file_regatta_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteRangeResponse.ProtoReflect.Descriptor instead.
func (*DeleteRangeResponse) Descriptor() ([]byte, []int) {
	return file_regatta_proto_rawDescGZIP(), []int{6}
}

func (x *DeleteRangeResponse) GetHeader() *ResponseHeader {
	if x != nil {
		return x.Header
	}
	return nil
}

func (x *DeleteRangeResponse) GetDeleted() int64 {
	if x != nil {
		return x.Deleted
	}
	return 0
}

func (x *DeleteRangeResponse) GetPrevKvs() []*KeyValue {
	if x != nil {
		return x.PrevKvs
	}
	return nil
}

// From google paxosdb paper:
// Our implementation hinges around a powerful primitive which we call MultiOp. All other database
// operations except for iteration are implemented as a single call to MultiOp. A MultiOp is applied atomically
// and consists of three components:
// 1. A list of tests called guard. Each test in guard checks a single entry in the database. It may check
// for the absence or presence of a value, or compare with a given value. Two different tests in the guard
// may apply to the same or different entries in the database. All tests in the guard are applied and
// MultiOp returns the results. If all tests are true, MultiOp executes t op (see item 2 below), otherwise
// it executes f op (see item 3 below).
// 2. A list of database operations called t op. Each operation in the list is either an insert, delete, or
// lookup operation, and applies to a database entry(ies). Two different operations in the list may apply
// to the same or different entries in the database. These operations are executed
// if guard evaluates to true.
// 3. A list of database operations called f op. Like t op, but executed if guard evaluates to false.
type TxnRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// table name of the table
	Table []byte `protobuf:"bytes,1,opt,name=table,proto3" json:"table,omitempty"`
	// compare is a list of predicates representing a conjunction of terms.
	// If the comparisons succeed, then the success requests will be processed in order,
	// and the response will contain their respective responses in order.
	// If the comparisons fail, then the failure requests will be processed in order,
	// and the response will contain their respective responses in order.
	Compare []*Compare `protobuf:"bytes,2,rep,name=compare,proto3" json:"compare,omitempty"`
	// success is a list of requests which will be applied when compare evaluates to true.
	Success []*RequestOp `protobuf:"bytes,3,rep,name=success,proto3" json:"success,omitempty"`
	// failure is a list of requests which will be applied when compare evaluates to false.
	Failure []*RequestOp `protobuf:"bytes,4,rep,name=failure,proto3" json:"failure,omitempty"`
}

func (x *TxnRequest) Reset() {
	*x = TxnRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_regatta_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TxnRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TxnRequest) ProtoMessage() {}

func (x *TxnRequest) ProtoReflect() protoreflect.Message {
	mi := &file_regatta_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TxnRequest.ProtoReflect.Descriptor instead.
func (*TxnRequest) Descriptor() ([]byte, []int) {
	return file_regatta_proto_rawDescGZIP(), []int{7}
}

func (x *TxnRequest) GetTable() []byte {
	if x != nil {
		return x.Table
	}
	return nil
}

func (x *TxnRequest) GetCompare() []*Compare {
	if x != nil {
		return x.Compare
	}
	return nil
}

func (x *TxnRequest) GetSuccess() []*RequestOp {
	if x != nil {
		return x.Success
	}
	return nil
}

func (x *TxnRequest) GetFailure() []*RequestOp {
	if x != nil {
		return x.Failure
	}
	return nil
}

type TxnResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// succeeded is set to true if the compare evaluated to true or false otherwise.
	Succeeded bool `protobuf:"varint,2,opt,name=succeeded,proto3" json:"succeeded,omitempty"`
	// responses is a list of responses corresponding to the results from applying
	// success if succeeded is true or failure if succeeded is false.
	Responses []*ResponseOp `protobuf:"bytes,3,rep,name=responses,proto3" json:"responses,omitempty"`
}

func (x *TxnResponse) Reset() {
	*x = TxnResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_regatta_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TxnResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TxnResponse) ProtoMessage() {}

func (x *TxnResponse) ProtoReflect() protoreflect.Message {
	mi := &file_regatta_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TxnResponse.ProtoReflect.Descriptor instead.
func (*TxnResponse) Descriptor() ([]byte, []int) {
	return file_regatta_proto_rawDescGZIP(), []int{8}
}

func (x *TxnResponse) GetHeader() *ResponseHeader {
	if x != nil {
		return x.Header
	}
	return nil
}

func (x *TxnResponse) GetSucceeded() bool {
	if x != nil {
		return x.Succeeded
	}
	return false
}

func (x *TxnResponse) GetResponses() []*ResponseOp {
	if x != nil {
		return x.Responses
	}
	return nil
}

var File_regatta_proto protoreflect.FileDescriptor

var file_regatta_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x72, 0x65, 0x67, 0x61, 0x74, 0x74, 0x61, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x0a, 0x72, 0x65, 0x67, 0x61, 0x74, 0x74, 0x61, 0x2e, 0x76, 0x31, 0x1a, 0x0a, 0x6d, 0x76, 0x63,
	0x63, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xab, 0x01, 0x0a, 0x0e, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x1d, 0x0a, 0x0a, 0x63, 0x6c,
	0x75, 0x73, 0x74, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09,
	0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x49, 0x64, 0x12, 0x1b, 0x0a, 0x09, 0x6d, 0x65, 0x6d,
	0x62, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x08, 0x6d, 0x65,
	0x6d, 0x62, 0x65, 0x72, 0x49, 0x64, 0x12, 0x1a, 0x0a, 0x08, 0x72, 0x65, 0x76, 0x69, 0x73, 0x69,
	0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x72, 0x65, 0x76, 0x69, 0x73, 0x69,
	0x6f, 0x6e, 0x12, 0x1b, 0x0a, 0x09, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x74, 0x65, 0x72, 0x6d, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x04, 0x52, 0x08, 0x72, 0x61, 0x66, 0x74, 0x54, 0x65, 0x72, 0x6d, 0x12,
	0x24, 0x0a, 0x0e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x5f, 0x69,
	0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0c, 0x72, 0x61, 0x66, 0x74, 0x4c, 0x65, 0x61,
	0x64, 0x65, 0x72, 0x49, 0x64, 0x22, 0xfd, 0x02, 0x0a, 0x0c, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x12, 0x10, 0x0a, 0x03,
	0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x1b,
	0x0a, 0x09, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x65, 0x6e, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x08, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x45, 0x6e, 0x64, 0x12, 0x14, 0x0a, 0x05, 0x6c,
	0x69, 0x6d, 0x69, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x6c, 0x69, 0x6d, 0x69,
	0x74, 0x12, 0x22, 0x0a, 0x0c, 0x6c, 0x69, 0x6e, 0x65, 0x61, 0x72, 0x69, 0x7a, 0x61, 0x62, 0x6c,
	0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0c, 0x6c, 0x69, 0x6e, 0x65, 0x61, 0x72, 0x69,
	0x7a, 0x61, 0x62, 0x6c, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x6b, 0x65, 0x79, 0x73, 0x5f, 0x6f, 0x6e,
	0x6c, 0x79, 0x18, 0x06, 0x20, 0x01, 0x28, 0x08, 0x52, 0x08, 0x6b, 0x65, 0x79, 0x73, 0x4f, 0x6e,
	0x6c, 0x79, 0x12, 0x1d, 0x0a, 0x0a, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x5f, 0x6f, 0x6e, 0x6c, 0x79,
	0x18, 0x07, 0x20, 0x01, 0x28, 0x08, 0x52, 0x09, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x4f, 0x6e, 0x6c,
	0x79, 0x12, 0x28, 0x0a, 0x10, 0x6d, 0x69, 0x6e, 0x5f, 0x6d, 0x6f, 0x64, 0x5f, 0x72, 0x65, 0x76,
	0x69, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x08, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0e, 0x6d, 0x69, 0x6e,
	0x4d, 0x6f, 0x64, 0x52, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x28, 0x0a, 0x10, 0x6d,
	0x61, 0x78, 0x5f, 0x6d, 0x6f, 0x64, 0x5f, 0x72, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x18,
	0x09, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0e, 0x6d, 0x61, 0x78, 0x4d, 0x6f, 0x64, 0x52, 0x65, 0x76,
	0x69, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x2e, 0x0a, 0x13, 0x6d, 0x69, 0x6e, 0x5f, 0x63, 0x72, 0x65,
	0x61, 0x74, 0x65, 0x5f, 0x72, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x0a, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x11, 0x6d, 0x69, 0x6e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x52, 0x65, 0x76,
	0x69, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x2e, 0x0a, 0x13, 0x6d, 0x61, 0x78, 0x5f, 0x63, 0x72, 0x65,
	0x61, 0x74, 0x65, 0x5f, 0x72, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x0b, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x11, 0x6d, 0x61, 0x78, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x52, 0x65, 0x76,
	0x69, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x92, 0x01, 0x0a, 0x0d, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x32, 0x0a, 0x06, 0x68, 0x65, 0x61, 0x64, 0x65,
	0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x72, 0x65, 0x67, 0x61, 0x74, 0x74,
	0x61, 0x2e, 0x76, 0x31, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x48, 0x65, 0x61,
	0x64, 0x65, 0x72, 0x52, 0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x23, 0x0a, 0x03, 0x6b,
	0x76, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x6d, 0x76, 0x63, 0x63, 0x2e,
	0x76, 0x31, 0x2e, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x03, 0x6b, 0x76, 0x73,
	0x12, 0x12, 0x0a, 0x04, 0x6d, 0x6f, 0x72, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x52, 0x04,
	0x6d, 0x6f, 0x72, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x05, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x22, 0x63, 0x0a, 0x0a, 0x50, 0x75,
	0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x61, 0x62, 0x6c,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x12, 0x10,
	0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79,
	0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x17, 0x0a, 0x07, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x6b,
	0x76, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x52, 0x06, 0x70, 0x72, 0x65, 0x76, 0x4b, 0x76, 0x22,
	0x6d, 0x0a, 0x0b, 0x50, 0x75, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x32,
	0x0a, 0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a,
	0x2e, 0x72, 0x65, 0x67, 0x61, 0x74, 0x74, 0x61, 0x2e, 0x76, 0x31, 0x2e, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x52, 0x06, 0x68, 0x65, 0x61, 0x64,
	0x65, 0x72, 0x12, 0x2a, 0x0a, 0x07, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x6b, 0x76, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x6d, 0x76, 0x63, 0x63, 0x2e, 0x76, 0x31, 0x2e, 0x4b, 0x65,
	0x79, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x06, 0x70, 0x72, 0x65, 0x76, 0x4b, 0x76, 0x22, 0x72,
	0x0a, 0x12, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x05, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65,
	0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x1b, 0x0a, 0x09,
	0x72, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x65, 0x6e, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x08, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x45, 0x6e, 0x64, 0x12, 0x17, 0x0a, 0x07, 0x70, 0x72, 0x65,
	0x76, 0x5f, 0x6b, 0x76, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x52, 0x06, 0x70, 0x72, 0x65, 0x76,
	0x4b, 0x76, 0x22, 0x91, 0x01, 0x0a, 0x13, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x61, 0x6e,
	0x67, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x32, 0x0a, 0x06, 0x68, 0x65,
	0x61, 0x64, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x72, 0x65, 0x67,
	0x61, 0x74, 0x74, 0x61, 0x2e, 0x76, 0x31, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x52, 0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x18,
	0x0a, 0x07, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x07, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x12, 0x2c, 0x0a, 0x08, 0x70, 0x72, 0x65, 0x76,
	0x5f, 0x6b, 0x76, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x6d, 0x76, 0x63,
	0x63, 0x2e, 0x76, 0x31, 0x2e, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x07, 0x70,
	0x72, 0x65, 0x76, 0x4b, 0x76, 0x73, 0x22, 0xaa, 0x01, 0x0a, 0x0a, 0x54, 0x78, 0x6e, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x12, 0x2a, 0x0a, 0x07, 0x63,
	0x6f, 0x6d, 0x70, 0x61, 0x72, 0x65, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x6d,
	0x76, 0x63, 0x63, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x6f, 0x6d, 0x70, 0x61, 0x72, 0x65, 0x52, 0x07,
	0x63, 0x6f, 0x6d, 0x70, 0x61, 0x72, 0x65, 0x12, 0x2c, 0x0a, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65,
	0x73, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x6d, 0x76, 0x63, 0x63, 0x2e,
	0x76, 0x31, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x4f, 0x70, 0x52, 0x07, 0x73, 0x75,
	0x63, 0x63, 0x65, 0x73, 0x73, 0x12, 0x2c, 0x0a, 0x07, 0x66, 0x61, 0x69, 0x6c, 0x75, 0x72, 0x65,
	0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x6d, 0x76, 0x63, 0x63, 0x2e, 0x76, 0x31,
	0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x4f, 0x70, 0x52, 0x07, 0x66, 0x61, 0x69, 0x6c,
	0x75, 0x72, 0x65, 0x22, 0x92, 0x01, 0x0a, 0x0b, 0x54, 0x78, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x12, 0x32, 0x0a, 0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x72, 0x65, 0x67, 0x61, 0x74, 0x74, 0x61, 0x2e, 0x76, 0x31,
	0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x52,
	0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x1c, 0x0a, 0x09, 0x73, 0x75, 0x63, 0x63, 0x65,
	0x65, 0x64, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x09, 0x73, 0x75, 0x63, 0x63,
	0x65, 0x65, 0x64, 0x65, 0x64, 0x12, 0x31, 0x0a, 0x09, 0x72, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x6d, 0x76, 0x63, 0x63, 0x2e,
	0x76, 0x31, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x4f, 0x70, 0x52, 0x09, 0x72,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x73, 0x32, 0x82, 0x02, 0x0a, 0x02, 0x4b, 0x56, 0x12,
	0x3c, 0x0a, 0x05, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x18, 0x2e, 0x72, 0x65, 0x67, 0x61, 0x74,
	0x74, 0x61, 0x2e, 0x76, 0x31, 0x2e, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x19, 0x2e, 0x72, 0x65, 0x67, 0x61, 0x74, 0x74, 0x61, 0x2e, 0x76, 0x31, 0x2e,
	0x52, 0x61, 0x6e, 0x67, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x36, 0x0a,
	0x03, 0x50, 0x75, 0x74, 0x12, 0x16, 0x2e, 0x72, 0x65, 0x67, 0x61, 0x74, 0x74, 0x61, 0x2e, 0x76,
	0x31, 0x2e, 0x50, 0x75, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x17, 0x2e, 0x72,
	0x65, 0x67, 0x61, 0x74, 0x74, 0x61, 0x2e, 0x76, 0x31, 0x2e, 0x50, 0x75, 0x74, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x4e, 0x0a, 0x0b, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52,
	0x61, 0x6e, 0x67, 0x65, 0x12, 0x1e, 0x2e, 0x72, 0x65, 0x67, 0x61, 0x74, 0x74, 0x61, 0x2e, 0x76,
	0x31, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x1f, 0x2e, 0x72, 0x65, 0x67, 0x61, 0x74, 0x74, 0x61, 0x2e, 0x76,
	0x31, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x36, 0x0a, 0x03, 0x54, 0x78, 0x6e, 0x12, 0x16, 0x2e, 0x72,
	0x65, 0x67, 0x61, 0x74, 0x74, 0x61, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x78, 0x6e, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x17, 0x2e, 0x72, 0x65, 0x67, 0x61, 0x74, 0x74, 0x61, 0x2e, 0x76,
	0x31, 0x2e, 0x54, 0x78, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x09, 0x5a,
	0x07, 0x2e, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_regatta_proto_rawDescOnce sync.Once
	file_regatta_proto_rawDescData = file_regatta_proto_rawDesc
)

func file_regatta_proto_rawDescGZIP() []byte {
	file_regatta_proto_rawDescOnce.Do(func() {
		file_regatta_proto_rawDescData = protoimpl.X.CompressGZIP(file_regatta_proto_rawDescData)
	})
	return file_regatta_proto_rawDescData
}

var file_regatta_proto_msgTypes = make([]protoimpl.MessageInfo, 9)
var file_regatta_proto_goTypes = []interface{}{
	(*ResponseHeader)(nil),      // 0: regatta.v1.ResponseHeader
	(*RangeRequest)(nil),        // 1: regatta.v1.RangeRequest
	(*RangeResponse)(nil),       // 2: regatta.v1.RangeResponse
	(*PutRequest)(nil),          // 3: regatta.v1.PutRequest
	(*PutResponse)(nil),         // 4: regatta.v1.PutResponse
	(*DeleteRangeRequest)(nil),  // 5: regatta.v1.DeleteRangeRequest
	(*DeleteRangeResponse)(nil), // 6: regatta.v1.DeleteRangeResponse
	(*TxnRequest)(nil),          // 7: regatta.v1.TxnRequest
	(*TxnResponse)(nil),         // 8: regatta.v1.TxnResponse
	(*KeyValue)(nil),            // 9: mvcc.v1.KeyValue
	(*Compare)(nil),             // 10: mvcc.v1.Compare
	(*RequestOp)(nil),           // 11: mvcc.v1.RequestOp
	(*ResponseOp)(nil),          // 12: mvcc.v1.ResponseOp
}
var file_regatta_proto_depIdxs = []int32{
	0,  // 0: regatta.v1.RangeResponse.header:type_name -> regatta.v1.ResponseHeader
	9,  // 1: regatta.v1.RangeResponse.kvs:type_name -> mvcc.v1.KeyValue
	0,  // 2: regatta.v1.PutResponse.header:type_name -> regatta.v1.ResponseHeader
	9,  // 3: regatta.v1.PutResponse.prev_kv:type_name -> mvcc.v1.KeyValue
	0,  // 4: regatta.v1.DeleteRangeResponse.header:type_name -> regatta.v1.ResponseHeader
	9,  // 5: regatta.v1.DeleteRangeResponse.prev_kvs:type_name -> mvcc.v1.KeyValue
	10, // 6: regatta.v1.TxnRequest.compare:type_name -> mvcc.v1.Compare
	11, // 7: regatta.v1.TxnRequest.success:type_name -> mvcc.v1.RequestOp
	11, // 8: regatta.v1.TxnRequest.failure:type_name -> mvcc.v1.RequestOp
	0,  // 9: regatta.v1.TxnResponse.header:type_name -> regatta.v1.ResponseHeader
	12, // 10: regatta.v1.TxnResponse.responses:type_name -> mvcc.v1.ResponseOp
	1,  // 11: regatta.v1.KV.Range:input_type -> regatta.v1.RangeRequest
	3,  // 12: regatta.v1.KV.Put:input_type -> regatta.v1.PutRequest
	5,  // 13: regatta.v1.KV.DeleteRange:input_type -> regatta.v1.DeleteRangeRequest
	7,  // 14: regatta.v1.KV.Txn:input_type -> regatta.v1.TxnRequest
	2,  // 15: regatta.v1.KV.Range:output_type -> regatta.v1.RangeResponse
	4,  // 16: regatta.v1.KV.Put:output_type -> regatta.v1.PutResponse
	6,  // 17: regatta.v1.KV.DeleteRange:output_type -> regatta.v1.DeleteRangeResponse
	8,  // 18: regatta.v1.KV.Txn:output_type -> regatta.v1.TxnResponse
	15, // [15:19] is the sub-list for method output_type
	11, // [11:15] is the sub-list for method input_type
	11, // [11:11] is the sub-list for extension type_name
	11, // [11:11] is the sub-list for extension extendee
	0,  // [0:11] is the sub-list for field type_name
}

func init() { file_regatta_proto_init() }
func file_regatta_proto_init() {
	if File_regatta_proto != nil {
		return
	}
	file_mvcc_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_regatta_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ResponseHeader); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_regatta_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RangeRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_regatta_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RangeResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_regatta_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PutRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_regatta_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PutResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_regatta_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteRangeRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_regatta_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteRangeResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_regatta_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TxnRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_regatta_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TxnResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_regatta_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   9,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_regatta_proto_goTypes,
		DependencyIndexes: file_regatta_proto_depIdxs,
		MessageInfos:      file_regatta_proto_msgTypes,
	}.Build()
	File_regatta_proto = out.File
	file_regatta_proto_rawDesc = nil
	file_regatta_proto_goTypes = nil
	file_regatta_proto_depIdxs = nil
}
