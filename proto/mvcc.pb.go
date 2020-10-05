//
// Regatta MVCC protobuffer specification
//

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0
// 	protoc        v3.13.0
// source: mvcc.proto

package proto

import (
	proto "github.com/golang/protobuf/proto"
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

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type Command_CommandType int32

const (
	Command_PUT    Command_CommandType = 0
	Command_DELETE Command_CommandType = 1
)

// Enum value maps for Command_CommandType.
var (
	Command_CommandType_name = map[int32]string{
		0: "PUT",
		1: "DELETE",
	}
	Command_CommandType_value = map[string]int32{
		"PUT":    0,
		"DELETE": 1,
	}
)

func (x Command_CommandType) Enum() *Command_CommandType {
	p := new(Command_CommandType)
	*p = x
	return p
}

func (x Command_CommandType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Command_CommandType) Descriptor() protoreflect.EnumDescriptor {
	return file_mvcc_proto_enumTypes[0].Descriptor()
}

func (Command_CommandType) Type() protoreflect.EnumType {
	return &file_mvcc_proto_enumTypes[0]
}

func (x Command_CommandType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Command_CommandType.Descriptor instead.
func (Command_CommandType) EnumDescriptor() ([]byte, []int) {
	return file_mvcc_proto_rawDescGZIP(), []int{0, 0}
}

type Command struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// type is the kind of event. If type is a PUT, it indicates
	// new data has been stored to the key. If type is a DELETE,
	// it indicates the key was deleted.
	Type Command_CommandType `protobuf:"varint,1,opt,name=type,proto3,enum=mvcc.v1.Command_CommandType" json:"type,omitempty"`
	// kv holds the KeyValue for the event.
	// A PUT event contains current kv pair.
	// A PUT event with kv.Version=1 indicates the creation of a key.
	// A DELETE/EXPIRE event contains the deleted key with
	// its modification revision set to the revision of deletion.
	Kv *KeyValue `protobuf:"bytes,2,opt,name=kv,proto3" json:"kv,omitempty"`
	// prev_kv holds the key-value pair before the event happens.
	PrevKv *KeyValue `protobuf:"bytes,3,opt,name=prev_kv,json=prevKv,proto3" json:"prev_kv,omitempty"`
}

func (x *Command) Reset() {
	*x = Command{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mvcc_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Command) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Command) ProtoMessage() {}

func (x *Command) ProtoReflect() protoreflect.Message {
	mi := &file_mvcc_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Command.ProtoReflect.Descriptor instead.
func (*Command) Descriptor() ([]byte, []int) {
	return file_mvcc_proto_rawDescGZIP(), []int{0}
}

func (x *Command) GetType() Command_CommandType {
	if x != nil {
		return x.Type
	}
	return Command_PUT
}

func (x *Command) GetKv() *KeyValue {
	if x != nil {
		return x.Kv
	}
	return nil
}

func (x *Command) GetPrevKv() *KeyValue {
	if x != nil {
		return x.PrevKv
	}
	return nil
}

type KeyValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// key is the key in bytes. An empty key is not allowed.
	Key []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// create_revision is the revision of last creation on this key.
	CreateRevision int64 `protobuf:"varint,2,opt,name=create_revision,json=createRevision,proto3" json:"create_revision,omitempty"`
	// mod_revision is the revision of last modification on this key.
	ModRevision int64 `protobuf:"varint,3,opt,name=mod_revision,json=modRevision,proto3" json:"mod_revision,omitempty"`
	// value is the value held by the key, in bytes.
	Value []byte `protobuf:"bytes,4,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *KeyValue) Reset() {
	*x = KeyValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mvcc_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KeyValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KeyValue) ProtoMessage() {}

func (x *KeyValue) ProtoReflect() protoreflect.Message {
	mi := &file_mvcc_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KeyValue.ProtoReflect.Descriptor instead.
func (*KeyValue) Descriptor() ([]byte, []int) {
	return file_mvcc_proto_rawDescGZIP(), []int{1}
}

func (x *KeyValue) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *KeyValue) GetCreateRevision() int64 {
	if x != nil {
		return x.CreateRevision
	}
	return 0
}

func (x *KeyValue) GetModRevision() int64 {
	if x != nil {
		return x.ModRevision
	}
	return 0
}

func (x *KeyValue) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

var File_mvcc_proto protoreflect.FileDescriptor

var file_mvcc_proto_rawDesc = []byte{
	0x0a, 0x0a, 0x6d, 0x76, 0x63, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x07, 0x6d, 0x76,
	0x63, 0x63, 0x2e, 0x76, 0x31, 0x22, 0xae, 0x01, 0x0a, 0x07, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e,
	0x64, 0x12, 0x30, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32,
	0x1c, 0x2e, 0x6d, 0x76, 0x63, 0x63, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e,
	0x64, 0x2e, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x54, 0x79, 0x70, 0x65, 0x52, 0x04, 0x74,
	0x79, 0x70, 0x65, 0x12, 0x21, 0x0a, 0x02, 0x6b, 0x76, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x11, 0x2e, 0x6d, 0x76, 0x63, 0x63, 0x2e, 0x76, 0x31, 0x2e, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c,
	0x75, 0x65, 0x52, 0x02, 0x6b, 0x76, 0x12, 0x2a, 0x0a, 0x07, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x6b,
	0x76, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x6d, 0x76, 0x63, 0x63, 0x2e, 0x76,
	0x31, 0x2e, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x06, 0x70, 0x72, 0x65, 0x76,
	0x4b, 0x76, 0x22, 0x22, 0x0a, 0x0b, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x54, 0x79, 0x70,
	0x65, 0x12, 0x07, 0x0a, 0x03, 0x50, 0x55, 0x54, 0x10, 0x00, 0x12, 0x0a, 0x0a, 0x06, 0x44, 0x45,
	0x4c, 0x45, 0x54, 0x45, 0x10, 0x01, 0x22, 0x7e, 0x0a, 0x08, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c,
	0x75, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x03, 0x6b, 0x65, 0x79, 0x12, 0x27, 0x0a, 0x0f, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x5f, 0x72,
	0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0e, 0x63,
	0x72, 0x65, 0x61, 0x74, 0x65, 0x52, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x21, 0x0a,
	0x0c, 0x6d, 0x6f, 0x64, 0x5f, 0x72, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x0b, 0x6d, 0x6f, 0x64, 0x52, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e,
	0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x42, 0x09, 0x5a, 0x07, 0x2e, 0x3b, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_mvcc_proto_rawDescOnce sync.Once
	file_mvcc_proto_rawDescData = file_mvcc_proto_rawDesc
)

func file_mvcc_proto_rawDescGZIP() []byte {
	file_mvcc_proto_rawDescOnce.Do(func() {
		file_mvcc_proto_rawDescData = protoimpl.X.CompressGZIP(file_mvcc_proto_rawDescData)
	})
	return file_mvcc_proto_rawDescData
}

var file_mvcc_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_mvcc_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_mvcc_proto_goTypes = []interface{}{
	(Command_CommandType)(0), // 0: mvcc.v1.Command.CommandType
	(*Command)(nil),          // 1: mvcc.v1.Command
	(*KeyValue)(nil),         // 2: mvcc.v1.KeyValue
}
var file_mvcc_proto_depIdxs = []int32{
	0, // 0: mvcc.v1.Command.type:type_name -> mvcc.v1.Command.CommandType
	2, // 1: mvcc.v1.Command.kv:type_name -> mvcc.v1.KeyValue
	2, // 2: mvcc.v1.Command.prev_kv:type_name -> mvcc.v1.KeyValue
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_mvcc_proto_init() }
func file_mvcc_proto_init() {
	if File_mvcc_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_mvcc_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Command); i {
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
		file_mvcc_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KeyValue); i {
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
			RawDescriptor: file_mvcc_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_mvcc_proto_goTypes,
		DependencyIndexes: file_mvcc_proto_depIdxs,
		EnumInfos:         file_mvcc_proto_enumTypes,
		MessageInfos:      file_mvcc_proto_msgTypes,
	}.Build()
	File_mvcc_proto = out.File
	file_mvcc_proto_rawDesc = nil
	file_mvcc_proto_goTypes = nil
	file_mvcc_proto_depIdxs = nil
}
