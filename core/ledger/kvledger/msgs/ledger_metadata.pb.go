// Code generated by protoc-gen-go. DO NOT EDIT.
// source: ledger_metadata.proto

package msgs

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// Status specifies the status of a ledger
type Status int32

const (
	Status_ACTIVE             Status = 0
	Status_INACTIVE           Status = 1
	Status_UNDER_CONSTRUCTION Status = 2
	Status_UNDER_DELETION     Status = 3
)

var Status_name = map[int32]string{
	0: "ACTIVE",
	1: "INACTIVE",
	2: "UNDER_CONSTRUCTION",
	3: "UNDER_DELETION",
}

var Status_value = map[string]int32{
	"ACTIVE":             0,
	"INACTIVE":           1,
	"UNDER_CONSTRUCTION": 2,
	"UNDER_DELETION":     3,
}

func (x Status) String() string {
	return proto.EnumName(Status_name, int32(x))
}

func (Status) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_8173a53a47b026a1, []int{0}
}

// BootSnapshotMetadata 捕获用于引导账本的快照的元数据。
type BootSnapshotMetadata struct {
	SingableMetadata     string   `protobuf:"bytes,1,opt,name=singableMetadata,proto3" json:"singableMetadata,omitempty"`     // 可签名元数据
	AdditionalMetadata   string   `protobuf:"bytes,2,opt,name=additionalMetadata,proto3" json:"additionalMetadata,omitempty"` // 附加元数据
	XXX_NoUnkeyedLiteral struct{} `json:"-"`                                                                                  // 无关键字文字
	XXX_unrecognized     []byte   `json:"-"`                                                                                  // 未识别的字段
	XXX_sizecache        int32    `json:"-"`                                                                                  // 大小缓存
}

func (m *BootSnapshotMetadata) Reset()         { *m = BootSnapshotMetadata{} }
func (m *BootSnapshotMetadata) String() string { return proto.CompactTextString(m) }
func (*BootSnapshotMetadata) ProtoMessage()    {}
func (*BootSnapshotMetadata) Descriptor() ([]byte, []int) {
	return fileDescriptor_8173a53a47b026a1, []int{0}
}

func (m *BootSnapshotMetadata) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BootSnapshotMetadata.Unmarshal(m, b)
}
func (m *BootSnapshotMetadata) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BootSnapshotMetadata.Marshal(b, m, deterministic)
}
func (m *BootSnapshotMetadata) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BootSnapshotMetadata.Merge(m, src)
}
func (m *BootSnapshotMetadata) XXX_Size() int {
	return xxx_messageInfo_BootSnapshotMetadata.Size(m)
}
func (m *BootSnapshotMetadata) XXX_DiscardUnknown() {
	xxx_messageInfo_BootSnapshotMetadata.DiscardUnknown(m)
}

var xxx_messageInfo_BootSnapshotMetadata proto.InternalMessageInfo

func (m *BootSnapshotMetadata) GetSingableMetadata() string {
	if m != nil {
		return m.SingableMetadata
	}
	return ""
}

func (m *BootSnapshotMetadata) GetAdditionalMetadata() string {
	if m != nil {
		return m.AdditionalMetadata
	}
	return ""
}

// LedgerMetadata 指定账本的元数据。
type LedgerMetadata struct {
	Status               Status                `protobuf:"varint,1,opt,name=status,proto3,enum=msgs.Status" json:"status,omitempty"`                                         // 状态
	BootSnapshotMetadata *BootSnapshotMetadata `protobuf:"bytes,2,opt,name=boot_snapshot_metadata,json=bootSnapshotMetadata,proto3" json:"boot_snapshot_metadata,omitempty"` // 启动快照元数据
	XXX_NoUnkeyedLiteral struct{}              `json:"-"`                                                                                                                    // 无关键字文字
	XXX_unrecognized     []byte                `json:"-"`                                                                                                                    // 未识别的字段
	XXX_sizecache        int32                 `json:"-"`                                                                                                                    // 大小缓存
}

func (m *LedgerMetadata) Reset()         { *m = LedgerMetadata{} }
func (m *LedgerMetadata) String() string { return proto.CompactTextString(m) }
func (*LedgerMetadata) ProtoMessage()    {}
func (*LedgerMetadata) Descriptor() ([]byte, []int) {
	return fileDescriptor_8173a53a47b026a1, []int{1}
}

func (m *LedgerMetadata) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_LedgerMetadata.Unmarshal(m, b)
}
func (m *LedgerMetadata) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_LedgerMetadata.Marshal(b, m, deterministic)
}
func (m *LedgerMetadata) XXX_Merge(src proto.Message) {
	xxx_messageInfo_LedgerMetadata.Merge(m, src)
}
func (m *LedgerMetadata) XXX_Size() int {
	return xxx_messageInfo_LedgerMetadata.Size(m)
}
func (m *LedgerMetadata) XXX_DiscardUnknown() {
	xxx_messageInfo_LedgerMetadata.DiscardUnknown(m)
}

var xxx_messageInfo_LedgerMetadata proto.InternalMessageInfo

func (m *LedgerMetadata) GetStatus() Status {
	if m != nil {
		return m.Status
	}
	return Status_ACTIVE
}

func (m *LedgerMetadata) GetBootSnapshotMetadata() *BootSnapshotMetadata {
	if m != nil {
		return m.BootSnapshotMetadata
	}
	return nil
}

func init() {
	proto.RegisterEnum("msgs.Status", Status_name, Status_value)
	proto.RegisterType((*BootSnapshotMetadata)(nil), "msgs.BootSnapshotMetadata")
	proto.RegisterType((*LedgerMetadata)(nil), "msgs.LedgerMetadata")
}

func init() { proto.RegisterFile("ledger_metadata.proto", fileDescriptor_8173a53a47b026a1) }

var fileDescriptor_8173a53a47b026a1 = []byte{
	// 285 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x91, 0x41, 0x4f, 0x83, 0x30,
	0x1c, 0xc5, 0x65, 0x1a, 0xa2, 0x7f, 0x17, 0x42, 0x9a, 0xb9, 0x18, 0x4f, 0x66, 0xf1, 0x60, 0x76,
	0x80, 0x64, 0x1e, 0x8c, 0x47, 0xc7, 0x38, 0x90, 0x4c, 0x66, 0x80, 0x79, 0xf0, 0x42, 0x5a, 0xa8,
	0x40, 0x04, 0x4a, 0xda, 0xce, 0xc4, 0x6f, 0xe0, 0xc7, 0x36, 0x2b, 0x95, 0x8b, 0xdc, 0xda, 0xdf,
	0xff, 0xb5, 0xef, 0xbd, 0x16, 0xae, 0x6a, 0x9a, 0x17, 0x94, 0xa7, 0x0d, 0x95, 0x38, 0xc7, 0x12,
	0x3b, 0x1d, 0x67, 0x92, 0xa1, 0xb3, 0x46, 0x14, 0x62, 0xc1, 0x61, 0xb6, 0x66, 0x4c, 0xc6, 0x2d,
	0xee, 0x44, 0xc9, 0xe4, 0x8b, 0xd6, 0xa0, 0x25, 0xd8, 0xa2, 0x6a, 0x0b, 0x4c, 0x6a, 0xfa, 0xc7,
	0xae, 0x8d, 0x5b, 0xe3, 0xfe, 0x22, 0xfa, 0xc7, 0x91, 0x03, 0x08, 0xe7, 0x79, 0x25, 0x2b, 0xd6,
	0xe2, 0x7a, 0x50, 0x4f, 0x94, 0x7a, 0x64, 0xb2, 0xf8, 0x31, 0xc0, 0xda, 0xaa, 0x4c, 0xc3, 0x15,
	0x77, 0x60, 0x0a, 0x89, 0xe5, 0x41, 0x28, 0x13, 0x6b, 0x35, 0x75, 0x8e, 0xe9, 0x9c, 0x58, 0xb1,
	0x48, 0xcf, 0xd0, 0x2b, 0xcc, 0x09, 0x63, 0x32, 0x15, 0x3a, 0xed, 0x50, 0x49, 0x99, 0x5d, 0xae,
	0x6e, 0xfa, 0x53, 0x63, 0x85, 0xa2, 0x19, 0x19, 0xa1, 0xcb, 0x10, 0xcc, 0xde, 0x03, 0x01, 0x98,
	0xcf, 0x5e, 0x12, 0xbc, 0xf9, 0xf6, 0x09, 0x9a, 0xc2, 0x79, 0x10, 0xea, 0x9d, 0x81, 0xe6, 0x80,
	0xf6, 0xe1, 0xc6, 0x8f, 0x52, 0x6f, 0x17, 0xc6, 0x49, 0xb4, 0xf7, 0x92, 0x60, 0x17, 0xda, 0x13,
	0x84, 0xc0, 0xea, 0xf9, 0xc6, 0xdf, 0xfa, 0x8a, 0x9d, 0xae, 0x9f, 0xde, 0x1f, 0x8b, 0x4a, 0x96,
	0x07, 0xe2, 0x64, 0xac, 0x71, 0xcb, 0xef, 0x8e, 0xf2, 0xfe, 0xf5, 0xdd, 0x0f, 0x4c, 0x78, 0x95,
	0xb9, 0x19, 0xe3, 0xd4, 0xd5, 0xe8, 0xf3, 0x4b, 0x2f, 0x8e, 0xa9, 0x89, 0xa9, 0xbe, 0xe5, 0xe1,
	0x37, 0x00, 0x00, 0xff, 0xff, 0xaa, 0xee, 0x9d, 0x64, 0xaf, 0x01, 0x00, 0x00,
}
