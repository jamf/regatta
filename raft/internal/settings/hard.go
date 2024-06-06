// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Package settings is used for managing internal parameters that can be set at
compile time by expert level users. Most of those parameters can also be
overwritten by using the json mechanism described below.
*/
package settings

const (
	LRUMaxSessionCount  = 4096
	LogDBEntryBatchSize = 48
)

// BlockFileMagicNumber is the magic number used in block based snapshot files.
var BlockFileMagicNumber = []byte{0x3F, 0x5B, 0xCB, 0xF1, 0xFA, 0xBA, 0x81, 0x9F}

const (
	//
	// RSM
	//

	// SnapshotHeaderSize defines the snapshot header size in number of bytes.
	SnapshotHeaderSize uint64 = 1024

	//
	// transport
	//

	// UnmanagedDeploymentID is the special deployment ID value used when no user
	// deployment ID is specified.
	UnmanagedDeploymentID uint64 = 1
	// MaxMessageBatchSize is the max size for a single message batch sent between
	// nodehosts.
	MaxMessageBatchSize uint64 = LargeEntitySize
	// SnapshotChunkSize is the snapshot chunk size.
	SnapshotChunkSize uint64 = 2 * 1024 * 1024
)
