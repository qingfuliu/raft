// Copyright 2019 The etcd Authors
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

package rafttest

import (
	"bufio"
	"fmt"
	"math"
	"strings"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// InteractionOpts groups the options for an InteractionEnv.
type InteractionOpts struct {
	OnConfig func(*raft.Config)

	// SetRandomizedElectionTimeout is used to plumb this function down from the
	// raft test package.
	SetRandomizedElectionTimeout func(node *raft.RawNode, timeout int)
}

// Node is a member of a raft group tested via an InteractionEnv.
type Node struct {
	*raft.RawNode
	Storage

	Config     *raft.Config
	AppendWork []pb.Message // []MsgStorageAppend
	ApplyWork  []pb.Message // []MsgStorageApply
	History    []pb.Snapshot
}

// InteractionEnv facilitates testing of complex interactions between the
// members of a raft group.
type InteractionEnv struct {
	Options  *InteractionOpts
	Nodes    []Node
	Messages []pb.Message // in-flight messages

	Output *RedirectLogger
}

// NewInteractionEnv initializes an InteractionEnv. opts may be nil.
func NewInteractionEnv(opts *InteractionOpts) *InteractionEnv {
	if opts == nil {
		opts = &InteractionOpts{}
	}
	return &InteractionEnv{
		Options: opts,
		Output: &RedirectLogger{
			Builder: &strings.Builder{},
		},
	}
}

func (env *InteractionEnv) withIndent(f func()) {
	orig := env.Output.Builder
	env.Output.Builder = &strings.Builder{}
	f()

	scanner := bufio.NewScanner(strings.NewReader(env.Output.Builder.String()))
	for scanner.Scan() {
		orig.WriteString("  " + scanner.Text() + "\n")
	}
	env.Output.Builder = orig
}

// Storage is the interface used by InteractionEnv. It is comprised of raft's
// Storage interface plus access to operations that maintain the log and drive
// the Ready handling loop.
type Storage interface {
	raft.Storage
	SetHardState(state pb.HardState) error
	/**
	将快照应用到log，新快照必须比旧快照新
	overwrites（抛弃原先的快照和日志）
	*/
	ApplySnapshot(pb.Snapshot) error
	Compact(newFirstIndex uint64) error
	Append([]pb.Entry) error
}

// raftConfigStub sets up a raft.Config stub with reasonable testing defaults.
// In particular, no limits are set. It is not a complete config: ID and Storage
// must be set for each node using the stub as a template.
func raftConfigStub() raft.Config {
	return raft.Config{
		ElectionTick:    3,
		HeartbeatTick:   1,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: math.MaxInt32,
	}
}

func defaultEntryFormatter(b []byte) string {
	return fmt.Sprintf("%q", b)
}
