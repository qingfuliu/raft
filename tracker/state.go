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

package tracker

// StateType is the state of a tracked follower.
type StateType uint64

const (
	// StateProbe indicates a follower whose last index isn't known. Such a
	// follower is "probed" (i.e. an append sent periodically) to narrow down
	// its last index. In the ideal (and common) case, only one round of probing
	// is necessary as the follower will react with a hint. Followers that are
	// probed over extended periods of time are often offline.

	/*
			“状态探测（StateProbe）” 表示一个其最后索引未知的跟随者。
		对于这样的跟随者，会对其进行 “探测”（即定期发送追加信息），以便缩小其最后索引的范围。
		在理想（且常见）的情况下，只需要一轮探测就足够了，因为跟随者会回复一个提示信息。
		那些长时间被探测的跟随者通常处于离线状态。
	*/
	StateProbe StateType = iota
	// StateReplicate is the state steady in which a follower eagerly receives
	// log entries to append to its log.

	/*
		“StateReplicate”（复制状态）是一种稳定状态，处于该状态下的跟随者会积极地接收日志条目并将其追加到自身的日志中。
	*/
	StateReplicate
	// StateSnapshot indicates a follower that needs log entries not available
	// from the leader's Raft log. Such a follower needs a full snapshot to
	// return to StateReplicate.
	/*
			“StateSnapshot”（快照状态）表示某个跟随者需要的日志条目在领导者的 Raft 日志中不存在。
		这样的跟随者需要完整的快照数据，才能恢复到“StateReplicate”（复制状态）
	*/
	StateSnapshot
)

var prstmap = [...]string{
	"StateProbe",
	"StateReplicate",
	"StateSnapshot",
}

func (st StateType) String() string { return prstmap[st] }
