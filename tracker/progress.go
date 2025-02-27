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

import (
	"fmt"
	"slices"
	"strings"
)

// Progress represents a follower’s progress in the view of the leader. Leader
// maintains progresses of all followers, and sends entries to the follower
// based on its progress.
//
// NB(tbg): Progress is basically a state machine whose transitions are mostly
// strewn around `*raft.raft`. Additionally, some fields are only used when in a
// certain State. All of this isn't ideal.
// +--------------------------------------------------------+
// |                  send snapshot                         |
// |                                                        |
// +---------+----------+                                  +----------v---------+
// +--->       probe        |                                  |      snapshot      |
// |   |  max inflight = 1  <----------------------------------+  max inflight = 0  |
// |   +---------+----------+                                  +--------------------+
// |             |            1. snapshot success
// |             |               (next=snapshot.index + 1)
// |             |            2. snapshot failure
// |             |               (no change)
// |             |            3. receives msgAppResp(rej=false&&index>lastsnap.index)
// |             |               (match=m.index,next=match+1)
// receives msgAppResp(rej=true)
// (next=match+1)|             |
// |             |
// |             |
// |             |   receives msgAppResp(rej=false&&index>match)
// |             |   (match=m.index,next=match+1)
// |             |
// |             |
// |             |
// |   +---------v----------+
// |   |     replicate      |
// +---+  max inflight = n  |
// +--------------------+
// https://xujiajiadexiaokeai.github.io/2021-10-28/progress-in-etcd/
type Progress struct {
	// Match is the index up to which the follower's log is known to match the
	// leader's.
	//追随者的日志与领导者的日志相匹配的位置的索引。
	Match uint64
	// Next is the log index of the next entry to send to this follower. All
	// entries with indices in (Match, Next) interval are already in flight.
	//
	// Invariant: 0 <= Match < Next.
	// NB: it follows that Next >= 1.
	//
	// In StateSnapshot, Next == PendingSnapshot + 1.
	/**
	发送给follower的下一条日志的索引
	0 <= Match < Next，(Match, Next) 之间的条目已经被发送了
	*/
	Next uint64

	// sentCommit is the highest commit index in flight to the follower.
	//
	// Generally, it is monotonic, but con regress in some cases, e.g. when
	// converting to `StateProbe` or when receiving a rejection from a follower.
	//
	// In StateSnapshot, sentCommit == PendingSnapshot == Next-1.

	/*
			sentCommit 是发送给跟随者的最高提交索引。
		    通常，它是单调的，但在某些情况下会出现倒退，例如当State转换为 `StateProbe` 或收到跟随者的拒绝时。
			在 StateSnapshot 中，sentCommit == PendingSnapshot == Next-1。
	*/
	sentCommit uint64

	// State defines how the leader should interact with the follower.
	//
	// When in StateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	//
	// When in StateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in StateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	/**
	在StateProbe中，leader每个心跳间隔最多发送一条复制消息。它还探讨了追随者的实际进展。
	在StateReplicate中，leader乐观地增加next到发送Replicate消息时发送的最新条目之后。这是一种优化状态，用于将日志条目快速复制到关注者。
	在StateSnapshot中，leader之前应该已经发送了快照，并停止发送任何复制消息。
	*/
	State StateType

	// PendingSnapshot is used in StateSnapshot and tracks the last index of the
	// leader at the time at which it realized a snapshot was necessary. This
	// matches the index in the MsgSnap message emitted from raft.
	//
	// While there is a pending snapshot, replication to the follower is paused.
	// The follower will transition back to StateReplicate if the leader
	// receives an MsgAppResp from it that reconnects the follower to the
	// leader's log (such an MsgAppResp is emitted when the follower applies a
	// snapshot). It may be surprising that PendingSnapshot is not taken into
	// account here, but consider that complex systems may delegate the sending
	// of snapshots to alternative datasources (i.e. not the leader). In such
	// setups, it is difficult to manufacture a snapshot at a particular index
	// requested by raft and the actual index may be ahead or behind. This
	// should be okay, as long as the snapshot allows replication to resume.
	//
	// The follower will transition to StateProbe if ReportSnapshot is called on
	// the leader; if SnapshotFinish is passed then PendingSnapshot becomes the
	// basis for the next attempt to append. In practice, the first mechanism is
	// the one that is relevant in most cases. However, if this MsgAppResp is
	// lost (fallible network) then the second mechanism ensures that in this
	// case the follower does not erroneously remain in StateSnapshot.
	/*
			PendingSnapshot 用于 StateSnapshot，
		    当领导者意识到这个节点需要一个快照时候，用于跟踪最新日志的index。这与从 raft 发出的 MsgSnap 消息中的索引相匹配。
			当有待处理的快照时，向跟随者的复制将暂停。
			如果领导者从其收到 MsgAppResp（当跟随者应用快照时会发出这样的 MsgAppResp)，将跟随者重新连接到领导者的日志（转换为StateReplicate）
	*/
	PendingSnapshot uint64

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	// This is always true on the leader.
	/*
			如果节点最近处于活动状态，则 RecentActive 为真。从相应的跟随者接收任何消息都节点进度处于活动状态。
		   一个选举超时时间没有接收到任何消息，RecentActive 可以重置为 false。
			在领导者身上，RecentActive始终为真。
	*/
	RecentActive bool

	// MsgAppFlowPaused is used when the MsgApp flow to a node is throttled. This
	// happens in StateProbe, or StateReplicate with saturated Inflights. In both
	// cases, we need to continue sending MsgApp once in a while to guarantee
	// progress, but we only do so when MsgAppFlowPaused is false (it is reset on
	// receiving a heartbeat response), to not overflow the receiver. See
	// IsPaused().
	/*
		当发送给该节点的 MsgApp 流受到限制时，将使用 MsgAppFlowPaused。
		这种情况发生在 StateProbe 或 Inflights 饱和的 StateReplicate 中。
		在这两种情况下，我们都需要不时继续发送 MsgApp 以保证进度，
		但我们仅在 MsgAppFlowPaused 为 false（在收到心跳响应时重置）时才这样做，以免接收器溢出。请参阅 IsPaused()。
	*/
	MsgAppFlowPaused bool

	// Inflights is a sliding window for the inflight messages.
	// Each inflight message contains one or more log entries.
	// The max number of entries per message is defined in raft config as MaxSizePerMsg.
	// Thus inflight effectively limits both the number of inflight messages
	// and the bandwidth each Progress can use.
	// When inflights is Full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.FreeLE with the index of the last
	// received entry.
	/*
		Inflights是正在发送的消息的滑动窗口
		每一个滑动窗口包含一个或多个日志
		每条消息的最大日志条目数在 raft 配置中定义为 MaxSizePerMsg。
		当领导者发出消息时，最后一个条目的索引应该添加到 inflights 中。索引必须按顺序添加到 inflights 中。
		当领导者收到回复时，应该通过使用最后收到的条目的索引调用 inflights.FreeLE 来释放先前的 inflights。
		因此，inflight 有效地限制了飞行中消息的数量，以及每个 Progress 可以使用的带宽，当 inflights 已满时，不应再发送消息。
	*/
	Inflights *Inflights

	// IsLearner is true if this progress is tracked for a learner.
	/*
		如果节点是learner，那么IsLearner为true
	*/
	IsLearner bool
}

// ResetState moves the Progress into the specified State, resetting MsgAppFlowPaused,
// PendingSnapshot, and Inflights.

/*
重制Progress：
刷新Inflights
设置State
设置PendingSnapshot 和 MsgAppFlowPaused 为false
*/
func (pr *Progress) ResetState(state StateType) {
	pr.MsgAppFlowPaused = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.Inflights.reset()
}

// BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
// optionally and if larger, the index of the pending snapshot.

/*
转换为StateProbe状态
1、pr.State == StateSnapshot  pr.Next = max(pr.Match+1, pendingSnapshot+1)
2、pr.Next = pr.Match + 1
pr.sentCommit = min(pr.sentCommit, pr.Next-1)
*/
func (pr *Progress) BecomeProbe() {
	// If the original state is StateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
	if pr.State == StateSnapshot {
		pendingSnapshot := pr.PendingSnapshot
		pr.ResetState(StateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.ResetState(StateProbe)
		pr.Next = pr.Match + 1
	}
	pr.sentCommit = min(pr.sentCommit, pr.Next-1)
}

// BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
/*
转换为StateReplicate
pr.Next = pr.Match + 1
*/
func (pr *Progress) BecomeReplicate() {
	pr.ResetState(StateReplicate)
	pr.Next = pr.Match + 1
}

// BecomeSnapshot moves the Progress to StateSnapshot with the specified pending
// snapshot index.
/*
转换为StateSnapshot。将index<=snapshoti的日志作为索引转换为快照发送出去
	pr.PendingSnapshot = snapshoti
	pr.Next = snapshoti + 1
	pr.sentCommit = snapshoti
*/
func (pr *Progress) BecomeSnapshot(snapshoti uint64) {
	pr.ResetState(StateSnapshot)
	pr.PendingSnapshot = snapshoti
	pr.Next = snapshoti + 1
	pr.sentCommit = snapshoti
}

// SentEntries updates the progress on the given number of consecutive entries
// being sent in a MsgApp, with the given total bytes size, appended at log
// indices >= pr.Next.
//
// Must be used with StateProbe or StateReplicate.

/*
更新：

	pr.Next += uint64(entries)
	pr.Inflights.Add(pr.Next-1, bytes)
	pr.MsgAppFlowPaused = pr.Inflights.Full()
*/
func (pr *Progress) SentEntries(entries int, bytes uint64) {
	switch pr.State {
	case StateReplicate:
		if entries > 0 {
			pr.Next += uint64(entries)
			pr.Inflights.Add(pr.Next-1, bytes)
		}
		// If this message overflows the in-flights tracker, or it was already full,
		// consider this message being a probe, so that the flow is paused.
		pr.MsgAppFlowPaused = pr.Inflights.Full()
	case StateProbe:
		// TODO(pavelkalinnikov): this condition captures the previous behaviour,
		// but we should set MsgAppFlowPaused unconditionally for simplicity, because any
		// MsgApp in StateProbe is a probe, not only non-empty ones.
		if entries > 0 {
			pr.MsgAppFlowPaused = true
		}
	default:
		panic(fmt.Sprintf("sending append in unhandled state %s", pr.State))
	}
}

// CanBumpCommit returns true if sending the given commit index can potentially
// advance the follower's commit index.
func (pr *Progress) CanBumpCommit(index uint64) bool {
	// Sending the given commit index may bump the follower's commit index up to
	// Next-1 in normal operation, or higher in some rare cases. Allow sending a
	// commit index eagerly only if we haven't already sent one that bumps the
	// follower's commit all the way to Next-1.
	return index > pr.sentCommit && pr.sentCommit < pr.Next-1
}

// SentCommit updates the sentCommit.

/*
代表commit及其之前的日志已经更新了，Progress需要更新pr.sentCommit = commit
*/
func (pr *Progress) SentCommit(commit uint64) {
	pr.sentCommit = commit
}

// MaybeUpdate is called when an MsgAppResp arrives from the follower, with the
// index acked by it. The method returns false if the given n index comes from
// an outdated message. Otherwise it updates the progress and returns true.

/*
当接收者返回MsgAppResp时调用
如果是过期的msg，也就是n <= pr.Matc，返回false
否则，更新pr.Match = n
更新pr.Next和pr.MsgAppFlowPaused = false
*/
func (pr *Progress) MaybeUpdate(n uint64) bool {
	if n <= pr.Match {
		return false
	}
	pr.Match = n
	pr.Next = max(pr.Next, n+1) // invariant: Match < Next
	pr.MsgAppFlowPaused = false
	return true
}

// MaybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection. The
// arguments are the index of the append message rejected by the follower, and
// the hint that we want to decrease to.
//
// Rejections can happen spuriously as messages are sent out of order or
// duplicated. In such cases, the rejection pertains to an index that the
// Progress already knows were previously acknowledged, and false is returned
// without changing the Progress.
//
// If the rejection is genuine, Next is lowered sensibly, and the Progress is
// cleared for sending log entries.

/*
如果日志被拒绝了，MaybeDecrTo需要被调用以减小next
1.status == StateReplicate&& rejected > pr.Match，pr.Next = pr.Match + 1,pr.sentCommit = min(pr.sentCommit, pr.Next-1)

		2.if pr.Next-1 != rejected {
						return false
	}

pr.Next = max(min(rejected, matchHint+1), pr.Match+1)
pr.sentCommit = min(pr.sentCommit, pr.Next-1)
*/
func (pr *Progress) MaybeDecrTo(rejected, matchHint uint64) bool {
	if pr.State == StateReplicate {
		// The rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match {
			return false
		}
		// Directly decrease next to match + 1.
		//
		// TODO(tbg): why not use matchHint if it's larger?
		pr.Next = pr.Match + 1
		// Regress the sentCommit since it unlikely has been applied.
		pr.sentCommit = min(pr.sentCommit, pr.Next-1)
		return true
	}

	// The rejection must be stale if "rejected" does not match next - 1. This
	// is because non-replicating followers are probed one entry at a time.
	// The check is a best effort assuming message reordering is rare.
	if pr.Next-1 != rejected {
		return false
	}

	pr.Next = max(min(rejected, matchHint+1), pr.Match+1)
	// Regress the sentCommit since it unlikely has been applied.
	pr.sentCommit = min(pr.sentCommit, pr.Next-1)
	pr.MsgAppFlowPaused = false
	return true
}

// IsPaused returns whether sending log entries to this node has been throttled.
// This is done when a node has rejected recent MsgApps, is currently waiting
// for a snapshot, or has reached the MaxInflightMsgs limit. In normal
// operation, this is false. A throttled node will be contacted less frequently
// until it has reached a state in which it's able to accept a steady stream of
// log entries again.

/*
* state==StateSnapshot or pr.MsgAppFlowPaused
 */
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case StateProbe:
		return pr.MsgAppFlowPaused
	case StateReplicate:
		return pr.MsgAppFlowPaused
	case StateSnapshot:
		return true
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s match=%d next=%d", pr.State, pr.Match, pr.Next)
	if pr.IsLearner {
		fmt.Fprint(&buf, " learner")
	}
	if pr.IsPaused() {
		fmt.Fprint(&buf, " paused")
	}
	if pr.PendingSnapshot > 0 {
		fmt.Fprintf(&buf, " pendingSnap=%d", pr.PendingSnapshot)
	}
	if !pr.RecentActive {
		fmt.Fprint(&buf, " inactive")
	}
	if n := pr.Inflights.Count(); n > 0 {
		fmt.Fprintf(&buf, " inflight=%d", n)
		if pr.Inflights.Full() {
			fmt.Fprint(&buf, "[full]")
		}
	}
	return buf.String()
}

// ProgressMap is a map of *Progress.
type ProgressMap map[uint64]*Progress

// String prints the ProgressMap in sorted key order, one Progress per line.
func (m ProgressMap) String() string {
	ids := make([]uint64, 0, len(m))
	for k := range m {
		ids = append(ids, k)
	}
	slices.Sort(ids)
	var buf strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&buf, "%d: %s\n", id, m[id])
	}
	return buf.String()
}
