package raft

import (
	"fmt"
	"time"

	metrics "github.com/armon/go-metrics"
)

// LogType describes various types of log entries.
// 表示log的类型
type LogType uint8

const (
	// LogCommand is applied to a user FSM.
	LogCommand LogType = iota

	// LogNoop is used to assert leadership.
	LogNoop

	// LogAddPeerDeprecated is used to add a new peer. This should only be used with
	// older protocol versions designed to be compatible with unversioned
	// Raft servers. See comments in config.go for details.
	LogAddPeerDeprecated

	// LogRemovePeerDeprecated is used to remove an existing peer. This should only be
	// used with older protocol versions designed to be compatible with
	// unversioned Raft servers. See comments in config.go for details.
	LogRemovePeerDeprecated

	// LogBarrier is used to ensure all preceding operations have been
	// applied to the FSM. It is similar to LogNoop, but instead of returning
	// once committed, it only returns once the FSM manager acks it. Otherwise
	// it is possible there are operations committed but not yet applied to
	// the FSM.
	// 阻塞版本的no-op
	LogBarrier

	// LogConfiguration establishes a membership change configuration. It is
	// created when a server is added, removed, promoted, etc. Only used
	// when protocol version 1 or greater is in use.
	// 集群配置变更的log
	LogConfiguration
)

// String returns LogType as a human readable string.
func (lt LogType) String() string {
	switch lt {
	case LogCommand:
		return "LogCommand"
	case LogNoop:
		return "LogNoop"
	case LogAddPeerDeprecated:
		return "LogAddPeerDeprecated"
	case LogRemovePeerDeprecated:
		return "LogRemovePeerDeprecated"
	case LogBarrier:
		return "LogBarrier"
	case LogConfiguration:
		return "LogConfiguration"
	default:
		return fmt.Sprintf("%d", lt)
	}
}

// Log entries are replicated to all members of the Raft cluster
// and form the heart of the replicated state machine.
type Log struct {
	// Index holds the index of the log entry.
	// log日志的index
	Index uint64

	// Term holds the election term of the log entry.
	// log日志的term
	Term uint64

	// Type holds the type of the log entry.
	// log的type
	Type LogType

	// Data holds the log entry's type-specific data.
	// log的真实数据
	Data []byte

	// Extensions holds an opaque byte slice of information for middleware. It
	// is up to the client of the library to properly modify this as it adds
	// layers and remove those layers when appropriate. This value is a part of
	// the log, so very large values could cause timing issues.
	//
	// N.B. It is _up to the client_ to handle upgrade paths. For instance if
	// using this with go-raftchunking, the client should ensure that all Raft
	// peers are using a version that can handle that extension before ever
	// actually triggering chunking behavior. It is sometimes sufficient to
	// ensure that non-leaders are upgraded first, then the current leader is
	// upgraded, but a leader changeover during this process could lead to
	// trouble, so gating extension behavior via some flag in the client
	// program is also a good idea.
	// 由client来处理的Extensions 扩展使用
	Extensions []byte

	// AppendedAt stores the time the leader first appended this log to it's
	// LogStore. Followers will observe the leader's time. It is not used for
	// coordination or as part of the replication protocol at all. It exists only
	// to provide operational information for example how many seconds worth of
	// logs are present on the leader which might impact follower's ability to
	// catch up after restoring a large snapshot. We should never rely on this
	// being in the past when appending on a follower or reading a log back since
	// the clock skew can mean a follower could see a log with a future timestamp.
	// In general too the leader is not required to persist the log before
	// delivering to followers although the current implementation happens to do
	// this.
	// leader第一次追加这个日志的时间，由于时钟问题不可靠，只是用来追踪/监控的字段
	AppendedAt time.Time
}

// LogStore is used to provide an interface for storing
// and retrieving logs in a durable fashion.
// 用于存储日志和检索日志的抽象
type LogStore interface {
	// FirstIndex returns the first index written. 0 for no entries.
	FirstIndex() (uint64, error)

	// LastIndex returns the last index written. 0 for no entries.
	LastIndex() (uint64, error)

	// GetLog gets a log entry at a given index.
	GetLog(index uint64, log *Log) error

	// StoreLog stores a log entry.
	StoreLog(log *Log) error

	// StoreLogs stores multiple log entries.
	StoreLogs(logs []*Log) error

	// DeleteRange deletes a range of log entries. The range is inclusive.
	DeleteRange(min, max uint64) error
}

// 获取最早的log
func oldestLog(s LogStore) (Log, error) {
	var l Log

	// We might get unlucky and have a truncate right between getting first log
	// index and fetching it so keep trying until we succeed or hard fail.
	var lastFailIdx uint64
	var lastErr error
	for {
		firstIdx, err := s.FirstIndex()
		if err != nil {
			return l, err
		}
		if firstIdx == 0 {
			return l, ErrLogNotFound
		}
		if firstIdx == lastFailIdx {
			// Got same index as last time around which errored, don't bother trying
			// to fetch it again just return the error.
			return l, lastErr
		}
		err = s.GetLog(firstIdx, &l)
		if err == nil {
			// We found the oldest log, break the loop
			break
		}
		// We failed, keep trying to see if there is a new firstIndex
		lastFailIdx = firstIdx
		lastErr = err
	}
	return l, nil
}

func emitLogStoreMetrics(s LogStore, prefix []string, interval time.Duration, stopCh <-chan struct{}) {
	for {
		select {
		case <-time.After(interval):
			// In error case emit 0 as the age
			ageMs := float32(0.0)
			l, err := oldestLog(s)
			if err == nil && !l.AppendedAt.IsZero() {
				ageMs = float32(time.Since(l.AppendedAt).Milliseconds())
			}
			metrics.SetGauge(append(prefix, "oldestLogAge"), ageMs)
		case <-stopCh:
			return
		}
	}
}
