package raft

import (
	"io"
	"time"
)

// RPCResponse captures both a response and a potential error.
// 对RPC响应的封装
type RPCResponse struct {
	Response interface{}
	Error    error
}

// RPC has a command, and provides a response mechanism.
// 对RPC的抽象
type RPC struct {
	Command  interface{}
	// 只有InstallSnapshot请求时才会被赋值
	Reader   io.Reader // Set only for InstallSnapshot
	RespChan chan<- RPCResponse
}

// Respond is used to respond with a response, error or both
// 响应
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, err}
}

// Transport provides an interface for network transports
// to allow Raft to communicate with other nodes.
// 规定了raft节点之间的通信
type Transport interface {
	// Consumer returns a channel that can be used to
	// consume and respond to RPC requests.
	// 返回一个消费rpc请求的channel
	Consumer() <-chan RPC

	// LocalAddr is used to return our local address to distinguish from our peers.
	// 返回本地的地址
	LocalAddr() ServerAddress

	// AppendEntriesPipeline returns an interface that can be used to pipeline
	// AppendEntries requests.
	// 返回一个AppendPipeline用于AppendEntries请求的管道
	AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error)

	// AppendEntries sends the appropriate RPC to the target node.
	// 向目标节点发送合适的rpc请求
	AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error

	// RequestVote sends the appropriate RPC to the target node.
	// 向目标节点发送合适的rpc请求
	RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error

	// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
	// the ReadCloser and streamed to the client.
	// 向follower推送快照
	InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error

	// EncodePeer is used to serialize a peer's address.
	// 用于序列化peer的地址
	EncodePeer(id ServerID, addr ServerAddress) []byte

	// DecodePeer is used to deserialize a peer's address.
	// 用于反序列化peer的地址
	DecodePeer([]byte) ServerAddress

	// SetHeartbeatHandler is used to setup a heartbeat handler
	// as a fast-pass. This is to avoid head-of-line blocking from
	// disk IO. If a Transport does not support this, it can simply
	// ignore the call, and push the heartbeat onto the Consumer channel.
	SetHeartbeatHandler(cb func(rpc RPC))

	// TimeoutNow is used to start a leadership transfer to the target node.
	// 向目标节点启动一次领导权转换
	TimeoutNow(id ServerID, target ServerAddress, args *TimeoutNowRequest, resp *TimeoutNowResponse) error
}

// WithClose is an interface that a transport may provide which
// allows a transport to be shut down cleanly when a Raft instance
// shuts down.
//
// It is defined separately from Transport as unfortunately it wasn't in the
// original interface specification.
// 规定了raft实例关闭时transport的关闭行为
type WithClose interface {
	// Close permanently closes a transport, stopping
	// any associated goroutines and freeing other resources.
	Close() error
}

// LoopbackTransport is an interface that provides a loopback transport suitable for testing
// e.g. InmemTransport. It's there so we don't have to rewrite tests.
// 测试使用的回环传输抽象
type LoopbackTransport interface {
	Transport // Embedded transport reference
	WithPeers // Embedded peer management
	WithClose // with a close routine
}

// WithPeers is an interface that a transport may provide which allows for connection and
// disconnection. Unless the transport is a loopback transport, the transport specified to
// "Connect" is likely to be nil.
// transport可能会实现的接口用于和peers之间的连接和断连
type WithPeers interface {
	Connect(peer ServerAddress, t Transport) // Connect a peer
	Disconnect(peer ServerAddress)           // Disconnect a given peer
	DisconnectAll()                          // Disconnect all peers, possibly to reconnect them later
}

// AppendPipeline is used for pipelining AppendEntries requests. It is used
// to increase the replication throughput by masking latency and better
// utilizing bandwidth.
// 加速日志复制吞吐量 屏蔽延迟 更好的利用带宽
type AppendPipeline interface {
	// AppendEntries is used to add another request to the pipeline.
	// The send may block which is an effective form of back-pressure.
	// 向pipeline中加入一个新的AppendEntries请求
	AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error)

	// Consumer returns a channel that can be used to consume
	// response futures when they are ready.
	// 返回一个消费channel
	Consumer() <-chan AppendFuture

	// Close closes the pipeline and cancels all inflight RPCs
	// 关闭pipeline并取消所有inflight rpc
	Close() error
}

// AppendFuture is used to return information about a pipelined AppendEntries request.
type AppendFuture interface {
	Future

	// Start returns the time that the append request was started.
	// It is always OK to call this method.
	// 返回append request开始的时间
	Start() time.Time

	// Request holds the parameters of the AppendEntries call.
	// It is always OK to call this method.
	// 返回request
	Request() *AppendEntriesRequest

	// Response holds the results of the AppendEntries call.
	// This method must only be called after the Error
	// method returns, and will only be valid on success.
	// 返回response
	Response() *AppendEntriesResponse
}
