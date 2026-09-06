#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/codec.h"
#include "protocol/types.h"
#include "storage/record.h"

// Pure encode/decode structs for the M2 request/response payloads. No I/O —
// net/server.cpp reads/writes the outer frame and hands the payload bytes
// here. Records reuse storage/record.h's on-disk frame format verbatim, per
// AGENT_BRIEF.md.

namespace mk {

struct ProduceRequest {
    std::string topic;
    uint32_t partitionId = 0;
    uint8_t acks = 1;
    std::vector<Record> records;
};
void encode_produce_request(const ProduceRequest& req, std::vector<uint8_t>& buf);
ProduceRequest decode_produce_request(Reader& r);

struct ProduceResponse {
    ErrorCode errorCode = ErrorCode::None;
    uint64_t baseOffset = 0;
    // Populated only when errorCode == NotLeaderForPartition (M4): redirect target.
    std::string leaderHost;
    uint32_t leaderPort = 0;
};
void encode_produce_response(const ProduceResponse& resp, std::vector<uint8_t>& buf);
ProduceResponse decode_produce_response(Reader& r);

struct FetchRequest {
    std::string topic;
    uint32_t partitionId = 0;
    uint64_t fetchOffset = 0;
    uint32_t maxBytes = 0;
};
void encode_fetch_request(const FetchRequest& req, std::vector<uint8_t>& buf);
FetchRequest decode_fetch_request(Reader& r);

// M5: FETCH_REPLICA. Not byte-identical to FetchRequest despite the brief's
// "same framing as FETCH" — the leader needs to know which broker is asking,
// to update the right follower's progress. Response reuses FetchResponse.
struct ReplicaFetchRequest {
    std::string topic;
    uint32_t partitionId = 0;
    uint32_t replicaId = 0;
    uint64_t fetchOffset = 0;
    uint32_t maxBytes = 0;
};
void encode_replica_fetch_request(const ReplicaFetchRequest& req, std::vector<uint8_t>& buf);
ReplicaFetchRequest decode_replica_fetch_request(Reader& r);

struct FetchResponse {
    ErrorCode errorCode = ErrorCode::None;
    uint64_t highWatermark = 0;
    std::vector<Record> records;
    // Populated only when errorCode == NotLeaderForPartition (M4): redirect target.
    std::string leaderHost;
    uint32_t leaderPort = 0;
};
void encode_fetch_response(const FetchResponse& resp, std::vector<uint8_t>& buf);
FetchResponse decode_fetch_response(Reader& r);

struct MetadataRequest {
    std::string topic;
};
void encode_metadata_request(const MetadataRequest& req, std::vector<uint8_t>& buf);
MetadataRequest decode_metadata_request(Reader& r);

struct PartitionMetadata {
    uint32_t partitionId = 0;
    uint32_t leaderId = 0;
    std::string leaderHost;
    uint32_t leaderPort = 0;
};

struct MetadataResponse {
    ErrorCode errorCode = ErrorCode::None;
    std::vector<PartitionMetadata> partitions;
};
void encode_metadata_response(const MetadataResponse& resp, std::vector<uint8_t>& buf);
MetadataResponse decode_metadata_response(Reader& r);

struct CreateTopicRequest {
    std::string topic;
    uint32_t numPartitions = 1;
    uint32_t replicationFactor = 1;  // M5; ignored in standalone mode
};
void encode_create_topic_request(const CreateTopicRequest& req, std::vector<uint8_t>& buf);
CreateTopicRequest decode_create_topic_request(Reader& r);

struct CreateTopicResponse {
    ErrorCode errorCode = ErrorCode::None;
    // Populated only when errorCode == NotLeaderForPartition (M4, reused here for
    // "wrong broker for this cluster-control op"): redirect to the controller.
    uint32_t controllerId = 0;
    std::string controllerHost;
    uint32_t controllerPort = 0;
};
void encode_create_topic_response(const CreateTopicResponse& resp, std::vector<uint8_t>& buf);
CreateTopicResponse decode_create_topic_response(Reader& r);

// ---- Consumer groups (M3) ----
// AGENT_BRIEF.md describes these conceptually but doesn't specify byte-level
// payloads (unlike Produce/Fetch/Metadata) — these follow the same
// length-prefixed-string / big-endian style as the rest of protocol/.

struct FindCoordinatorRequest {
    std::string groupId;
};
void encode_find_coordinator_request(const FindCoordinatorRequest& req, std::vector<uint8_t>& buf);
FindCoordinatorRequest decode_find_coordinator_request(Reader& r);

struct FindCoordinatorResponse {
    ErrorCode errorCode = ErrorCode::None;
    uint32_t coordinatorId = 0;
    std::string host;
    uint32_t port = 0;
};
void encode_find_coordinator_response(const FindCoordinatorResponse& resp, std::vector<uint8_t>& buf);
FindCoordinatorResponse decode_find_coordinator_response(Reader& r);

struct JoinGroupRequest {
    std::string groupId;
    std::string topic;
    uint32_t sessionTimeoutMs = 0;
};
void encode_join_group_request(const JoinGroupRequest& req, std::vector<uint8_t>& buf);
JoinGroupRequest decode_join_group_request(Reader& r);

struct JoinGroupResponse {
    ErrorCode errorCode = ErrorCode::None;
    uint32_t generationId = 0;
    std::string memberId;      // coordinator-assigned
    std::string leaderId;
    std::vector<std::string> members;
};
void encode_join_group_response(const JoinGroupResponse& resp, std::vector<uint8_t>& buf);
JoinGroupResponse decode_join_group_response(Reader& r);

struct SyncGroupAssignment {
    std::string memberId;
    std::vector<uint32_t> partitions;
};

struct SyncGroupRequest {
    std::string groupId;
    std::string memberId;
    uint32_t generationId = 0;
    std::vector<SyncGroupAssignment> assignments;  // non-empty only from the leader
};
void encode_sync_group_request(const SyncGroupRequest& req, std::vector<uint8_t>& buf);
SyncGroupRequest decode_sync_group_request(Reader& r);

struct SyncGroupResponse {
    ErrorCode errorCode = ErrorCode::None;
    std::vector<uint32_t> partitions;  // this member's assignment
};
void encode_sync_group_response(const SyncGroupResponse& resp, std::vector<uint8_t>& buf);
SyncGroupResponse decode_sync_group_response(Reader& r);

struct HeartbeatRequest {
    std::string groupId;
    std::string memberId;
    uint32_t generationId = 0;
};
void encode_heartbeat_request(const HeartbeatRequest& req, std::vector<uint8_t>& buf);
HeartbeatRequest decode_heartbeat_request(Reader& r);

struct HeartbeatResponse {
    ErrorCode errorCode = ErrorCode::None;
};
void encode_heartbeat_response(const HeartbeatResponse& resp, std::vector<uint8_t>& buf);
HeartbeatResponse decode_heartbeat_response(Reader& r);

struct LeaveGroupRequest {
    std::string groupId;
    std::string memberId;
};
void encode_leave_group_request(const LeaveGroupRequest& req, std::vector<uint8_t>& buf);
LeaveGroupRequest decode_leave_group_request(Reader& r);

struct LeaveGroupResponse {
    ErrorCode errorCode = ErrorCode::None;
};
void encode_leave_group_response(const LeaveGroupResponse& resp, std::vector<uint8_t>& buf);
LeaveGroupResponse decode_leave_group_response(Reader& r);

struct CommitOffsetRequest {
    std::string groupId;
    std::string memberId;
    uint32_t generationId = 0;
    std::string topic;
    uint32_t partitionId = 0;
    uint64_t offset = 0;
};
void encode_commit_offset_request(const CommitOffsetRequest& req, std::vector<uint8_t>& buf);
CommitOffsetRequest decode_commit_offset_request(Reader& r);

struct CommitOffsetResponse {
    ErrorCode errorCode = ErrorCode::None;
};
void encode_commit_offset_response(const CommitOffsetResponse& resp, std::vector<uint8_t>& buf);
CommitOffsetResponse decode_commit_offset_response(Reader& r);

struct FetchOffsetRequest {
    std::string groupId;
    std::string topic;
    uint32_t partitionId = 0;
};
void encode_fetch_offset_request(const FetchOffsetRequest& req, std::vector<uint8_t>& buf);
FetchOffsetRequest decode_fetch_offset_request(Reader& r);

struct FetchOffsetResponse {
    ErrorCode errorCode = ErrorCode::None;
    int64_t offset = -1;  // -1 if nothing committed yet
};
void encode_fetch_offset_response(const FetchOffsetResponse& resp, std::vector<uint8_t>& buf);
FetchOffsetResponse decode_fetch_offset_response(Reader& r);

// ---- Cluster / Raft-inspired election (M4) ----
// Broker-to-broker RPCs, not client-facing. AGENT_BRIEF.md names the request
// types but not the payloads — these follow the same style as the rest of
// protocol/.

struct RequestVoteRequest {
    uint32_t term = 0;
    uint32_t candidateId = 0;
};
void encode_request_vote_request(const RequestVoteRequest& req, std::vector<uint8_t>& buf);
RequestVoteRequest decode_request_vote_request(Reader& r);

struct RequestVoteResponse {
    uint32_t term = 0;
    bool voteGranted = false;
};
void encode_request_vote_response(const RequestVoteResponse& resp, std::vector<uint8_t>& buf);
RequestVoteResponse decode_request_vote_response(Reader& r);

struct ControllerHeartbeatRequest {
    uint32_t term = 0;
    uint32_t leaderId = 0;
};
void encode_controller_heartbeat_request(const ControllerHeartbeatRequest& req, std::vector<uint8_t>& buf);
ControllerHeartbeatRequest decode_controller_heartbeat_request(Reader& r);

struct ControllerHeartbeatResponse {
    uint32_t term = 0;
    bool success = false;
};
void encode_controller_heartbeat_response(const ControllerHeartbeatResponse& resp, std::vector<uint8_t>& buf);
ControllerHeartbeatResponse decode_controller_heartbeat_response(Reader& r);

// Controller -> peer liveness ping, separate from ControllerHeartbeat: this one
// tracks which brokers are up for CREATE_TOPIC's round-robin assignment; losing
// it does not trigger a re-election (see AGENT_BRIEF.md M4 notes in the plan).
struct BrokerHeartbeatRequest {};
void encode_broker_heartbeat_request(const BrokerHeartbeatRequest& req, std::vector<uint8_t>& buf);
BrokerHeartbeatRequest decode_broker_heartbeat_request(Reader& r);

struct BrokerHeartbeatResponse {
    bool alive = true;
};
void encode_broker_heartbeat_response(const BrokerHeartbeatResponse& resp, std::vector<uint8_t>& buf);
BrokerHeartbeatResponse decode_broker_heartbeat_response(Reader& r);

struct LeaderAndISREntry {
    std::string topic;
    uint32_t partitionId = 0;
    uint32_t leaderId = 0;
    std::vector<uint32_t> replicas;
    std::vector<uint32_t> isr;
};

struct LeaderAndISRRequest {
    std::vector<LeaderAndISREntry> assignments;
};
void encode_leader_and_isr_request(const LeaderAndISRRequest& req, std::vector<uint8_t>& buf);
LeaderAndISRRequest decode_leader_and_isr_request(Reader& r);

struct LeaderAndISRResponse {
    ErrorCode errorCode = ErrorCode::None;
};
void encode_leader_and_isr_response(const LeaderAndISRResponse& resp, std::vector<uint8_t>& buf);
LeaderAndISRResponse decode_leader_and_isr_response(Reader& r);

// M5: leader -> controller, sent when a partition's ISR membership changes.
struct IsrUpdateRequest {
    std::string topic;
    uint32_t partitionId = 0;
    std::vector<uint32_t> isr;
};
void encode_isr_update_request(const IsrUpdateRequest& req, std::vector<uint8_t>& buf);
IsrUpdateRequest decode_isr_update_request(Reader& r);

struct IsrUpdateResponse {
    ErrorCode errorCode = ErrorCode::None;
};
void encode_isr_update_response(const IsrUpdateResponse& resp, std::vector<uint8_t>& buf);
IsrUpdateResponse decode_isr_update_response(Reader& r);

}  // namespace mk
