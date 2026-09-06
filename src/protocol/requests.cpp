#include "protocol/requests.h"

using namespace std;

namespace mk {

namespace {
int16_t err_i16(ErrorCode e) { return static_cast<int16_t>(e); }
ErrorCode err_from_i16(int16_t v) { return static_cast<ErrorCode>(v); }
}  // namespace

// ---- Produce ----

void encode_produce_request(const ProduceRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.topic);
    put_u32(buf, req.partitionId);
    put_u8(buf, req.acks);
    put_u32(buf, static_cast<uint32_t>(req.records.size()));
    for (const Record& rec : req.records) encode_record(rec, buf);
}

ProduceRequest decode_produce_request(Reader& r) {
    ProduceRequest req;
    req.topic = r.get_string();
    req.partitionId = r.get_u32();
    req.acks = r.get_u8();
    uint32_t count = r.get_u32();
    req.records.reserve(count);
    for (uint32_t i = 0; i < count; ++i) req.records.push_back(decode_record(r));
    return req;
}

void encode_produce_response(const ProduceResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
    put_u64(buf, resp.baseOffset);
    put_string(buf, resp.leaderHost);
    put_u32(buf, resp.leaderPort);
}

ProduceResponse decode_produce_response(Reader& r) {
    ProduceResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    resp.baseOffset = r.get_u64();
    resp.leaderHost = r.get_string();
    resp.leaderPort = r.get_u32();
    return resp;
}

// ---- Fetch ----

void encode_fetch_request(const FetchRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.topic);
    put_u32(buf, req.partitionId);
    put_u64(buf, req.fetchOffset);
    put_u32(buf, req.maxBytes);
}

FetchRequest decode_fetch_request(Reader& r) {
    FetchRequest req;
    req.topic = r.get_string();
    req.partitionId = r.get_u32();
    req.fetchOffset = r.get_u64();
    req.maxBytes = r.get_u32();
    return req;
}

void encode_fetch_response(const FetchResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
    put_u64(buf, resp.highWatermark);
    put_string(buf, resp.leaderHost);
    put_u32(buf, resp.leaderPort);
    put_u32(buf, static_cast<uint32_t>(resp.records.size()));
    for (const Record& rec : resp.records) encode_record(rec, buf);
}

FetchResponse decode_fetch_response(Reader& r) {
    FetchResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    resp.highWatermark = r.get_u64();
    resp.leaderHost = r.get_string();
    resp.leaderPort = r.get_u32();
    uint32_t count = r.get_u32();
    resp.records.reserve(count);
    for (uint32_t i = 0; i < count; ++i) resp.records.push_back(decode_record(r));
    return resp;
}

void encode_replica_fetch_request(const ReplicaFetchRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.topic);
    put_u32(buf, req.partitionId);
    put_u32(buf, req.replicaId);
    put_u64(buf, req.fetchOffset);
    put_u32(buf, req.maxBytes);
}

ReplicaFetchRequest decode_replica_fetch_request(Reader& r) {
    ReplicaFetchRequest req;
    req.topic = r.get_string();
    req.partitionId = r.get_u32();
    req.replicaId = r.get_u32();
    req.fetchOffset = r.get_u64();
    req.maxBytes = r.get_u32();
    return req;
}

// ---- Metadata ----

void encode_metadata_request(const MetadataRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.topic);
}

MetadataRequest decode_metadata_request(Reader& r) {
    MetadataRequest req;
    req.topic = r.get_string();
    return req;
}

void encode_metadata_response(const MetadataResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
    put_u32(buf, static_cast<uint32_t>(resp.partitions.size()));
    for (const PartitionMetadata& p : resp.partitions) {
        put_u32(buf, p.partitionId);
        put_u32(buf, p.leaderId);
        put_string(buf, p.leaderHost);
        put_u32(buf, p.leaderPort);
    }
}

MetadataResponse decode_metadata_response(Reader& r) {
    MetadataResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    uint32_t count = r.get_u32();
    resp.partitions.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        PartitionMetadata p;
        p.partitionId = r.get_u32();
        p.leaderId = r.get_u32();
        p.leaderHost = r.get_string();
        p.leaderPort = r.get_u32();
        resp.partitions.push_back(move(p));
    }
    return resp;
}

// ---- CreateTopic ----

void encode_create_topic_request(const CreateTopicRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.topic);
    put_u32(buf, req.numPartitions);
    put_u32(buf, req.replicationFactor);
}

CreateTopicRequest decode_create_topic_request(Reader& r) {
    CreateTopicRequest req;
    req.topic = r.get_string();
    req.numPartitions = r.get_u32();
    req.replicationFactor = r.get_u32();
    return req;
}

void encode_create_topic_response(const CreateTopicResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
    put_u32(buf, resp.controllerId);
    put_string(buf, resp.controllerHost);
    put_u32(buf, resp.controllerPort);
}

CreateTopicResponse decode_create_topic_response(Reader& r) {
    CreateTopicResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    resp.controllerId = r.get_u32();
    resp.controllerHost = r.get_string();
    resp.controllerPort = r.get_u32();
    return resp;
}

// ---- Consumer groups (M3) ----

void encode_find_coordinator_request(const FindCoordinatorRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.groupId);
}

FindCoordinatorRequest decode_find_coordinator_request(Reader& r) {
    FindCoordinatorRequest req;
    req.groupId = r.get_string();
    return req;
}

void encode_find_coordinator_response(const FindCoordinatorResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
    put_u32(buf, resp.coordinatorId);
    put_string(buf, resp.host);
    put_u32(buf, resp.port);
}

FindCoordinatorResponse decode_find_coordinator_response(Reader& r) {
    FindCoordinatorResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    resp.coordinatorId = r.get_u32();
    resp.host = r.get_string();
    resp.port = r.get_u32();
    return resp;
}

void encode_join_group_request(const JoinGroupRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.groupId);
    put_string(buf, req.topic);
    put_u32(buf, req.sessionTimeoutMs);
}

JoinGroupRequest decode_join_group_request(Reader& r) {
    JoinGroupRequest req;
    req.groupId = r.get_string();
    req.topic = r.get_string();
    req.sessionTimeoutMs = r.get_u32();
    return req;
}

void encode_join_group_response(const JoinGroupResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
    put_u32(buf, resp.generationId);
    put_string(buf, resp.memberId);
    put_string(buf, resp.leaderId);
    put_u32(buf, static_cast<uint32_t>(resp.members.size()));
    for (const string& m : resp.members) put_string(buf, m);
}

JoinGroupResponse decode_join_group_response(Reader& r) {
    JoinGroupResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    resp.generationId = r.get_u32();
    resp.memberId = r.get_string();
    resp.leaderId = r.get_string();
    uint32_t count = r.get_u32();
    resp.members.reserve(count);
    for (uint32_t i = 0; i < count; ++i) resp.members.push_back(r.get_string());
    return resp;
}

void encode_sync_group_request(const SyncGroupRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.groupId);
    put_string(buf, req.memberId);
    put_u32(buf, req.generationId);
    put_u32(buf, static_cast<uint32_t>(req.assignments.size()));
    for (const SyncGroupAssignment& a : req.assignments) {
        put_string(buf, a.memberId);
        put_u32(buf, static_cast<uint32_t>(a.partitions.size()));
        for (uint32_t p : a.partitions) put_u32(buf, p);
    }
}

SyncGroupRequest decode_sync_group_request(Reader& r) {
    SyncGroupRequest req;
    req.groupId = r.get_string();
    req.memberId = r.get_string();
    req.generationId = r.get_u32();
    uint32_t count = r.get_u32();
    req.assignments.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        SyncGroupAssignment a;
        a.memberId = r.get_string();
        uint32_t pcount = r.get_u32();
        a.partitions.reserve(pcount);
        for (uint32_t j = 0; j < pcount; ++j) a.partitions.push_back(r.get_u32());
        req.assignments.push_back(move(a));
    }
    return req;
}

void encode_sync_group_response(const SyncGroupResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
    put_u32(buf, static_cast<uint32_t>(resp.partitions.size()));
    for (uint32_t p : resp.partitions) put_u32(buf, p);
}

SyncGroupResponse decode_sync_group_response(Reader& r) {
    SyncGroupResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    uint32_t count = r.get_u32();
    resp.partitions.reserve(count);
    for (uint32_t i = 0; i < count; ++i) resp.partitions.push_back(r.get_u32());
    return resp;
}

void encode_heartbeat_request(const HeartbeatRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.groupId);
    put_string(buf, req.memberId);
    put_u32(buf, req.generationId);
}

HeartbeatRequest decode_heartbeat_request(Reader& r) {
    HeartbeatRequest req;
    req.groupId = r.get_string();
    req.memberId = r.get_string();
    req.generationId = r.get_u32();
    return req;
}

void encode_heartbeat_response(const HeartbeatResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
}

HeartbeatResponse decode_heartbeat_response(Reader& r) {
    HeartbeatResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    return resp;
}

void encode_leave_group_request(const LeaveGroupRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.groupId);
    put_string(buf, req.memberId);
}

LeaveGroupRequest decode_leave_group_request(Reader& r) {
    LeaveGroupRequest req;
    req.groupId = r.get_string();
    req.memberId = r.get_string();
    return req;
}

void encode_leave_group_response(const LeaveGroupResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
}

LeaveGroupResponse decode_leave_group_response(Reader& r) {
    LeaveGroupResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    return resp;
}

void encode_commit_offset_request(const CommitOffsetRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.groupId);
    put_string(buf, req.memberId);
    put_u32(buf, req.generationId);
    put_string(buf, req.topic);
    put_u32(buf, req.partitionId);
    put_u64(buf, req.offset);
}

CommitOffsetRequest decode_commit_offset_request(Reader& r) {
    CommitOffsetRequest req;
    req.groupId = r.get_string();
    req.memberId = r.get_string();
    req.generationId = r.get_u32();
    req.topic = r.get_string();
    req.partitionId = r.get_u32();
    req.offset = r.get_u64();
    return req;
}

void encode_commit_offset_response(const CommitOffsetResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
}

CommitOffsetResponse decode_commit_offset_response(Reader& r) {
    CommitOffsetResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    return resp;
}

void encode_fetch_offset_request(const FetchOffsetRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.groupId);
    put_string(buf, req.topic);
    put_u32(buf, req.partitionId);
}

FetchOffsetRequest decode_fetch_offset_request(Reader& r) {
    FetchOffsetRequest req;
    req.groupId = r.get_string();
    req.topic = r.get_string();
    req.partitionId = r.get_u32();
    return req;
}

void encode_fetch_offset_response(const FetchOffsetResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
    put_i64(buf, resp.offset);
}

FetchOffsetResponse decode_fetch_offset_response(Reader& r) {
    FetchOffsetResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    resp.offset = r.get_i64();
    return resp;
}

// ---- Cluster / Raft-inspired election (M4) ----

void encode_request_vote_request(const RequestVoteRequest& req, vector<uint8_t>& buf) {
    put_u32(buf, req.term);
    put_u32(buf, req.candidateId);
}

RequestVoteRequest decode_request_vote_request(Reader& r) {
    RequestVoteRequest req;
    req.term = r.get_u32();
    req.candidateId = r.get_u32();
    return req;
}

void encode_request_vote_response(const RequestVoteResponse& resp, vector<uint8_t>& buf) {
    put_u32(buf, resp.term);
    put_u8(buf, resp.voteGranted ? 1 : 0);
}

RequestVoteResponse decode_request_vote_response(Reader& r) {
    RequestVoteResponse resp;
    resp.term = r.get_u32();
    resp.voteGranted = r.get_u8() != 0;
    return resp;
}

void encode_controller_heartbeat_request(const ControllerHeartbeatRequest& req, vector<uint8_t>& buf) {
    put_u32(buf, req.term);
    put_u32(buf, req.leaderId);
}

ControllerHeartbeatRequest decode_controller_heartbeat_request(Reader& r) {
    ControllerHeartbeatRequest req;
    req.term = r.get_u32();
    req.leaderId = r.get_u32();
    return req;
}

void encode_controller_heartbeat_response(const ControllerHeartbeatResponse& resp, vector<uint8_t>& buf) {
    put_u32(buf, resp.term);
    put_u8(buf, resp.success ? 1 : 0);
}

ControllerHeartbeatResponse decode_controller_heartbeat_response(Reader& r) {
    ControllerHeartbeatResponse resp;
    resp.term = r.get_u32();
    resp.success = r.get_u8() != 0;
    return resp;
}

void encode_broker_heartbeat_request(const BrokerHeartbeatRequest& req, vector<uint8_t>& buf) {
    (void)req;
    (void)buf;
}

BrokerHeartbeatRequest decode_broker_heartbeat_request(Reader& r) {
    (void)r;
    return BrokerHeartbeatRequest{};
}

void encode_broker_heartbeat_response(const BrokerHeartbeatResponse& resp, vector<uint8_t>& buf) {
    put_u8(buf, resp.alive ? 1 : 0);
}

BrokerHeartbeatResponse decode_broker_heartbeat_response(Reader& r) {
    BrokerHeartbeatResponse resp;
    resp.alive = r.get_u8() != 0;
    return resp;
}

void encode_leader_and_isr_request(const LeaderAndISRRequest& req, vector<uint8_t>& buf) {
    put_u32(buf, static_cast<uint32_t>(req.assignments.size()));
    for (const LeaderAndISREntry& e : req.assignments) {
        put_string(buf, e.topic);
        put_u32(buf, e.partitionId);
        put_u32(buf, e.leaderId);
        put_u32(buf, static_cast<uint32_t>(e.replicas.size()));
        for (uint32_t id : e.replicas) put_u32(buf, id);
        put_u32(buf, static_cast<uint32_t>(e.isr.size()));
        for (uint32_t id : e.isr) put_u32(buf, id);
    }
}

LeaderAndISRRequest decode_leader_and_isr_request(Reader& r) {
    LeaderAndISRRequest req;
    uint32_t count = r.get_u32();
    req.assignments.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        LeaderAndISREntry e;
        e.topic = r.get_string();
        e.partitionId = r.get_u32();
        e.leaderId = r.get_u32();
        uint32_t replicaCount = r.get_u32();
        e.replicas.reserve(replicaCount);
        for (uint32_t j = 0; j < replicaCount; ++j) e.replicas.push_back(r.get_u32());
        uint32_t isrCount = r.get_u32();
        e.isr.reserve(isrCount);
        for (uint32_t j = 0; j < isrCount; ++j) e.isr.push_back(r.get_u32());
        req.assignments.push_back(move(e));
    }
    return req;
}

void encode_leader_and_isr_response(const LeaderAndISRResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
}

LeaderAndISRResponse decode_leader_and_isr_response(Reader& r) {
    LeaderAndISRResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    return resp;
}

void encode_isr_update_request(const IsrUpdateRequest& req, vector<uint8_t>& buf) {
    put_string(buf, req.topic);
    put_u32(buf, req.partitionId);
    put_u32(buf, static_cast<uint32_t>(req.isr.size()));
    for (uint32_t id : req.isr) put_u32(buf, id);
}

IsrUpdateRequest decode_isr_update_request(Reader& r) {
    IsrUpdateRequest req;
    req.topic = r.get_string();
    req.partitionId = r.get_u32();
    uint32_t count = r.get_u32();
    req.isr.reserve(count);
    for (uint32_t i = 0; i < count; ++i) req.isr.push_back(r.get_u32());
    return req;
}

void encode_isr_update_response(const IsrUpdateResponse& resp, vector<uint8_t>& buf) {
    put_i16(buf, err_i16(resp.errorCode));
}

IsrUpdateResponse decode_isr_update_response(Reader& r) {
    IsrUpdateResponse resp;
    resp.errorCode = err_from_i16(r.get_i16());
    return resp;
}

}  // namespace mk
