#include "broker/broker.h"

#include <algorithm>
#include <filesystem>
#include <fstream>

#include "common/murmur2.h"
#include "protocol/requests.h"
#include "protocol/types.h"

using namespace std;
namespace fs = std::filesystem;

namespace mk {

namespace {
constexpr uint32_t kAcksAllTimeoutMs = 5000;
}  // namespace

Broker::Broker(uint32_t id, string host, uint16_t port, string dataDir,
               optional<map<uint32_t, Address>> peers, bool uncleanLeaderElectionEnable)
    : id_(id),
      host_(move(host)),
      port_(port),
      dataDir_(move(dataDir)),
      groupCoordinator_(dataDir_, id_, host_, port_) {
    if (peers.has_value()) {
        membership_ = make_unique<ClusterMembership>(id_, move(*peers));
        cluster_ = make_unique<ClusterController>(dataDir_, *membership_, /*startBackgroundThread=*/true,
                                                    uncleanLeaderElectionEnable);
        isrManager_ = make_unique<IsrManager>(id_);

        // M6: the controller's own broker won't hear its own
        // broadcast_leader_and_isr (it only sends to peers), so when it
        // promotes a partition via check_partition_failover it needs an
        // explicit nudge to reconcile its own replication state.
        cluster_->set_on_assignment_change([this] { reconcile_replication(); });

        // M5 checkpoint B: report ISR changes to the controller. If this
        // broker *is* the controller, that's a direct local call; otherwise
        // it's a best-effort network hop — mirrors the same
        // local-vs-remote split used for redirects elsewhere.
        isrManager_->set_on_isr_change(
            [this](const string& topic, uint32_t partitionId, const vector<uint32_t>& isr) {
                if (!cluster_) return;
                if (cluster_->is_leader()) {
                    cluster_->update_isr(topic, partitionId, isr);
                    return;
                }
                int64_t controllerId = cluster_->current_leader_id();
                if (controllerId < 0) return;
                auto addr = membership_->address_of(static_cast<uint32_t>(controllerId));
                if (!addr) return;

                IsrUpdateRequest req{topic, partitionId, isr};
                vector<uint8_t> payload;
                encode_isr_update_request(req, payload);
                RequestFrame frame{static_cast<uint16_t>(RequestType::IsrUpdate), 1, payload};
                membership_->call(static_cast<uint32_t>(controllerId), frame);  // best-effort
            });
    }
}

Partition& Broker::get_or_create_partition(const string& topic, uint32_t partitionId) {
    lock_guard<mutex> lock(mu_);
    Key key{topic, partitionId};
    auto it = partitions_.find(key);
    if (it != partitions_.end()) return *it->second;

    string dir = dataDir_ + "/" + topic + "/" + to_string(partitionId);
    auto part = make_unique<Partition>(dir);
    part->recover();
    Partition& ref = *part;
    partitions_.emplace(move(key), move(part));
    return ref;
}

uint32_t Broker::partition_count_for(const string& topic) {
    lock_guard<mutex> lock(mu_);
    auto it = topics_.find(topic);
    if (it != topics_.end()) return it->second;

    string path = dataDir_ + "/" + topic + "/.partitions";
    uint32_t count = 1;  // M2 zero-config default: nobody explicitly created this topic
    ifstream in(path);
    if (in) {
        in >> count;
        if (count == 0) count = 1;
    }
    topics_[topic] = count;
    return count;
}

ResponseFrame Broker::handle(const RequestFrame& req) {
    switch (static_cast<RequestType>(req.requestType)) {
        case RequestType::Produce:
            return handle_produce(req.correlationId, req.payload);
        case RequestType::Fetch:
            return handle_fetch(req.correlationId, req.payload);
        case RequestType::Metadata:
            return handle_metadata(req.correlationId, req.payload);
        case RequestType::CreateTopic:
            return handle_create_topic(req.correlationId, req.payload);
        case RequestType::FindCoordinator:
            return handle_find_coordinator(req.correlationId, req.payload);
        case RequestType::JoinGroup:
            return handle_join_group(req.correlationId, req.payload);
        case RequestType::SyncGroup:
            return handle_sync_group(req.correlationId, req.payload);
        case RequestType::Heartbeat:
            return handle_heartbeat(req.correlationId, req.payload);
        case RequestType::LeaveGroup:
            return handle_leave_group(req.correlationId, req.payload);
        case RequestType::CommitOffset:
            return handle_commit_offset(req.correlationId, req.payload);
        case RequestType::FetchOffset:
            return handle_fetch_offset(req.correlationId, req.payload);
        case RequestType::RequestVote:
            return handle_request_vote(req.correlationId, req.payload);
        case RequestType::ControllerHeartbeat:
            return handle_controller_heartbeat(req.correlationId, req.payload);
        case RequestType::BrokerHeartbeat:
            return handle_broker_heartbeat(req.correlationId, req.payload);
        case RequestType::LeaderAndISR:
            return handle_leader_and_isr(req.correlationId, req.payload);
        case RequestType::Replicate:
            return handle_replicate(req.correlationId, req.payload);
        case RequestType::IsrUpdate:
            return handle_isr_update(req.correlationId, req.payload);
        default: {
            vector<uint8_t> payload;
            put_i16(payload, static_cast<int16_t>(ErrorCode::UnknownRequestType));
            return ResponseFrame{req.correlationId, payload};
        }
    }
}

ResponseFrame Broker::handle_produce(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    ProduceRequest req = decode_produce_request(r);

    if (cluster_) {
        auto assignment = cluster_->assignment_for(req.topic, req.partitionId);
        if (!assignment) {
            ProduceResponse resp{ErrorCode::UnknownTopic, 0, "", 0};
            vector<uint8_t> out;
            encode_produce_response(resp, out);
            return ResponseFrame{correlationId, out};
        }
        if (assignment->leaderId != id_) {
            auto addr = membership_->address_of(assignment->leaderId);
            ProduceResponse resp{ErrorCode::NotLeaderForPartition, 0,
                                  addr ? addr->host : "", addr ? static_cast<uint32_t>(addr->port) : 0};
            vector<uint8_t> out;
            encode_produce_response(resp, out);
            return ResponseFrame{correlationId, out};
        }
        // We are the assigned leader for this partition — fall through as usual.
    }

    Partition& partition = get_or_create_partition(req.topic, req.partitionId);
    int64_t recordCount = static_cast<int64_t>(req.records.size());
    int64_t base = partition.append(move(req.records));
    if (cluster_ && isrManager_) {
        isrManager_->on_leader_append(req.topic, req.partitionId);
    }

    // acks=all: block this connection's own thread until the ISR has caught
    // up past our batch's last offset (same "thread-per-connection can just
    // block" pattern already used for M3's rebalance window and M4's
    // election). No replicas configured (standalone mode, or cluster mode
    // with replication factor 1) means nothing to wait for.
    if (req.acks == 0xFF && cluster_ && isrManager_ && recordCount > 0) {
        int64_t requiredOffset = base + recordCount - 1;
        bool ok = isrManager_->wait_for_hwm(req.topic, req.partitionId, requiredOffset,
                                             kAcksAllTimeoutMs);
        if (!ok) {
            ProduceResponse resp{ErrorCode::RequestTimedOut, static_cast<uint64_t>(base), "", 0};
            vector<uint8_t> out;
            encode_produce_response(resp, out);
            return ResponseFrame{correlationId, out};
        }
    }

    ProduceResponse resp{ErrorCode::None, static_cast<uint64_t>(base), "", 0};
    vector<uint8_t> out;
    encode_produce_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_fetch(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    FetchRequest req = decode_fetch_request(r);

    if (cluster_) {
        auto assignment = cluster_->assignment_for(req.topic, req.partitionId);
        if (!assignment) {
            FetchResponse resp{ErrorCode::UnknownTopic, 0, {}, "", 0};
            vector<uint8_t> out;
            encode_fetch_response(resp, out);
            return ResponseFrame{correlationId, out};
        }
        if (assignment->leaderId != id_) {
            auto addr = membership_->address_of(assignment->leaderId);
            FetchResponse resp{ErrorCode::NotLeaderForPartition, 0, {},
                                addr ? addr->host : "", addr ? static_cast<uint32_t>(addr->port) : 0};
            vector<uint8_t> out;
            encode_fetch_response(resp, out);
            return ResponseFrame{correlationId, out};
        }
    }

    Partition& partition = get_or_create_partition(req.topic, req.partitionId);
    vector<Record> records =
        partition.read(static_cast<int64_t>(req.fetchOffset), req.maxBytes);

    FetchResponse resp{ErrorCode::None, static_cast<uint64_t>(partition.high_watermark()),
                        move(records), "", 0};
    vector<uint8_t> out;
    encode_fetch_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_metadata(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    MetadataRequest req = decode_metadata_request(r);

    MetadataResponse resp;
    resp.errorCode = ErrorCode::None;

    if (cluster_) {
        auto assignments = cluster_->assignments_for_topic(req.topic);
        if (assignments.empty()) {
            resp.errorCode = ErrorCode::UnknownTopic;  // cluster mode: no lazy auto-create
        } else {
            for (const auto& [p, pa] : assignments) {
                auto addr = membership_->address_of(pa.leaderId);
                resp.partitions.push_back(PartitionMetadata{
                    p, pa.leaderId, addr ? addr->host : "",
                    addr ? static_cast<uint32_t>(addr->port) : 0});
            }
        }
    } else {
        uint32_t partitionCount = partition_count_for(req.topic);
        for (uint32_t p = 0; p < partitionCount; ++p) {
            resp.partitions.push_back(PartitionMetadata{p, id_, host_, port_});
        }
    }

    vector<uint8_t> out;
    encode_metadata_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_create_topic(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    CreateTopicRequest req = decode_create_topic_request(r);

    if (cluster_) {
        auto result = cluster_->create_topic(req.topic, req.numPartitions, req.replicationFactor);
        if (!result) {
            // Not the controller — redirect the client there.
            int64_t leaderId = cluster_->current_leader_id();
            CreateTopicResponse resp;
            resp.errorCode = ErrorCode::NotLeaderForPartition;
            if (leaderId >= 0) {
                resp.controllerId = static_cast<uint32_t>(leaderId);
                auto addr = membership_->address_of(resp.controllerId);
                if (addr) {
                    resp.controllerHost = addr->host;
                    resp.controllerPort = addr->port;
                }
            }
            vector<uint8_t> out;
            encode_create_topic_response(resp, out);
            return ResponseFrame{correlationId, out};
        }
        reconcile_replication();
        CreateTopicResponse resp{ErrorCode::None, 0, "", 0};
        vector<uint8_t> out;
        encode_create_topic_response(resp, out);
        return ResponseFrame{correlationId, out};
    }

    lock_guard<mutex> lock(mu_);
    string dir = dataDir_ + "/" + req.topic;
    string path = dir + "/.partitions";

    // The on-disk file (not the topics_ cache) is the source of truth for
    // "already explicitly created" — a topic that was only ever lazily
    // touched by Produce/Fetch/Metadata (defaulted to 1 partition, no file
    // written) must not block a later real CreateTopic from setting the
    // requested count.
    if (!fs::exists(path)) {
        fs::create_directories(dir);
        ofstream out(path);
        out << req.numPartitions;
        topics_[req.topic] = req.numPartitions;
    }

    CreateTopicResponse resp{ErrorCode::None, 0, "", 0};
    vector<uint8_t> out;
    encode_create_topic_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_find_coordinator(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    FindCoordinatorRequest req = decode_find_coordinator_request(r);

    if (cluster_) {
        vector<uint32_t> ids = membership_->all_ids();  // ascending — every broker agrees on this order
        uint32_t h = static_cast<uint32_t>(
            murmur2(reinterpret_cast<const uint8_t*>(req.groupId.data()), req.groupId.size()) &
            0x7fffffff);
        uint32_t coordId = ids[h % ids.size()];
        auto addr = membership_->address_of(coordId);
        FindCoordinatorResponse resp{ErrorCode::None, coordId, addr ? addr->host : "",
                                      addr ? static_cast<uint32_t>(addr->port) : 0};
        vector<uint8_t> out;
        encode_find_coordinator_response(resp, out);
        return ResponseFrame{correlationId, out};
    }

    FindCoordinatorResponse resp = groupCoordinator_.find_coordinator(req);
    vector<uint8_t> out;
    encode_find_coordinator_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_join_group(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    JoinGroupRequest req = decode_join_group_request(r);
    JoinGroupResponse resp = groupCoordinator_.join_group(req);
    vector<uint8_t> out;
    encode_join_group_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_sync_group(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    SyncGroupRequest req = decode_sync_group_request(r);
    SyncGroupResponse resp = groupCoordinator_.sync_group(req);
    vector<uint8_t> out;
    encode_sync_group_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_heartbeat(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    HeartbeatRequest req = decode_heartbeat_request(r);
    HeartbeatResponse resp = groupCoordinator_.heartbeat(req);
    vector<uint8_t> out;
    encode_heartbeat_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_leave_group(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    LeaveGroupRequest req = decode_leave_group_request(r);
    LeaveGroupResponse resp = groupCoordinator_.leave_group(req);
    vector<uint8_t> out;
    encode_leave_group_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_commit_offset(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    CommitOffsetRequest req = decode_commit_offset_request(r);
    CommitOffsetResponse resp = groupCoordinator_.commit_offset(req);
    vector<uint8_t> out;
    encode_commit_offset_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_fetch_offset(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    FetchOffsetRequest req = decode_fetch_offset_request(r);
    FetchOffsetResponse resp = groupCoordinator_.fetch_offset(req);
    vector<uint8_t> out;
    encode_fetch_offset_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_request_vote(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    RequestVoteRequest req = decode_request_vote_request(r);
    RequestVoteResponse resp = cluster_
                                    ? cluster_->handle_request_vote(req)
                                    : RequestVoteResponse{req.term, false};
    vector<uint8_t> out;
    encode_request_vote_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_controller_heartbeat(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    ControllerHeartbeatRequest req = decode_controller_heartbeat_request(r);
    ControllerHeartbeatResponse resp = cluster_
                                            ? cluster_->handle_controller_heartbeat(req)
                                            : ControllerHeartbeatResponse{req.term, false};
    vector<uint8_t> out;
    encode_controller_heartbeat_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_broker_heartbeat(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    BrokerHeartbeatRequest req = decode_broker_heartbeat_request(r);
    BrokerHeartbeatResponse resp =
        cluster_ ? cluster_->handle_broker_heartbeat(req) : BrokerHeartbeatResponse{false};
    vector<uint8_t> out;
    encode_broker_heartbeat_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_leader_and_isr(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    LeaderAndISRRequest req = decode_leader_and_isr_request(r);
    LeaderAndISRResponse resp{ErrorCode::None};
    if (cluster_) {
        cluster_->apply_leader_and_isr(req.assignments);
        reconcile_replication();
    } else {
        resp.errorCode = ErrorCode::UnknownRequestType;
    }
    vector<uint8_t> out;
    encode_leader_and_isr_response(resp, out);
    return ResponseFrame{correlationId, out};
}

ResponseFrame Broker::handle_replicate(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    ReplicaFetchRequest req = decode_replica_fetch_request(r);

    if (cluster_) {
        auto assignment = cluster_->assignment_for(req.topic, req.partitionId);
        if (!assignment) {
            FetchResponse resp{ErrorCode::UnknownTopic, 0, {}, "", 0};
            vector<uint8_t> out;
            encode_fetch_response(resp, out);
            return ResponseFrame{correlationId, out};
        }
        if (assignment->leaderId != id_) {
            auto addr = membership_->address_of(assignment->leaderId);
            FetchResponse resp{ErrorCode::NotLeaderForPartition, 0, {},
                                addr ? addr->host : "", addr ? static_cast<uint32_t>(addr->port) : 0};
            vector<uint8_t> out;
            encode_fetch_response(resp, out);
            return ResponseFrame{correlationId, out};
        }
    }

    Partition& partition = get_or_create_partition(req.topic, req.partitionId);
    vector<Record> records = partition.read_up_to_leo(static_cast<int64_t>(req.fetchOffset), req.maxBytes);

    if (cluster_ && isrManager_) {
        isrManager_->on_replica_fetch(req.topic, req.partitionId, req.replicaId, req.fetchOffset);
    }

    FetchResponse resp{ErrorCode::None, static_cast<uint64_t>(partition.high_watermark()),
                        move(records), "", 0};
    vector<uint8_t> out;
    encode_fetch_response(resp, out);
    return ResponseFrame{correlationId, out};
}

void Broker::reconcile_replication() {
    if (!cluster_ || !isrManager_) return;

    auto assignments = cluster_->all_assignments();
    lock_guard<mutex> lock(followersMu_);

    // Track which (topic,partitionId) are still relevant so anything not
    // touched below (this broker no longer involved at all) can be torn down.
    unordered_map<Key, bool, KeyHash> stillRelevant;
    unordered_set<Key, KeyHash> newLeading;

    for (const auto& [key, pa] : assignments) {
        Key k{key.first, key.second};
        bool isLeader = (pa.leaderId == id_);
        bool isReplica = !isLeader &&
                          find(pa.replicas.begin(), pa.replicas.end(), id_) != pa.replicas.end();

        if (isLeader) {
            stillRelevant[k] = true;
            followers_.erase(k);  // can't be both leader and follower for the same partition
            Partition& partition = get_or_create_partition(k.topic, k.partitionId);
            if (!leadingPartitions_.count(k)) {
                // M6: just promoted (was following, or never seen before) —
                // a follower's HWM is never consumer-visible while following
                // (M5's append_replicated deliberately never touches it), so
                // without this a freshly-promoted leader looks empty to
                // consumers despite having the data. Followers only ever
                // pull (never diverge ahead of a leader), so there's nothing
                // to truncate here — jumping HWM to our own LEO is complete,
                // not a partial substitute for it.
                partition.set_high_watermark(partition.log_end_offset());
            }
            newLeading.insert(k);
            isrManager_->set_leading(k.topic, k.partitionId, partition, pa.replicas);
        } else if (isReplica) {
            stillRelevant[k] = true;
            isrManager_->stop_leading(k.topic, k.partitionId);

            auto addr = membership_->address_of(pa.leaderId);
            if (!addr) continue;
            auto it = followers_.find(k);
            bool needsRecreate = it == followers_.end() || it->second->leader_host() != addr->host ||
                                  it->second->leader_port() != addr->port;
            if (needsRecreate) {
                Partition& partition = get_or_create_partition(k.topic, k.partitionId);
                followers_[k] = make_unique<FollowerFetcher>(k.topic, k.partitionId, id_, addr->host,
                                                               addr->port, partition);
            }
        }
    }

    for (auto it = followers_.begin(); it != followers_.end();) {
        if (!stillRelevant.count(it->first)) {
            it = followers_.erase(it);
        } else {
            ++it;
        }
    }
    leadingPartitions_ = move(newLeading);
}

ResponseFrame Broker::handle_isr_update(uint32_t correlationId, const vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    IsrUpdateRequest req = decode_isr_update_request(r);
    IsrUpdateResponse resp{ErrorCode::None};
    if (cluster_) {
        cluster_->update_isr(req.topic, req.partitionId, req.isr);
    } else {
        resp.errorCode = ErrorCode::UnknownRequestType;
    }
    vector<uint8_t> out;
    encode_isr_update_response(resp, out);
    return ResponseFrame{correlationId, out};
}

}  // namespace mk
