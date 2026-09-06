#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "cluster/controller.h"
#include "cluster/membership.h"
#include "group/coordinator.h"
#include "net/socket.h"
#include "replication/follower.h"
#include "replication/isr_manager.h"
#include "storage/partition.h"

namespace mk {

// Standalone mode (no peers, M1-M3): owns every partition it's asked about
// (get-or-create on first use) and is always "leader" of every partition for
// any topic. Cluster mode (M4, peers given): defers to cluster_'s assignment
// map instead — see handle_produce/handle_fetch/handle_metadata/
// handle_create_topic/handle_find_coordinator.
class Broker {
public:
    // peers, when given, engages cluster mode: Raft-inspired election runs
    // immediately (checkpoint A). Must include an entry for id. When absent,
    // this broker behaves exactly as it did in M1-M3 (standalone).
    // uncleanLeaderElectionEnable governs M6 partition failover: whether the
    // controller (if this broker becomes it) may promote a non-ISR replica
    // when no ISR member of a leaderless partition is alive.
    Broker(uint32_t id, std::string host, uint16_t port, std::string dataDir,
           std::optional<std::map<uint32_t, Address>> peers = std::nullopt,
           bool uncleanLeaderElectionEnable = false);

    ResponseFrame handle(const RequestFrame& req);

private:
    struct Key {
        std::string topic;
        uint32_t partitionId;
        bool operator==(const Key& o) const {
            return partitionId == o.partitionId && topic == o.topic;
        }
    };
    struct KeyHash {
        size_t operator()(const Key& k) const {
            return std::hash<std::string>()(k.topic) ^ (std::hash<uint32_t>()(k.partitionId) << 1);
        }
    };

    Partition& get_or_create_partition(const std::string& topic, uint32_t partitionId);

    // Returns the known partition count for a topic, loading it from
    // <dataDir>/<topic>/.partitions on first touch if not already cached, or
    // defaulting to 1 (M2's zero-config auto-create behavior) if that file
    // doesn't exist either. Caller must hold mu_.
    uint32_t partition_count_for(const std::string& topic);

    ResponseFrame handle_produce(uint32_t correlationId, const std::vector<uint8_t>& payload);
    ResponseFrame handle_fetch(uint32_t correlationId, const std::vector<uint8_t>& payload);
    ResponseFrame handle_metadata(uint32_t correlationId, const std::vector<uint8_t>& payload);
    ResponseFrame handle_create_topic(uint32_t correlationId, const std::vector<uint8_t>& payload);

    ResponseFrame handle_find_coordinator(uint32_t correlationId, const std::vector<uint8_t>& payload);
    ResponseFrame handle_join_group(uint32_t correlationId, const std::vector<uint8_t>& payload);
    ResponseFrame handle_sync_group(uint32_t correlationId, const std::vector<uint8_t>& payload);
    ResponseFrame handle_heartbeat(uint32_t correlationId, const std::vector<uint8_t>& payload);
    ResponseFrame handle_leave_group(uint32_t correlationId, const std::vector<uint8_t>& payload);
    ResponseFrame handle_commit_offset(uint32_t correlationId, const std::vector<uint8_t>& payload);
    ResponseFrame handle_fetch_offset(uint32_t correlationId, const std::vector<uint8_t>& payload);

    ResponseFrame handle_request_vote(uint32_t correlationId, const std::vector<uint8_t>& payload);
    ResponseFrame handle_controller_heartbeat(uint32_t correlationId, const std::vector<uint8_t>& payload);
    ResponseFrame handle_broker_heartbeat(uint32_t correlationId, const std::vector<uint8_t>& payload);
    ResponseFrame handle_leader_and_isr(uint32_t correlationId, const std::vector<uint8_t>& payload);

    // M5: serves FETCH_REPLICA (a follower broker catching up).
    ResponseFrame handle_replicate(uint32_t correlationId, const std::vector<uint8_t>& payload);

    // M5 checkpoint B: a partition leader reporting its ISR changed to us
    // (only meaningful when this broker is the controller).
    ResponseFrame handle_isr_update(uint32_t correlationId, const std::vector<uint8_t>& payload);

    // M5: reconciles this broker's replication state (which partitions it
    // leads vs. follows) against cluster_'s current assignment. Called after
    // every apply_leader_and_isr and every successful create_topic. No-op in
    // standalone mode.
    void reconcile_replication();

    uint32_t id_;
    std::string host_;
    uint16_t port_;
    std::string dataDir_;
    std::mutex mu_;
    std::unordered_map<Key, std::unique_ptr<Partition>, KeyHash> partitions_;
    std::unordered_map<std::string, uint32_t> topics_;  // topic -> partition count
    GroupCoordinator groupCoordinator_;

    // Cluster mode (M4 checkpoint A) — both null in standalone mode. Declared
    // in this order (membership_ before cluster_) so membership_ outlives
    // cluster_: cluster_'s background thread uses membership_ and must be
    // fully stopped (in ~ClusterController) before membership_ is destroyed.
    std::unique_ptr<ClusterMembership> membership_;
    std::unique_ptr<ClusterController> cluster_;

    // M5 (cluster mode only): leader-side replica tracking, and one puller
    // per partition this broker follows but doesn't lead. followers_ has its
    // own mutex (not mu_) because reconcile_replication needs to call
    // get_or_create_partition — which locks mu_ itself — while iterating it;
    // reusing mu_ here would self-deadlock.
    std::unique_ptr<IsrManager> isrManager_;
    std::mutex followersMu_;
    std::unordered_map<Key, std::unique_ptr<FollowerFetcher>, KeyHash> followers_;
    // M6: which partitions we believed we led as of the last reconcile pass —
    // lets reconcile_replication tell "just promoted" (jump HWM to LEO) apart
    // from "was already leading" (must NOT touch HWM, or a healthy leader's
    // real ISR-computed HWM would be clobbered back up to LEO on every call).
    std::unordered_set<Key, KeyHash> leadingPartitions_;
};

}  // namespace mk
