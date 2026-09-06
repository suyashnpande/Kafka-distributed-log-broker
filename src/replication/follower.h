#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

#include "net/socket.h"
#include "storage/partition.h"

// Follower-side puller (M5). One instance per (topic, partitionId) this
// broker replicates but doesn't lead. Owns its own connection to the
// partition's current leader (reconnect-on-failure, same spirit as
// ClusterMembership::call) and continuously pulls via FETCH_REPLICA,
// appending directly into the local Partition via append_replicated.

namespace mk {

class FollowerFetcher {
public:
    FollowerFetcher(std::string topic, uint32_t partitionId, uint32_t selfBrokerId,
                     std::string leaderHost, uint16_t leaderPort, Partition& partition);
    ~FollowerFetcher();

    FollowerFetcher(const FollowerFetcher&) = delete;
    FollowerFetcher& operator=(const FollowerFetcher&) = delete;

    const std::string& leader_host() const { return leaderHost_; }
    uint16_t leader_port() const { return leaderPort_; }

private:
    void run_loop();

    std::string topic_;
    uint32_t partitionId_;
    uint32_t selfBrokerId_;
    std::string leaderHost_;
    uint16_t leaderPort_;
    Partition& partition_;

    std::atomic<bool> stop_{false};
    std::thread thread_;
};

}  // namespace mk
