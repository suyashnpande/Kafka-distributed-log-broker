#pragma once

#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "protocol/requests.h"

// Single-broker group coordinator (M3). No background reaper thread: member
// staleness is checked opportunistically inside heartbeat() itself, which is
// enough at demo scale since heartbeats arrive every sessionTimeoutMs/3.
// FindCoordinator always answers "this broker" — the indirection only starts
// to matter once M4 adds a cluster.

namespace mk {

struct MemberState {
    uint32_t sessionTimeoutMs = 0;
    uint64_t lastHeartbeatMs = 0;
};

struct GroupState {
    uint32_t generationId = 0;
    std::string topic;
    std::string leaderId;
    std::map<std::string, MemberState> members;  // sorted by memberId

    bool rebalanceInProgress = false;  // a join window is currently open
    bool needsRebalance = false;       // set on eviction/leave; forces survivors to rejoin
    uint64_t joinDeadlineMs = 0;

    bool syncReady = false;  // leader's SyncGroup has posted the assignment for this generation
    std::map<std::string, std::vector<uint32_t>> assignment;

    bool offsetsLoaded = false;
    std::map<std::pair<std::string, uint32_t>, uint64_t> offsets;  // (topic,partition) -> offset

    std::mutex mu;
    std::condition_variable cv;
};

class GroupCoordinator {
public:
    GroupCoordinator(std::string dataDir, uint32_t brokerId, std::string host, uint16_t port);

    FindCoordinatorResponse find_coordinator(const FindCoordinatorRequest& req);
    JoinGroupResponse join_group(const JoinGroupRequest& req);
    SyncGroupResponse sync_group(const SyncGroupRequest& req);
    HeartbeatResponse heartbeat(const HeartbeatRequest& req);
    LeaveGroupResponse leave_group(const LeaveGroupRequest& req);
    CommitOffsetResponse commit_offset(const CommitOffsetRequest& req);
    FetchOffsetResponse fetch_offset(const FetchOffsetRequest& req);

private:
    GroupState& get_or_create_group(const std::string& groupId);
    std::string generate_member_id(const std::string& groupId);

    // Caller must hold g.mu.
    void load_offsets_if_needed(GroupState& g, const std::string& groupId);
    void persist_offsets(GroupState& g, const std::string& groupId);

    std::string dataDir_;
    uint32_t brokerId_;
    std::string host_;
    uint16_t port_;

    std::mutex groupsMu_;
    std::unordered_map<std::string, std::unique_ptr<GroupState>> groups_;
    uint64_t nextMemberSeq_ = 0;  // guarded by groupsMu_
};

}  // namespace mk
