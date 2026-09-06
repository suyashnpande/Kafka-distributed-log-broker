#include "group/coordinator.h"

#include <chrono>
#include <filesystem>
#include <fstream>

using namespace std;
namespace fs = std::filesystem;

namespace mk {

namespace {

constexpr uint32_t kRebalanceWindowMs = 1000;
constexpr uint32_t kSyncTimeoutMs = 5000;

uint64_t now_ms() {
    return chrono::duration_cast<chrono::milliseconds>(
               chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

GroupCoordinator::GroupCoordinator(string dataDir, uint32_t brokerId, string host, uint16_t port)
    : dataDir_(move(dataDir)), brokerId_(brokerId), host_(move(host)), port_(port) {}

GroupState& GroupCoordinator::get_or_create_group(const string& groupId) {
    lock_guard<mutex> lock(groupsMu_);
    auto it = groups_.find(groupId);
    if (it != groups_.end()) return *it->second;
    auto g = make_unique<GroupState>();
    GroupState& ref = *g;
    groups_.emplace(groupId, move(g));
    return ref;
}

string GroupCoordinator::generate_member_id(const string& groupId) {
    lock_guard<mutex> lock(groupsMu_);
    return groupId + "-member-" + to_string(nextMemberSeq_++);
}

FindCoordinatorResponse GroupCoordinator::find_coordinator(const FindCoordinatorRequest& req) {
    (void)req;  // M3: single broker, always the coordinator for every group
    return FindCoordinatorResponse{ErrorCode::None, brokerId_, host_, port_};
}

JoinGroupResponse GroupCoordinator::join_group(const JoinGroupRequest& req) {
    GroupState& g = get_or_create_group(req.groupId);
    string memberId = generate_member_id(req.groupId);

    unique_lock<mutex> lock(g.mu);
    g.topic = req.topic;

    if (!g.rebalanceInProgress) {
        g.rebalanceInProgress = true;
        g.members.clear();  // fresh membership for the generation being formed
        g.joinDeadlineMs = now_ms() + kRebalanceWindowMs;
    }
    g.members[memberId] = MemberState{req.sessionTimeoutMs, now_ms()};

    auto deadline = chrono::system_clock::time_point(chrono::milliseconds(g.joinDeadlineMs));
    g.cv.wait_until(lock, deadline, [&] { return !g.rebalanceInProgress; });

    if (g.rebalanceInProgress) {
        // First thread to observe the deadline pass finalizes the generation;
        // everyone else (serialized by g.mu) just reads what it produced.
        g.generationId++;
        g.leaderId = g.members.empty() ? string() : g.members.begin()->first;  // sorted map: smallest id
        g.rebalanceInProgress = false;
        g.needsRebalance = false;
        g.syncReady = false;
        g.assignment.clear();
        g.cv.notify_all();
    }

    JoinGroupResponse resp;
    resp.errorCode = ErrorCode::None;
    resp.generationId = g.generationId;
    resp.memberId = memberId;
    resp.leaderId = g.leaderId;
    for (const auto& [id, _] : g.members) resp.members.push_back(id);
    return resp;
}

SyncGroupResponse GroupCoordinator::sync_group(const SyncGroupRequest& req) {
    GroupState& g = get_or_create_group(req.groupId);
    unique_lock<mutex> lock(g.mu);

    if (req.generationId != g.generationId) {
        return SyncGroupResponse{ErrorCode::RebalanceInProgress, {}};
    }
    if (!g.members.count(req.memberId)) {
        return SyncGroupResponse{ErrorCode::UnknownMemberId, {}};
    }

    if (!req.assignments.empty()) {
        g.assignment.clear();
        for (const SyncGroupAssignment& a : req.assignments) g.assignment[a.memberId] = a.partitions;
        g.syncReady = true;
        g.cv.notify_all();
    } else {
        uint32_t generationAtEntry = req.generationId;
        bool ready = g.cv.wait_for(lock, chrono::milliseconds(kSyncTimeoutMs), [&] {
            return g.syncReady || g.generationId != generationAtEntry;
        });
        if (!ready) return SyncGroupResponse{ErrorCode::RequestTimedOut, {}};
        if (g.generationId != generationAtEntry) {
            return SyncGroupResponse{ErrorCode::RebalanceInProgress, {}};
        }
    }

    SyncGroupResponse resp;
    resp.errorCode = ErrorCode::None;
    auto it = g.assignment.find(req.memberId);
    if (it != g.assignment.end()) resp.partitions = it->second;
    return resp;
}

HeartbeatResponse GroupCoordinator::heartbeat(const HeartbeatRequest& req) {
    GroupState& g = get_or_create_group(req.groupId);
    lock_guard<mutex> lock(g.mu);

    auto it = g.members.find(req.memberId);
    if (it == g.members.end()) {
        return HeartbeatResponse{ErrorCode::UnknownMemberId};
    }
    if (req.generationId != g.generationId || g.needsRebalance) {
        return HeartbeatResponse{ErrorCode::RebalanceInProgress};
    }
    it->second.lastHeartbeatMs = now_ms();

    // Opportunistic staleness sweep — no background reaper thread.
    uint64_t now = now_ms();
    for (auto mit = g.members.begin(); mit != g.members.end();) {
        if (mit->first != req.memberId &&
            now - mit->second.lastHeartbeatMs > mit->second.sessionTimeoutMs) {
            mit = g.members.erase(mit);
            g.needsRebalance = true;
        } else {
            ++mit;
        }
    }
    return HeartbeatResponse{ErrorCode::None};
}

LeaveGroupResponse GroupCoordinator::leave_group(const LeaveGroupRequest& req) {
    GroupState& g = get_or_create_group(req.groupId);
    lock_guard<mutex> lock(g.mu);
    if (g.members.erase(req.memberId) > 0) {
        g.needsRebalance = true;
    }
    return LeaveGroupResponse{ErrorCode::None};
}

void GroupCoordinator::load_offsets_if_needed(GroupState& g, const string& groupId) {
    if (g.offsetsLoaded) return;
    g.offsetsLoaded = true;
    string path = dataDir_ + "/__consumer_offsets/" + groupId + ".offsets";
    ifstream in(path);
    if (!in) return;
    string topic;
    uint32_t partition;
    uint64_t offset;
    while (in >> topic >> partition >> offset) {
        g.offsets[{topic, partition}] = offset;
    }
}

void GroupCoordinator::persist_offsets(GroupState& g, const string& groupId) {
    string dir = dataDir_ + "/__consumer_offsets";
    fs::create_directories(dir);
    ofstream out(dir + "/" + groupId + ".offsets", ios::trunc);
    for (const auto& [key, offset] : g.offsets) {
        out << key.first << " " << key.second << " " << offset << "\n";
    }
}

CommitOffsetResponse GroupCoordinator::commit_offset(const CommitOffsetRequest& req) {
    GroupState& g = get_or_create_group(req.groupId);
    lock_guard<mutex> lock(g.mu);
    load_offsets_if_needed(g, req.groupId);

    // Not validating memberId/generationId here: a commit landing just after
    // a rebalance is still a valid, forward-only offset update, and rejecting
    // it would only risk losing progress for no benefit at this scale.
    g.offsets[{req.topic, req.partitionId}] = req.offset;
    persist_offsets(g, req.groupId);
    return CommitOffsetResponse{ErrorCode::None};
}

FetchOffsetResponse GroupCoordinator::fetch_offset(const FetchOffsetRequest& req) {
    GroupState& g = get_or_create_group(req.groupId);
    lock_guard<mutex> lock(g.mu);
    load_offsets_if_needed(g, req.groupId);

    auto it = g.offsets.find({req.topic, req.partitionId});
    int64_t offset = (it != g.offsets.end()) ? static_cast<int64_t>(it->second) : -1;
    return FetchOffsetResponse{ErrorCode::None, offset};
}

}  // namespace mk
