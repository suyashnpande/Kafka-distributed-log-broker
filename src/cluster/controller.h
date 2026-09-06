#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cluster/membership.h"
#include "protocol/requests.h"

// Raft-inspired controller election (M4 checkpoint A) plus the controller's
// topic/partition assignment (checkpoint B). Leader-only election — no
// replicated log, no lastLogIndex/lastLogTerm completeness check, per
// AGENT_BRIEF.md's own "Raft-inspired, not full Raft" framing. A single
// background thread either runs the election timer (Follower/Candidate) or,
// as Leader, broadcasts ControllerHeartbeat (election-critical, every 50ms)
// and BrokerHeartbeat (liveness tracking for CREATE_TOPIC assignment only,
// every 200ms — losing this does NOT trigger a re-election). term/votedFor
// and the assignment map persist to <dataDir>/raft_state and
// <dataDir>/cluster_assignments respectively.

namespace mk {

enum class Role { Follower, Candidate, Leader };

struct PartitionAssignment {
    uint32_t leaderId = 0;
    std::vector<uint32_t> replicas;
    std::vector<uint32_t> isr;
};

class ClusterController {
public:
    // startBackgroundThread=false is a testing seam: it lets tests drive
    // handle_request_vote/handle_controller_heartbeat directly without racing
    // a live election timer. uncleanLeaderElectionEnable governs
    // check_partition_failover's fallback when no ISR member of a
    // leaderless partition is alive (see AGENT_BRIEF.md M6).
    ClusterController(std::string dataDir, ClusterMembership& membership,
                       bool startBackgroundThread = true,
                       bool uncleanLeaderElectionEnable = false);
    ~ClusterController();

    ClusterController(const ClusterController&) = delete;
    ClusterController& operator=(const ClusterController&) = delete;

    RequestVoteResponse handle_request_vote(const RequestVoteRequest& req);
    ControllerHeartbeatResponse handle_controller_heartbeat(const ControllerHeartbeatRequest& req);
    BrokerHeartbeatResponse handle_broker_heartbeat(const BrokerHeartbeatRequest& req);

    bool is_leader() const;
    uint32_t current_term() const;
    int64_t current_leader_id() const;  // -1 if unknown

    // Controller-only: round-robins numPartitions across currently-alive
    // brokers (leader = alive[p % n]; replicas = replicationFactor consecutive
    // alive brokers starting at the leader, wrapping; isr starts == replicas),
    // persists, and broadcasts LEADER_AND_ISR to every peer. Returns nullopt if
    // this broker isn't the leader right now (caller should redirect using
    // current_leader_id() instead).
    std::optional<std::vector<std::pair<uint32_t, PartitionAssignment>>> create_topic(
        const std::string& topic, uint32_t numPartitions, uint32_t replicationFactor = 1);

    // This broker's cached view of who leads (topic, partitionId). nullopt if
    // never learned (no CREATE_TOPIC/LEADER_AND_ISR seen for it).
    std::optional<PartitionAssignment> assignment_for(const std::string& topic,
                                                        uint32_t partitionId) const;

    // All partitions this broker knows about for a topic, ascending by id.
    std::vector<std::pair<uint32_t, PartitionAssignment>> assignments_for_topic(
        const std::string& topic) const;

    // M5: every (topic, partitionId) this broker knows about, regardless of
    // topic — used by Broker::reconcile_replication to find every partition
    // it's a replica (or leader) for.
    std::vector<std::pair<std::pair<std::string, uint32_t>, PartitionAssignment>> all_assignments() const;

    // Follower-side: apply an incoming LEADER_AND_ISR broadcast.
    void apply_leader_and_isr(const std::vector<LeaderAndISREntry>& entries);

    // M5 checkpoint B: a partition leader reports its ISR changed. Controller-
    // only — no-op (with a log line) if this broker isn't currently the
    // controller, matching the same "who's authoritative" pattern as
    // create_topic. Updates the assignment, persists, and rebroadcasts
    // LEADER_AND_ISR so every broker's cached view stays current.
    void update_isr(const std::string& topic, uint32_t partitionId,
                     const std::vector<uint32_t>& isr);

    // M6: fired (outside any internal lock) whenever check_partition_failover
    // reassigns a partition's leader. Broker uses this to reconcile its own
    // replication state when *this* broker (the controller) is the one
    // promoted or otherwise affected — broadcast_leader_and_isr never reaches
    // self, so without this the controller's own broker wouldn't notice its
    // own decision. Set once at construction time from Broker.
    using AssignmentChangeCallback = std::function<void()>;
    void set_on_assignment_change(AssignmentChangeCallback cb) { onAssignmentChange_ = std::move(cb); }

private:
    void run_loop();
    void start_election();              // caller must NOT hold mu_
    void broadcast_heartbeats();        // caller must NOT hold mu_
    void broadcast_broker_heartbeats(); // caller must NOT hold mu_
    void broadcast_leader_and_isr(const std::vector<LeaderAndISREntry>& entries);  // NOT hold mu_

    // M6: for every assignment whose leaderId isn't currently alive, promotes
    // a replacement (preferring an alive ISR member; falling back to any
    // alive replica only if uncleanLeaderElectionEnable_ is true) and
    // broadcasts LEADER_AND_ISR for each one reassigned. Caller must NOT hold
    // mu_ (broadcasts internally, same as create_topic/update_isr).
    void check_partition_failover();

    // All of these require mu_ to already be held by the caller.
    void step_down(uint32_t newTerm);
    void become_leader();
    void reset_election_deadline();
    void persist_state() const;
    void load_state();
    void persist_assignments() const;
    void load_assignments();
    std::vector<uint32_t> alive_broker_ids() const;
    void log(const std::string& msg) const;

    uint32_t next_correlation_id() { return correlationCounter_.fetch_add(1); }

    std::string dataDir_;
    ClusterMembership& membership_;
    std::atomic<bool> stop_{false};
    std::thread thread_;
    std::atomic<uint32_t> correlationCounter_{1};

    mutable std::mutex mu_;
    Role role_ = Role::Follower;
    uint32_t currentTerm_ = 0;
    int64_t votedFor_ = -1;
    int64_t currentLeaderId_ = -1;
    uint64_t electionDeadlineMs_ = 0;

    std::map<std::pair<std::string, uint32_t>, PartitionAssignment> assignments_;
    std::map<uint32_t, uint64_t> lastAliveMs_;  // peerId -> last successful BrokerHeartbeat

    bool uncleanLeaderElectionEnable_;
    AssignmentChangeCallback onAssignmentChange_;
};

}  // namespace mk
