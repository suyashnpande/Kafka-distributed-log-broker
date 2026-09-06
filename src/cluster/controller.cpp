#include "cluster/controller.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

using namespace std;

namespace mk {

namespace {

constexpr uint32_t kElectionTimeoutMinMs = 150;
constexpr uint32_t kElectionTimeoutMaxMs = 300;
constexpr uint32_t kHeartbeatIntervalMs = 50;
constexpr uint32_t kTimerPollMs = 20;
constexpr uint32_t kBrokerHeartbeatIntervalMs = 200;
constexpr uint32_t kAliveThresholdMs = 600;  // ~3 missed BrokerHeartbeats

uint64_t now_ms() {
    return chrono::duration_cast<chrono::milliseconds>(
               chrono::system_clock::now().time_since_epoch())
        .count();
}

uint32_t random_election_timeout() {
    static thread_local mt19937 gen(random_device{}());
    uniform_int_distribution<uint32_t> dist(kElectionTimeoutMinMs, kElectionTimeoutMaxMs);
    return dist(gen);
}

}  // namespace

ClusterController::ClusterController(string dataDir, ClusterMembership& membership,
                                       bool startBackgroundThread,
                                       bool uncleanLeaderElectionEnable)
    : dataDir_(move(dataDir)),
      membership_(membership),
      uncleanLeaderElectionEnable_(uncleanLeaderElectionEnable) {
    load_state();
    load_assignments();
    {
        lock_guard<mutex> lock(mu_);
        reset_election_deadline();
    }
    if (startBackgroundThread) {
        thread_ = thread(&ClusterController::run_loop, this);
    }
}

ClusterController::~ClusterController() {
    stop_.store(true);
    if (thread_.joinable()) thread_.join();
}

void ClusterController::log(const string& msg) const {
    cerr << "[broker " << membership_.self_id() << "] " << msg << "\n";
}

void ClusterController::persist_state() const {
    ofstream out(dataDir_ + "/raft_state", ios::trunc);
    out << currentTerm_ << "\n" << votedFor_ << "\n";
}

void ClusterController::load_state() {
    ifstream in(dataDir_ + "/raft_state");
    if (!in) return;
    uint32_t term;
    int64_t votedFor;
    if (in >> term >> votedFor) {
        currentTerm_ = term;
        votedFor_ = votedFor;
    }
}

void ClusterController::persist_assignments() const {
    ofstream out(dataDir_ + "/cluster_assignments", ios::trunc);
    for (const auto& [key, pa] : assignments_) {
        out << key.first << " " << key.second << " " << pa.leaderId << " " << pa.replicas.size();
        for (uint32_t id : pa.replicas) out << " " << id;
        out << " " << pa.isr.size();
        for (uint32_t id : pa.isr) out << " " << id;
        out << "\n";
    }
}

void ClusterController::load_assignments() {
    ifstream in(dataDir_ + "/cluster_assignments");
    if (!in) return;
    string topic;
    uint32_t partitionId, leaderId, replicaCount, isrCount, id;
    while (in >> topic >> partitionId >> leaderId >> replicaCount) {
        PartitionAssignment pa;
        pa.leaderId = leaderId;
        for (uint32_t i = 0; i < replicaCount; ++i) {
            in >> id;
            pa.replicas.push_back(id);
        }
        in >> isrCount;
        for (uint32_t i = 0; i < isrCount; ++i) {
            in >> id;
            pa.isr.push_back(id);
        }
        assignments_[{topic, partitionId}] = move(pa);
    }
}

void ClusterController::reset_election_deadline() {
    electionDeadlineMs_ = now_ms() + random_election_timeout();
}

void ClusterController::step_down(uint32_t newTerm) {
    if (newTerm > currentTerm_) {
        currentTerm_ = newTerm;
        votedFor_ = -1;
        persist_state();
    }
    role_ = Role::Follower;
    reset_election_deadline();
}

void ClusterController::become_leader() {
    role_ = Role::Leader;
    currentLeaderId_ = static_cast<int64_t>(membership_.self_id());
    log("became LEADER for term " + to_string(currentTerm_));
}

bool ClusterController::is_leader() const {
    lock_guard<mutex> lock(mu_);
    return role_ == Role::Leader;
}

uint32_t ClusterController::current_term() const {
    lock_guard<mutex> lock(mu_);
    return currentTerm_;
}

int64_t ClusterController::current_leader_id() const {
    lock_guard<mutex> lock(mu_);
    return currentLeaderId_;
}

RequestVoteResponse ClusterController::handle_request_vote(const RequestVoteRequest& req) {
    lock_guard<mutex> lock(mu_);
    if (req.term > currentTerm_) {
        step_down(req.term);
    }

    bool grant = false;
    if (req.term == currentTerm_ &&
        (votedFor_ == -1 || votedFor_ == static_cast<int64_t>(req.candidateId))) {
        votedFor_ = static_cast<int64_t>(req.candidateId);
        persist_state();
        grant = true;
        reset_election_deadline();  // granting a vote counts as hearing from a legitimate peer
    }
    return RequestVoteResponse{currentTerm_, grant};
}

ControllerHeartbeatResponse ClusterController::handle_controller_heartbeat(
    const ControllerHeartbeatRequest& req) {
    lock_guard<mutex> lock(mu_);
    if (req.term < currentTerm_) {
        return ControllerHeartbeatResponse{currentTerm_, false};  // stale leader
    }
    if (req.term > currentTerm_) {
        currentTerm_ = req.term;
        votedFor_ = -1;
        persist_state();
    }
    if (role_ != Role::Follower) {
        log("stepping down to follower on heartbeat from broker " + to_string(req.leaderId));
    }
    role_ = Role::Follower;
    currentLeaderId_ = static_cast<int64_t>(req.leaderId);
    reset_election_deadline();
    return ControllerHeartbeatResponse{currentTerm_, true};
}

BrokerHeartbeatResponse ClusterController::handle_broker_heartbeat(const BrokerHeartbeatRequest& req) {
    (void)req;
    return BrokerHeartbeatResponse{true};
}

vector<uint32_t> ClusterController::alive_broker_ids() const {
    lock_guard<mutex> lock(mu_);
    uint64_t now = now_ms();
    uint32_t selfId = membership_.self_id();
    vector<uint32_t> alive;
    for (uint32_t id : membership_.all_ids()) {
        if (id == selfId) {
            alive.push_back(id);
            continue;
        }
        auto it = lastAliveMs_.find(id);
        if (it != lastAliveMs_.end() && now - it->second <= kAliveThresholdMs) {
            alive.push_back(id);
        }
    }
    return alive;
}

optional<vector<pair<uint32_t, PartitionAssignment>>> ClusterController::create_topic(
    const string& topic, uint32_t numPartitions, uint32_t replicationFactor) {
    if (!is_leader()) return nullopt;

    vector<uint32_t> alive = alive_broker_ids();
    if (alive.empty()) return nullopt;  // shouldn't happen — self is always alive

    uint32_t rf = min(replicationFactor, static_cast<uint32_t>(alive.size()));
    if (rf == 0) rf = 1;

    vector<pair<uint32_t, PartitionAssignment>> result;
    vector<LeaderAndISREntry> wireEntries;
    {
        lock_guard<mutex> lock(mu_);
        for (uint32_t p = 0; p < numPartitions; ++p) {
            size_t leaderIdx = p % alive.size();
            uint32_t leader = alive[leaderIdx];
            vector<uint32_t> replicas;
            for (uint32_t k = 0; k < rf; ++k) {
                replicas.push_back(alive[(leaderIdx + k) % alive.size()]);
            }
            PartitionAssignment pa{leader, replicas, replicas};  // isr starts == replicas
            assignments_[{topic, p}] = pa;
            result.emplace_back(p, pa);
            wireEntries.push_back(LeaderAndISREntry{topic, p, pa.leaderId, pa.replicas, pa.isr});
        }
        persist_assignments();
    }

    broadcast_leader_and_isr(wireEntries);
    return result;
}

optional<PartitionAssignment> ClusterController::assignment_for(const string& topic,
                                                                   uint32_t partitionId) const {
    lock_guard<mutex> lock(mu_);
    auto it = assignments_.find({topic, partitionId});
    if (it == assignments_.end()) return nullopt;
    return it->second;
}

vector<pair<uint32_t, PartitionAssignment>> ClusterController::assignments_for_topic(
    const string& topic) const {
    lock_guard<mutex> lock(mu_);
    vector<pair<uint32_t, PartitionAssignment>> result;
    for (const auto& [key, pa] : assignments_) {
        if (key.first == topic) result.emplace_back(key.second, pa);
    }
    return result;  // assignments_ (std::map keyed by (topic,partitionId)) iterates in ascending order
}

vector<pair<pair<string, uint32_t>, PartitionAssignment>> ClusterController::all_assignments() const {
    lock_guard<mutex> lock(mu_);
    vector<pair<pair<string, uint32_t>, PartitionAssignment>> result;
    result.reserve(assignments_.size());
    for (const auto& [key, pa] : assignments_) result.emplace_back(key, pa);
    return result;
}

void ClusterController::apply_leader_and_isr(const vector<LeaderAndISREntry>& entries) {
    lock_guard<mutex> lock(mu_);
    for (const LeaderAndISREntry& e : entries) {
        assignments_[{e.topic, e.partitionId}] = PartitionAssignment{e.leaderId, e.replicas, e.isr};
    }
    persist_assignments();
}

void ClusterController::update_isr(const string& topic, uint32_t partitionId,
                                     const vector<uint32_t>& isr) {
    if (!is_leader()) {
        log("ignoring ISR update for " + topic + "/" + to_string(partitionId) +
            " — not the controller");
        return;
    }

    vector<LeaderAndISREntry> wireEntries;
    {
        lock_guard<mutex> lock(mu_);
        auto it = assignments_.find({topic, partitionId});
        if (it == assignments_.end()) return;
        it->second.isr = isr;
        persist_assignments();
        wireEntries.push_back(LeaderAndISREntry{topic, partitionId, it->second.leaderId,
                                                  it->second.replicas, it->second.isr});
    }
    broadcast_leader_and_isr(wireEntries);
}

void ClusterController::start_election() {
    uint32_t termForThisElection;
    uint32_t selfId = membership_.self_id();
    {
        lock_guard<mutex> lock(mu_);
        role_ = Role::Candidate;
        currentTerm_++;
        votedFor_ = static_cast<int64_t>(selfId);
        persist_state();
        reset_election_deadline();
        termForThisElection = currentTerm_;
    }
    log("became candidate for term " + to_string(termForThisElection));

    vector<uint32_t> peerIds;
    for (uint32_t id : membership_.all_ids()) {
        if (id != selfId) peerIds.push_back(id);
    }

    atomic<uint32_t> votes{1};  // vote for self
    vector<thread> threads;
    threads.reserve(peerIds.size());
    for (uint32_t peerId : peerIds) {
        threads.emplace_back([this, peerId, termForThisElection, selfId, &votes]() {
            RequestVoteRequest req{termForThisElection, selfId};
            vector<uint8_t> payload;
            encode_request_vote_request(req, payload);
            RequestFrame frame{static_cast<uint16_t>(RequestType::RequestVote),
                                next_correlation_id(), payload};
            auto respFrame = membership_.call(peerId, frame);
            if (!respFrame) return;

            Reader r(respFrame->payload.data(), respFrame->payload.size());
            RequestVoteResponse resp = decode_request_vote_response(r);

            lock_guard<mutex> lock(mu_);
            if (resp.term > currentTerm_) {
                step_down(resp.term);
                return;
            }
            if (resp.term == termForThisElection && resp.voteGranted) {
                votes.fetch_add(1);
            }
        });
    }
    for (thread& t : threads) t.join();

    lock_guard<mutex> lock(mu_);
    if (role_ != Role::Candidate || currentTerm_ != termForThisElection) {
        return;  // stepped down or moved on while votes were in flight
    }
    uint32_t majority = membership_.broker_count() / 2 + 1;
    if (votes.load() >= majority) {
        become_leader();
    }
    // Otherwise stays Candidate; the deadline reset above will retry the election.
}

void ClusterController::broadcast_heartbeats() {
    uint32_t term;
    uint32_t selfId = membership_.self_id();
    {
        lock_guard<mutex> lock(mu_);
        term = currentTerm_;
    }

    vector<uint32_t> peerIds;
    for (uint32_t id : membership_.all_ids()) {
        if (id != selfId) peerIds.push_back(id);
    }

    vector<thread> threads;
    threads.reserve(peerIds.size());
    for (uint32_t peerId : peerIds) {
        threads.emplace_back([this, peerId, term, selfId]() {
            ControllerHeartbeatRequest req{term, selfId};
            vector<uint8_t> payload;
            encode_controller_heartbeat_request(req, payload);
            RequestFrame frame{static_cast<uint16_t>(RequestType::ControllerHeartbeat),
                                next_correlation_id(), payload};
            auto respFrame = membership_.call(peerId, frame);
            if (!respFrame) return;

            Reader r(respFrame->payload.data(), respFrame->payload.size());
            ControllerHeartbeatResponse resp = decode_controller_heartbeat_response(r);

            lock_guard<mutex> lock(mu_);
            if (resp.term > currentTerm_) {
                step_down(resp.term);
            }
        });
    }
    for (thread& t : threads) t.join();
}

void ClusterController::broadcast_broker_heartbeats() {
    uint32_t selfId = membership_.self_id();
    vector<uint32_t> peerIds;
    for (uint32_t id : membership_.all_ids()) {
        if (id != selfId) peerIds.push_back(id);
    }

    vector<thread> threads;
    threads.reserve(peerIds.size());
    for (uint32_t peerId : peerIds) {
        threads.emplace_back([this, peerId]() {
            vector<uint8_t> payload;
            encode_broker_heartbeat_request(BrokerHeartbeatRequest{}, payload);
            RequestFrame frame{static_cast<uint16_t>(RequestType::BrokerHeartbeat),
                                next_correlation_id(), payload};
            auto respFrame = membership_.call(peerId, frame);
            if (!respFrame) return;

            lock_guard<mutex> lock(mu_);
            lastAliveMs_[peerId] = now_ms();
        });
    }
    for (thread& t : threads) t.join();
}

void ClusterController::broadcast_leader_and_isr(const vector<LeaderAndISREntry>& entries) {
    LeaderAndISRRequest req{entries};
    vector<uint8_t> payload;
    encode_leader_and_isr_request(req, payload);

    uint32_t selfId = membership_.self_id();
    vector<uint32_t> peerIds;
    for (uint32_t id : membership_.all_ids()) {
        if (id != selfId) peerIds.push_back(id);
    }

    vector<thread> threads;
    threads.reserve(peerIds.size());
    for (uint32_t peerId : peerIds) {
        threads.emplace_back([this, peerId, payload]() {
            RequestFrame frame{static_cast<uint16_t>(RequestType::LeaderAndISR),
                                next_correlation_id(), payload};
            membership_.call(peerId, frame);
            // Best-effort: a peer that's down when this fires won't learn the
            // assignment until a later broadcast (e.g. the next CREATE_TOPIC).
            // Full metadata catch-up-on-rejoin is out of scope for M4.
        });
    }
    for (thread& t : threads) t.join();
}

void ClusterController::check_partition_failover() {
    vector<uint32_t> alive = alive_broker_ids();
    auto isAlive = [&alive](uint32_t id) { return find(alive.begin(), alive.end(), id) != alive.end(); };

    vector<LeaderAndISREntry> wireEntries;
    bool anyChanged = false;
    {
        lock_guard<mutex> lock(mu_);
        for (auto& [key, pa] : assignments_) {
            if (isAlive(pa.leaderId)) continue;  // current leader is fine

            vector<uint32_t> isrCandidates;
            for (uint32_t id : pa.isr) {
                if (id != pa.leaderId && isAlive(id)) isrCandidates.push_back(id);
            }
            sort(isrCandidates.begin(), isrCandidates.end());

            uint32_t newLeader = 0;
            vector<uint32_t> newIsr;
            bool promoted = false;
            if (!isrCandidates.empty()) {
                newLeader = isrCandidates.front();
                newIsr = isrCandidates;
                promoted = true;
            } else if (uncleanLeaderElectionEnable_) {
                vector<uint32_t> replicaCandidates;
                for (uint32_t id : pa.replicas) {
                    if (id != pa.leaderId && isAlive(id)) replicaCandidates.push_back(id);
                }
                sort(replicaCandidates.begin(), replicaCandidates.end());
                if (!replicaCandidates.empty()) {
                    newLeader = replicaCandidates.front();
                    newIsr = {newLeader};
                    promoted = true;
                    log("UNCLEAN leader election for " + key.first + "/" + to_string(key.second) +
                        " -> broker " + to_string(newLeader) + " (possible data loss)");
                }
            }

            if (!promoted) {
                log("partition " + key.first + "/" + to_string(key.second) +
                    " has no alive leader candidate — staying unavailable" +
                    (uncleanLeaderElectionEnable_ ? "" : " (unclean leader election disabled)"));
                continue;
            }

            log("promoting broker " + to_string(newLeader) + " to leader of " + key.first + "/" +
                to_string(key.second) + " (old leader " + to_string(pa.leaderId) + " is dead)");
            pa.leaderId = newLeader;
            pa.isr = newIsr;
            wireEntries.push_back(
                LeaderAndISREntry{key.first, key.second, pa.leaderId, pa.replicas, pa.isr});
            anyChanged = true;
        }
        if (anyChanged) persist_assignments();
    }

    if (!wireEntries.empty()) broadcast_leader_and_isr(wireEntries);
    if (anyChanged && onAssignmentChange_) onAssignmentChange_();
}

void ClusterController::run_loop() {
    uint64_t lastBrokerHeartbeatMs = 0;
    while (!stop_.load()) {
        Role role;
        {
            lock_guard<mutex> lock(mu_);
            role = role_;
        }
        if (role == Role::Leader) {
            broadcast_heartbeats();
            uint64_t now = now_ms();
            if (now - lastBrokerHeartbeatMs >= kBrokerHeartbeatIntervalMs) {
                broadcast_broker_heartbeats();
                check_partition_failover();
                lastBrokerHeartbeatMs = now;
            }
            this_thread::sleep_for(chrono::milliseconds(kHeartbeatIntervalMs));
            continue;
        }

        uint64_t deadline;
        {
            lock_guard<mutex> lock(mu_);
            deadline = electionDeadlineMs_;
        }
        if (now_ms() >= deadline) {
            start_election();
        } else {
            this_thread::sleep_for(chrono::milliseconds(kTimerPollMs));
        }
    }
}

}  // namespace mk
