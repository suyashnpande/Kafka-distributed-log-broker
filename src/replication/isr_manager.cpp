#include "replication/isr_manager.h"

#include <algorithm>
#include <chrono>
#include <iostream>

using namespace std;

namespace mk {

namespace {

// AGENT_BRIEF.md's "replicaMaxLagBytes" is renamed to a message-count lag
// here: follower progress is tracked in offsets, and tracking actual byte lag
// would need every record's encoded size kept alongside its offset, which
// nothing else in the system does (see M5 plan notes).
constexpr uint64_t kReplicaLagTimeMs = 1000;
constexpr uint64_t kReplicaMaxLagMessages = 1000;
constexpr uint32_t kSweepIntervalMs = 200;

uint64_t now_ms() {
    return chrono::duration_cast<chrono::milliseconds>(
               chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

IsrManager::IsrManager(uint32_t selfBrokerId)
    : selfBrokerId_(selfBrokerId), thread_(&IsrManager::run_loop, this) {}

IsrManager::~IsrManager() {
    stop_.store(true);
    if (thread_.joinable()) thread_.join();
}

void IsrManager::set_leading(const string& topic, uint32_t partitionId, Partition& partition,
                              vector<uint32_t> replicas) {
    lock_guard<mutex> lock(mu_);
    PartitionState& st = partitions_[{topic, partitionId}];
    st.partition = &partition;
    st.replicas = move(replicas);
    if (st.isr.empty()) st.isr = st.replicas;  // assume in-sync until proven otherwise
}

void IsrManager::stop_leading(const string& topic, uint32_t partitionId) {
    lock_guard<mutex> lock(mu_);
    partitions_.erase({topic, partitionId});
}

void IsrManager::on_replica_fetch(const string& topic, uint32_t partitionId, uint32_t replicaId,
                                   uint64_t fetchOffset) {
    optional<vector<uint32_t>> changed;
    {
        lock_guard<mutex> lock(mu_);
        auto it = partitions_.find({topic, partitionId});
        if (it == partitions_.end()) return;
        PartitionState& st = it->second;
        st.followerProgress[replicaId] = fetchOffset;
        st.lastFetchMs[replicaId] = now_ms();
        changed = recompute(st);
    }
    if (changed && onIsrChange_) onIsrChange_(topic, partitionId, *changed);
}

void IsrManager::on_leader_append(const string& topic, uint32_t partitionId) {
    optional<vector<uint32_t>> changed;
    {
        lock_guard<mutex> lock(mu_);
        auto it = partitions_.find({topic, partitionId});
        if (it == partitions_.end()) return;
        changed = recompute(it->second);
    }
    if (changed && onIsrChange_) onIsrChange_(topic, partitionId, *changed);
}

vector<uint32_t> IsrManager::isr_for(const string& topic, uint32_t partitionId) const {
    lock_guard<mutex> lock(mu_);
    auto it = partitions_.find({topic, partitionId});
    return (it != partitions_.end()) ? it->second.isr : vector<uint32_t>{};
}

bool IsrManager::wait_for_hwm(const string& topic, uint32_t partitionId, int64_t requiredOffset,
                               uint32_t timeoutMs) {
    unique_lock<mutex> lock(mu_);
    Key key{topic, partitionId};
    return cv_.wait_for(lock, chrono::milliseconds(timeoutMs), [&] {
        auto it = partitions_.find(key);
        if (it == partitions_.end() || !it->second.partition) return false;
        return it->second.partition->high_watermark() > requiredOffset;
    });
}

optional<vector<uint32_t>> IsrManager::recompute(PartitionState& st) const {
    if (!st.partition) return nullopt;
    uint64_t leo = static_cast<uint64_t>(st.partition->log_end_offset());
    uint64_t now = now_ms();

    vector<uint32_t> newIsr;
    for (uint32_t id : st.replicas) {
        if (id == selfBrokerId_) {
            newIsr.push_back(id);  // the leader is trivially caught up to its own LEO
            continue;
        }
        auto progIt = st.followerProgress.find(id);
        auto timeIt = st.lastFetchMs.find(id);
        if (progIt == st.followerProgress.end() || timeIt == st.lastFetchMs.end()) {
            continue;  // never fetched yet
        }
        bool timeOk = (now - timeIt->second) <= kReplicaLagTimeMs;
        bool lagOk = leo >= progIt->second && (leo - progIt->second) <= kReplicaMaxLagMessages;
        if (timeOk && lagOk) newIsr.push_back(id);
    }
    sort(newIsr.begin(), newIsr.end());

    uint64_t hwm = leo;
    for (uint32_t id : newIsr) {
        if (id == selfBrokerId_) continue;
        hwm = min(hwm, st.followerProgress.at(id));
    }

    st.partition->set_high_watermark(static_cast<int64_t>(hwm));
    cv_.notify_all();

    if (newIsr != st.isr) {
        cerr << "[broker " << selfBrokerId_ << "] ISR changed to {";
        for (size_t i = 0; i < newIsr.size(); ++i) cerr << (i ? "," : "") << newIsr[i];
        cerr << "}\n";
        st.isr = newIsr;
        return newIsr;
    }
    return nullopt;
}

void IsrManager::run_loop() {
    while (!stop_.load()) {
        this_thread::sleep_for(chrono::milliseconds(kSweepIntervalMs));

        vector<pair<Key, vector<uint32_t>>> changes;
        {
            lock_guard<mutex> lock(mu_);
            for (auto& [key, st] : partitions_) {
                if (auto changed = recompute(st)) {
                    changes.emplace_back(key, move(*changed));
                }
            }
        }
        if (onIsrChange_) {
            for (const auto& [key, isr] : changes) onIsrChange_(key.first, key.second, isr);
        }
    }
}

}  // namespace mk
