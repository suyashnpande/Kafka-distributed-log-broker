#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "storage/partition.h"

// Leader-side replica tracking (M5). Owned by Broker, covering every
// partition this broker currently leads. No per-partition thread: a single
// ~200ms sweep thread re-evaluates every locally-led partition's ISR/HWM from
// elapsed time, so a follower that stops fetching entirely still eventually
// drops out of ISR even with no active produce/fetch traffic to trigger a
// reactive recheck (mirrors ClusterController's own election-timer thread).

namespace mk {

class IsrManager {
public:
    explicit IsrManager(uint32_t selfBrokerId);
    ~IsrManager();

    IsrManager(const IsrManager&) = delete;
    IsrManager& operator=(const IsrManager&) = delete;

    // Registers/updates the partitions this broker currently leads and their
    // full replica set, so the sweep thread knows what to evaluate. Called by
    // Broker::reconcile_replication whenever cluster assignment changes.
    // `partition` must outlive this registration — Broker owns both and only
    // calls stop_leading before a Partition would be destroyed.
    void set_leading(const std::string& topic, uint32_t partitionId, Partition& partition,
                      std::vector<uint32_t> replicas);
    void stop_leading(const std::string& topic, uint32_t partitionId);

    // Called from Broker::handle_replicate on every incoming FETCH_REPLICA.
    void on_replica_fetch(const std::string& topic, uint32_t partitionId, uint32_t replicaId,
                           uint64_t fetchOffset);

    // Called from Broker::handle_produce right after a local leader append,
    // to reclamp HWM immediately instead of waiting for the next replica
    // fetch or sweep tick to notice.
    void on_leader_append(const std::string& topic, uint32_t partitionId);

    // Current ISR membership for a locally-led partition (empty if unknown).
    std::vector<uint32_t> isr_for(const std::string& topic, uint32_t partitionId) const;

    // M5 checkpoint B: blocks (the caller's own thread — handle_produce's
    // connection thread) until this partition's high watermark exceeds
    // requiredOffset, or timeoutMs elapses. Returns false on timeout (or if
    // this broker stops leading the partition while waiting) — callers
    // surface that as ErrorCode::RequestTimedOut.
    bool wait_for_hwm(const std::string& topic, uint32_t partitionId, int64_t requiredOffset,
                       uint32_t timeoutMs);

    // Fired (outside any internal lock) whenever a recompute changes a
    // locally-led partition's ISR membership. Broker uses this to report the
    // change to the cluster controller (IsrUpdate). Set once at construction
    // time from Broker; not thread-safe to change afterward.
    using IsrChangeCallback =
        std::function<void(const std::string&, uint32_t, const std::vector<uint32_t>&)>;
    void set_on_isr_change(IsrChangeCallback cb) { onIsrChange_ = std::move(cb); }

private:
    struct PartitionState {
        Partition* partition = nullptr;
        std::vector<uint32_t> replicas;
        std::vector<uint32_t> isr;
        std::map<uint32_t, uint64_t> followerProgress;  // replicaId -> last known LEO
        std::map<uint32_t, uint64_t> lastFetchMs;       // replicaId -> last fetch time
    };
    using Key = std::pair<std::string, uint32_t>;

    // Recomputes ISR/HWM and notifies cv_. Caller holds mu_. Returns the new
    // ISR if membership changed, else nullopt — callers fire onIsrChange_
    // with it themselves, after releasing mu_ (the callback may do network
    // I/O and must not run with the lock held).
    std::optional<std::vector<uint32_t>> recompute(PartitionState& st) const;
    void run_loop();

    uint32_t selfBrokerId_;
    mutable std::mutex mu_;
    mutable std::condition_variable cv_;
    std::map<Key, PartitionState> partitions_;
    IsrChangeCallback onIsrChange_;

    std::atomic<bool> stop_{false};
    std::thread thread_;
};

}  // namespace mk
