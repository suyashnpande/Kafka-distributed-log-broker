#include "replication/follower.h"

#include <chrono>

#include "protocol/requests.h"
#include "protocol/types.h"

using namespace std;

namespace mk {

namespace {
constexpr uint32_t kMaxFetchBytes = 1 << 20;
constexpr uint32_t kBackoffMs = 30;
}  // namespace

FollowerFetcher::FollowerFetcher(string topic, uint32_t partitionId, uint32_t selfBrokerId,
                                   string leaderHost, uint16_t leaderPort, Partition& partition)
    : topic_(move(topic)),
      partitionId_(partitionId),
      selfBrokerId_(selfBrokerId),
      leaderHost_(move(leaderHost)),
      leaderPort_(leaderPort),
      partition_(partition) {
    thread_ = thread(&FollowerFetcher::run_loop, this);
}

FollowerFetcher::~FollowerFetcher() {
    stop_.store(true);
    if (thread_.joinable()) thread_.join();
}

void FollowerFetcher::run_loop() {
    Socket sock;
    uint32_t correlationId = 1;

    while (!stop_.load()) {
        if (!sock.valid()) {
            try {
                sock = Socket::connect(leaderHost_, leaderPort_);
            } catch (const exception&) {
                this_thread::sleep_for(chrono::milliseconds(kBackoffMs));
                continue;
            }
        }

        try {
            ReplicaFetchRequest req{topic_, partitionId_, selfBrokerId_,
                                     static_cast<uint64_t>(partition_.log_end_offset()),
                                     kMaxFetchBytes};
            vector<uint8_t> payload;
            encode_replica_fetch_request(req, payload);
            write_request_frame(sock, RequestFrame{static_cast<uint16_t>(RequestType::Replicate),
                                                     correlationId++, payload});
            ResponseFrame respFrame = read_response_frame(sock);
            Reader r(respFrame.payload.data(), respFrame.payload.size());
            FetchResponse resp = decode_fetch_response(r);

            if (resp.errorCode != ErrorCode::None || resp.records.empty()) {
                // Error (e.g. leader moved) or nothing new yet — either way,
                // back off. A stale leader gets fixed by
                // Broker::reconcile_replication recreating us once it
                // observes a new LEADER_AND_ISR; we don't try to self-heal.
                this_thread::sleep_for(chrono::milliseconds(kBackoffMs));
                continue;
            }
            partition_.append_replicated(move(resp.records));
            // Caught up on this batch — loop again immediately to keep
            // pulling without waiting out the backoff.
        } catch (const exception&) {
            sock = Socket();
            this_thread::sleep_for(chrono::milliseconds(kBackoffMs));
        }
    }
}

}  // namespace mk
