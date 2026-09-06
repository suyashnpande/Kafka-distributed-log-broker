#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "net/socket.h"

// Static cluster membership (--peers) plus a persistent-with-reconnect RPC
// client to each peer, used for the Raft-inspired election and (later) the
// controller's LEADER_AND_ISR broadcast. One TCP connection per peer is kept
// open and reused — at a 150-300ms election timeout with heartbeats well
// under that, reconnecting per call would be wasteful; a dead/failed call
// just drops the cached connection so the next call reconnects.

namespace mk {

struct Address {
    std::string host;
    uint16_t port = 0;
};

class ClusterMembership {
public:
    // peers must include an entry for selfId.
    ClusterMembership(uint32_t selfId, std::map<uint32_t, Address> peers);

    uint32_t self_id() const { return selfId_; }
    uint32_t broker_count() const { return static_cast<uint32_t>(peers_.size()); }
    std::vector<uint32_t> all_ids() const;  // ascending, includes self
    std::optional<Address> address_of(uint32_t id) const;

    // Sends req to peerId and returns its response, or nullopt on any
    // connect/send/recv failure (peer down, refused, etc.) — callers treat
    // that exactly like an RPC timeout. Not valid to call with peerId ==
    // self_id().
    std::optional<ResponseFrame> call(uint32_t peerId, const RequestFrame& req);

private:
    struct PeerConn {
        std::mutex mu;
        Socket sock;  // default-constructed = not connected yet
    };

    uint32_t selfId_;
    std::map<uint32_t, Address> peers_;
    std::unordered_map<uint32_t, std::unique_ptr<PeerConn>> conns_;
};

}  // namespace mk
