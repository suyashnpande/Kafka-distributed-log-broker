#pragma once

#include <cstdint>

namespace mk {

// Request frame's requestType field.
enum class RequestType : uint16_t {
    Produce = 1,
    Fetch = 2,
    Metadata = 3,
    Replicate = 10,           // M5
    BrokerHeartbeat = 11,      // M4
    LeaderAndISR = 12,         // M4
    RequestVote = 13,          // M4
    ControllerHeartbeat = 14,  // M4
    IsrUpdate = 15,            // M5 — no number assigned in AGENT_BRIEF.md's table
    JoinGroup = 20,            // M3
    SyncGroup = 21,            // M3
    Heartbeat = 22,            // M3
    LeaveGroup = 23,           // M3
    CommitOffset = 24,         // M3
    FetchOffset = 25,          // M3
    FindCoordinator = 26,      // M3
    CreateTopic = 27,          // M3 — no number assigned in AGENT_BRIEF.md's table
};

enum class ErrorCode : int16_t {
    None = 0,
    UnknownTopic = 3,
    NotLeaderForPartition = 6,
    RequestTimedOut = 7,
    UnknownMemberId = 25,
    RebalanceInProgress = 27,
    UnknownRequestType = 99,
};

}  // namespace mk
