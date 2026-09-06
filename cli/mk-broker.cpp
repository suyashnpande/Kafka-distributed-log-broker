#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <string>

#include "broker/broker.h"
#include "net/server.h"

using namespace std;
using namespace mk;

namespace {

void usage() {
    cerr << "usage: mk-broker --id N --port P --data-dir D [--host H] "
            "[--peers id@host:port,id@host:port,...] [--unclean-leader-election]\n";
}

// "1@localhost:9091,2@localhost:9092" -> {1: {localhost,9091}, 2: {localhost,9092}}
map<uint32_t, Address> parse_peers(const string& spec) {
    map<uint32_t, Address> peers;
    stringstream ss(spec);
    string token;
    while (getline(ss, token, ',')) {
        size_t at = token.find('@');
        size_t colon = token.rfind(':');
        if (at == string::npos || colon == string::npos || colon < at) {
            cerr << "malformed --peers entry (want id@host:port): " << token << "\n";
            exit(1);
        }
        uint32_t peerId = static_cast<uint32_t>(stoul(token.substr(0, at)));
        string peerHost = token.substr(at + 1, colon - at - 1);
        uint16_t peerPort = static_cast<uint16_t>(stoul(token.substr(colon + 1)));
        peers[peerId] = Address{peerHost, peerPort};
    }
    return peers;
}

}  // namespace

int main(int argc, char** argv) {
    uint32_t id = 0;
    bool haveId = false;
    uint16_t port = 0;
    bool havePort = false;
    string dataDir;
    string host = "0.0.0.0";
    optional<string> peersSpec;
    bool uncleanLeaderElectionEnable = false;

    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        auto next = [&](const char* flag) -> string {
            if (i + 1 >= argc) {
                cerr << "missing value for " << flag << "\n";
                exit(1);
            }
            return argv[++i];
        };
        if (arg == "--id") {
            id = static_cast<uint32_t>(stoul(next("--id")));
            haveId = true;
        } else if (arg == "--port") {
            port = static_cast<uint16_t>(stoul(next("--port")));
            havePort = true;
        } else if (arg == "--data-dir") {
            dataDir = next("--data-dir");
        } else if (arg == "--host") {
            host = next("--host");
        } else if (arg == "--peers") {
            peersSpec = next("--peers");
        } else if (arg == "--unclean-leader-election") {
            uncleanLeaderElectionEnable = true;
        } else {
            cerr << "unknown argument: " << arg << "\n";
            usage();
            return 1;
        }
    }

    if (!haveId || !havePort || dataDir.empty()) {
        usage();
        return 1;
    }

    optional<map<uint32_t, Address>> peers;
    if (peersSpec) {
        peers = parse_peers(*peersSpec);
        (*peers)[id] = Address{host, port};  // ensure self is always present
    }

    Broker broker(id, host, port, dataDir, peers, uncleanLeaderElectionEnable);
    TcpServer server(host, port, [&broker](const RequestFrame& req) {
        return broker.handle(req);
    });

    cout << "mk-broker id=" << id << " listening on " << host << ":" << port
         << " data-dir=" << dataDir;
    if (peers) cout << " cluster-size=" << peers->size();
    cout << "\n";
    server.run();
    return 0;
}
