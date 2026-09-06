#pragma once

#include <atomic>
#include <functional>
#include <string>

#include "net/socket.h"

// Thread-per-connection blocking TCP server (M2 network model). Upgrading to
// a thread pool is out of scope until M3+.

namespace mk {

class TcpServer {
public:
    using Handler = std::function<ResponseFrame(const RequestFrame&)>;

    TcpServer(std::string host, uint16_t port, Handler handler);

    // Binds and accepts connections until stop() is called. Blocking.
    void run();
    void stop();

private:
    void session(Socket client);

    std::string host_;
    uint16_t port_;
    Handler handler_;
    std::atomic<bool> running_{false};
    Socket listener_;
};

}  // namespace mk
