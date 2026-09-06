#pragma once

#include <cstdint>
#include <string>
#include <vector>

// Thin RAII wrapper over a POSIX TCP socket, plus the two frame shapes from
// AGENT_BRIEF.md section 5 (request/response), shared by the broker (server
// side) and the CLI tools (client side).

namespace mk {

class Socket {
public:
    Socket() = default;
    explicit Socket(int fd) : fd_(fd) {}
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;
    ~Socket();

    static Socket connect(const std::string& host, uint16_t port);
    static Socket listen(const std::string& host, uint16_t port, int backlog = 64);
    Socket accept() const;

    void send_all(const uint8_t* data, size_t len) const;
    void recv_all(uint8_t* data, size_t len) const;  // throws on EOF/error mid-read

    int fd() const { return fd_; }
    bool valid() const { return fd_ >= 0; }

private:
    void close_if_valid();
    int fd_ = -1;
};

struct RequestFrame {
    uint16_t requestType = 0;
    uint32_t correlationId = 0;
    std::vector<uint8_t> payload;
};

struct ResponseFrame {
    uint32_t correlationId = 0;
    std::vector<uint8_t> payload;
};

RequestFrame read_request_frame(const Socket& sock);
void write_request_frame(const Socket& sock, const RequestFrame& f);
ResponseFrame read_response_frame(const Socket& sock);
void write_response_frame(const Socket& sock, const ResponseFrame& f);

}  // namespace mk
