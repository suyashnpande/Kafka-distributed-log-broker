#include "net/socket.h"

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include "common/codec.h"

using namespace std;

namespace mk {

Socket::Socket(Socket&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        close_if_valid();
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

Socket::~Socket() { close_if_valid(); }

void Socket::close_if_valid() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

Socket Socket::connect(const string& host, uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    string portStr = to_string(port);
    int rc = getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res);
    if (rc != 0) {
        throw runtime_error("getaddrinfo(" + host + "): " + gai_strerror(rc));
    }

    int fd = -1;
    for (addrinfo* p = res; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;
        if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
        ::close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) {
        throw runtime_error("connect to " + host + ":" + portStr + " failed: " +
                             strerror(errno));
    }
    return Socket(fd);
}

Socket Socket::listen(const string& host, uint16_t port, int backlog) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    addrinfo* res = nullptr;
    string portStr = to_string(port);
    const char* node = host.empty() ? nullptr : host.c_str();
    int rc = getaddrinfo(node, portStr.c_str(), &hints, &res);
    if (rc != 0) {
        throw runtime_error("getaddrinfo(" + host + "): " + gai_strerror(rc));
    }

    int fd = -1;
    for (addrinfo* p = res; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;
        int yes = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        if (::bind(fd, p->ai_addr, p->ai_addrlen) == 0) break;
        ::close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) {
        throw runtime_error("bind on " + host + ":" + portStr + " failed: " +
                             strerror(errno));
    }
    if (::listen(fd, backlog) != 0) {
        ::close(fd);
        throw runtime_error(string("listen failed: ") + strerror(errno));
    }
    return Socket(fd);
}

Socket Socket::accept() const {
    int cfd = ::accept(fd_, nullptr, nullptr);
    if (cfd < 0) throw runtime_error(string("accept failed: ") + strerror(errno));
    return Socket(cfd);
}

void Socket::send_all(const uint8_t* data, size_t len) const {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd_, data + sent, len - sent, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw runtime_error(string("send failed: ") + strerror(errno));
        }
        if (n == 0) throw runtime_error("connection closed during send");
        sent += static_cast<size_t>(n);
    }
}

void Socket::recv_all(uint8_t* data, size_t len) const {
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::recv(fd_, data + got, len - got, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw runtime_error(string("recv failed: ") + strerror(errno));
        }
        if (n == 0) throw runtime_error("connection closed during recv");
        got += static_cast<size_t>(n);
    }
}

RequestFrame read_request_frame(const Socket& sock) {
    uint8_t lenBuf[4];
    sock.recv_all(lenBuf, 4);
    Reader lenReader(lenBuf, 4);
    uint32_t totalLen = lenReader.get_u32();

    vector<uint8_t> body(totalLen);
    if (totalLen > 0) sock.recv_all(body.data(), totalLen);

    Reader r(body.data(), body.size());
    RequestFrame f;
    f.requestType = r.get_u16();
    f.correlationId = r.get_u32();
    f.payload.assign(body.begin() + static_cast<long>(r.pos()), body.end());
    return f;
}

void write_request_frame(const Socket& sock, const RequestFrame& f) {
    vector<uint8_t> body;
    put_u16(body, f.requestType);
    put_u32(body, f.correlationId);
    body.insert(body.end(), f.payload.begin(), f.payload.end());

    vector<uint8_t> out;
    put_u32(out, static_cast<uint32_t>(body.size()));
    out.insert(out.end(), body.begin(), body.end());
    sock.send_all(out.data(), out.size());
}

ResponseFrame read_response_frame(const Socket& sock) {
    uint8_t lenBuf[4];
    sock.recv_all(lenBuf, 4);
    Reader lenReader(lenBuf, 4);
    uint32_t totalLen = lenReader.get_u32();

    vector<uint8_t> body(totalLen);
    if (totalLen > 0) sock.recv_all(body.data(), totalLen);

    ResponseFrame f;
    Reader r(body.data(), body.size());
    f.correlationId = r.get_u32();
    f.payload.assign(body.begin() + 4, body.end());
    return f;
}

void write_response_frame(const Socket& sock, const ResponseFrame& f) {
    vector<uint8_t> body;
    put_u32(body, f.correlationId);
    body.insert(body.end(), f.payload.begin(), f.payload.end());

    vector<uint8_t> out;
    put_u32(out, static_cast<uint32_t>(body.size()));
    out.insert(out.end(), body.begin(), body.end());
    sock.send_all(out.data(), out.size());
}

}  // namespace mk
