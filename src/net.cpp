#include "net.h"
#include "util.h"
#include <cstring>
#include <vector>
#include <iostream>

#ifdef _WIN32
#pragma comment(lib, "ws2_32.lib")
#endif

namespace redis {

ClientConnection::ClientConnection(socket_t sock, std::string client_ip, uint16_t client_port)
    : fd(sock), ip(std::move(client_ip)), port(client_port), last_active_time(getUnixTimeSec()) {}

ClientConnection::~ClientConnection() {
    if (fd != INVALID_SOCK) {
#ifdef _WIN32
        closesocket(fd);
#else
        ::close(fd);
#endif
        fd = INVALID_SOCK;
    }
}

void ClientConnection::appendWrite(std::string_view data) {
    write_buffer.append(data);
}

EventReactor::EventReactor(std::string bind_ip, int port)
    : bind_ip_(std::move(bind_ip)), port_(port) {
#ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);
#endif
}

EventReactor::~EventReactor() {
    stop();
#ifdef _WIN32
    WSACleanup();
#endif
}

bool EventReactor::setNonBlocking(socket_t fd) {
#ifdef _WIN32
    u_long mode = 1;
    return ioctlsocket(fd, FIONBIO, &mode) == 0;
#else
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
}

bool EventReactor::setTcpNoDelay(socket_t fd) {
    int val = 1;
#ifdef _WIN32
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&val), sizeof(val)) == 0;
#else
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) == 0;
#endif
}

bool EventReactor::init() {
    server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == INVALID_SOCK) {
        return false;
    }

    int reuse = 1;
#ifdef _WIN32
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
#else
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
#endif

    setNonBlocking(server_fd_);
    setTcpNoDelay(server_fd_);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port_));
    inet_pton(AF_INET, bind_ip_.c_str(), &addr.sin_addr);

    if (::bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
#ifdef _WIN32
        closesocket(server_fd_);
#else
        ::close(server_fd_);
#endif
        server_fd_ = INVALID_SOCK;
        return false;
    }

    if (::listen(server_fd_, 1024) != 0) {
#ifdef _WIN32
        closesocket(server_fd_);
#else
        ::close(server_fd_);
#endif
        server_fd_ = INVALID_SOCK;
        return false;
    }

#ifndef _WIN32
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        ::close(server_fd_);
        server_fd_ = INVALID_SOCK;
        return false;
    }
    updateEpoll(server_fd_, EPOLLIN | EPOLLET, EPOLL_CTL_ADD);
#endif

    return true;
}

#ifndef _WIN32
void EventReactor::updateEpoll(socket_t fd, uint32_t events, int op) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    epoll_ctl(epoll_fd_, op, fd, &ev);
}
#endif

void EventReactor::acceptConnections() {
    while (true) {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);

        socket_t client_fd = ::accept(server_fd_, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
        if (client_fd == INVALID_SOCK) {
            break;
        }

        setNonBlocking(client_fd);
        setTcpNoDelay(client_fd);

        char ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, ip_str, sizeof(ip_str));
        uint16_t client_port = ntohs(client_addr.sin_port);

        clients_[client_fd] = std::make_unique<ClientConnection>(client_fd, ip_str, client_port);

#ifndef _WIN32
        updateEpoll(client_fd, EPOLLIN | EPOLLET, EPOLL_CTL_ADD);
#endif
    }
}

void EventReactor::readFromClient(ClientConnection& client) {
    char buf[16384];
    bool disconnect = false;

    while (true) {
#ifdef _WIN32
        int n = recv(client.fd, buf, sizeof(buf), 0);
        if (n == SOCKET_ERROR) {
            int err = WSAGetLastError();
            if (err == WSAEWOULDBLOCK) {
                break;
            }
            disconnect = true;
            break;
        }
#else
        int n = ::recv(client.fd, buf, sizeof(buf), 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            disconnect = true;
            break;
        }
#endif
        if (n == 0) {
            disconnect = true;
            break;
        }

        client.last_active_time = getUnixTimeSec();
        client.parser.feed(buf, n);

        std::vector<std::string> args;
        while (client.parser.nextCommand(args)) {
            if (command_handler_) {
                command_handler_(client, args);
            }
        }
    }

    if (disconnect) {
        closeClient(client.fd);
    } else {
        if (!client.write_buffer.empty()) {
            writeToClient(client);
        }
    }
}

void EventReactor::writeToClient(ClientConnection& client) {
    while (!client.write_buffer.empty()) {
#ifdef _WIN32
        int n = send(client.fd, client.write_buffer.data(), static_cast<int>(client.write_buffer.size()), 0);
        if (n == SOCKET_ERROR) {
            int err = WSAGetLastError();
            if (err == WSAEWOULDBLOCK) {
                break;
            }
            closeClient(client.fd);
            return;
        }
#else
        int n = ::send(client.fd, client.write_buffer.data(), client.write_buffer.size(), 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            closeClient(client.fd);
            return;
        }
#endif
        if (n > 0) {
            client.write_buffer.erase(0, n);
        }
    }

#ifndef _WIN32
    if (!client.write_buffer.empty()) {
        updateEpoll(client.fd, EPOLLIN | EPOLLOUT | EPOLLET, EPOLL_CTL_MOD);
    } else {
        updateEpoll(client.fd, EPOLLIN | EPOLLET, EPOLL_CTL_MOD);
    }
#endif

    if (client.write_buffer.empty() && client.close_after_write) {
        closeClient(client.fd);
    }
}

void EventReactor::closeClient(socket_t fd) {
#ifndef _WIN32
    if (epoll_fd_ >= 0) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    }
#endif
    clients_.erase(fd);
}

void EventReactor::checkIdleClients() {
    if (idle_timeout_sec_ == 0) return;

    uint64_t now = getUnixTimeSec();
    std::vector<socket_t> to_close;

    for (const auto& [fd, client] : clients_) {
        if (now - client->last_active_time >= idle_timeout_sec_) {
            to_close.push_back(fd);
        }
    }

    for (socket_t fd : to_close) {
        closeClient(fd);
    }
}

#ifndef _WIN32
void EventReactor::handleEpollEvents() {
    constexpr int MAX_EVENTS = 1024;
    epoll_event events[MAX_EVENTS];

    int num_events = epoll_wait(epoll_fd_, events, MAX_EVENTS, 50);
    for (int i = 0; i < num_events; ++i) {
        int fd = events[i].data.fd;
        uint32_t ev = events[i].events;

        if (fd == server_fd_) {
            acceptConnections();
            continue;
        }

        auto it = clients_.find(fd);
        if (it == clients_.end()) continue;

        if (ev & (EPOLLERR | EPOLLHUP)) {
            closeClient(fd);
            continue;
        }

        if (ev & EPOLLIN) {
            readFromClient(*(it->second));
        }

        if (clients_.count(fd) && (ev & EPOLLOUT)) {
            writeToClient(*(it->second));
        }
    }
}
#else
void EventReactor::handlePollEvents() {
    std::vector<WSAPOLLFD> fds;
    fds.reserve(clients_.size() + 1);

    WSAPOLLFD server_poll{};
    server_poll.fd = server_fd_;
    server_poll.events = POLLIN;
    fds.push_back(server_poll);

    for (const auto& [fd, client] : clients_) {
        WSAPOLLFD pfd{};
        pfd.fd = fd;
        pfd.events = POLLIN | (client->write_buffer.empty() ? 0 : POLLOUT);
        fds.push_back(pfd);
    }

    int ret = WSAPoll(fds.data(), static_cast<ULONG>(fds.size()), 50);
    if (ret <= 0) return;

    if (fds[0].revents & POLLIN) {
        acceptConnections();
    }

    for (size_t i = 1; i < fds.size(); ++i) {
        socket_t client_fd = fds[i].fd;
        short revents = fds[i].revents;

        auto it = clients_.find(client_fd);
        if (it == clients_.end()) continue;

        if (revents & (POLLERR | POLLHUP | POLLNVAL)) {
            closeClient(client_fd);
            continue;
        }

        if (revents & POLLIN) {
            readFromClient(*(it->second));
        }

        if (clients_.count(client_fd) && (revents & POLLOUT)) {
            writeToClient(*(it->second));
        }
    }
}
#endif

void EventReactor::run() {
    running_ = true;
    uint64_t last_idle_check = getMonotonicTimeMs();

    while (running_) {
#ifndef _WIN32
        handleEpollEvents();
#else
        handlePollEvents();
#endif

        if (periodic_handler_) {
            periodic_handler_();
        }

        uint64_t now = getMonotonicTimeMs();
        if (now - last_idle_check >= 5000) {
            checkIdleClients();
            last_idle_check = now;
        }
    }
}

void EventReactor::stop() {
    running_ = false;

    clients_.clear();

    if (server_fd_ != INVALID_SOCK) {
#ifdef _WIN32
        closesocket(server_fd_);
#else
        ::close(server_fd_);
#endif
        server_fd_ = INVALID_SOCK;
    }

#ifndef _WIN32
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
#endif
}

}
