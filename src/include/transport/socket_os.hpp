/*
socket_os.hpp


*/
#pragma once
#ifndef SOCKET_COMPAT_HPP
#define SOCKET_COMPAT_HPP


// WINDOWS IMPLEMENTATION
#ifdef _WIN32

  #include <winsock2.h>
  #include <ws2tcpip.h>

  // Compiler retrieves the "Ws2_32.lib" library automatically.
  #pragma comment(lib, "Ws2_32.lib")

  // Windows uses 'int' for the length of addresses, but Linux uses 'socklen_t'.
  typedef int socklen_t;

// Starting the network library manually.
inline bool init_sockets() {
  // WSADATA Windows struct initialization
  WSADATA wsaData;
  // MAKEWORD(2,2) requests version 2.2 of Winsock
  return WSAStartup(MAKEWORD(2, 2), &wsaData) == 0;
}
inline void cleanup_sockets() {
  WSACleanup();
}

#define CLOSE_SOCKET(s) closesocket(s)
#define GET_SOCKET_ERROR() (WSAGetLastError())


// LINUX / MACOS IMPLEMENTATION
#else

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>

typedef int SOCKET;

#define INVALID_SOCKET -1
#define SOCKET_ERROR -1

// Stub functions for Linux
inline bool init_sockets() { return true; }
inline void cleanup_sockets() {}

#define CLOSE_SOCKET(s) close(s)
#define GET_SOCKET_ERROR() (errno)

#endif

#endif // SOCKET_COMPAT_HPP

inline void set_socket_timeout(SOCKET s, int seconds) {
#ifdef _WIN32
  DWORD timeout = seconds * 1000;
  setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
  setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, sizeof(timeout));
#else
  struct timeval timeout;
  timeout.tv_sec = seconds;
  timeout.tv_usec = 0;
  setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const void*)&timeout, sizeof(timeout));
  setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (const void*)&timeout, sizeof(timeout));
#endif
}