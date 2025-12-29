#ifndef DUCKDB_SOCKET_OS_H
#define DUCKDB_SOCKET_OS_H


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

#endif// _WIN32
#endif //DUCKDB_SOCKET_OS_H
