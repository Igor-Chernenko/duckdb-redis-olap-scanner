/*
redis_client.hpp


*/
#ifndef REDIS_CLIENT_HPP
#define REDIS_CLIENT_HPP

#include "socket_os.hpp"

#include <string>
#include <vector>
#include <iostream>


constexpr size_t BUFFER_SIZE = 16384;

class RedisClient {
private:

  SOCKET sock_fd;
  bool is_connected;

  /*
  Instead of creating new strings for every packet,
  we keep one big chunk of memory alive

     - buffer: Pointer to the start of the memory block
     - capacity: How big the block is
     - offset: Where we are currently writing in the buffer
  */
  char* buffer;
  size_t buffer_capacity;
  size_t current_offset;

  /*
    Grow buffer if a Redis response is larger than the buffer
  */
  void EnsureBufferSize(size_t needed_size);

  /*
  Reads the raw bytes back from the socket.
    - Returns a pointer to the internal 'buffer'.
    - Returns how long the data is (size_t)
  */
  std::pair<char*, size_t> ReadResponse();

public:

  std::string host = "127.0.0.1";
  int port = 6379;
  float connection_timeout = 5;

  // Constructor: allocates the empty buffer.
  RedisClient();
  // Destructor: calls CLOSE_SOCKET()
  ~RedisClient();

  /*
  Connects to the Redis server.
    - Takes standard C-strings (char*) to be compatible with DuckDB's internal strings.
    - Returns true if the handshake succeeded.
  */
  bool Connect();

  // Manually closes the connection.
  void Disconnect();

  bool SendCommand(const std::vector<std::string>& args);

  /*
  Reads the raw bytes back from the socket.
    - Returns a pointer to the internal 'buffer'.
    - Returns how long the data is (size_t)
  */
  bool CheckedReadResponse();
};

#endif // REDIS_CLIENT_HPP
