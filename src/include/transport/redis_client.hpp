#ifndef REDIS_CLIENT_HPP
#define REDIS_CLIENT_HPP

#include "transport/socket_os.hpp"
#include "transport/resp_parser.hpp"

#include <string>
#include <vector>
#include <iostream>


constexpr size_t BUFFER_SIZE = 16384;

class RedisClient {
private:

  SOCKET sock_fd;
  bool is_connected;

  /*
     - buffer: Pointer to the start of the memory block
     - capacity: How big the block is
     - offset: Where we are currently writing in the buffer
  */
  char* buffer;
  size_t buffer_capacity;
  size_t current_offset;
  std::string query;

  /*
    Grow buffer if a Redis response is larger than the buffer
  */
  void EnsureBufferSize(size_t needed_size);


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
  bool Connect(const char* host, int port);

  // Manually closes the connection.
  void Disconnect();


  std::vector<std::string_view> RedisScan(std::string& query, RespParser& resp_parser);
  /*
  Reads the raw bytes back from the socket.
    - Returns a pointer to the internal 'buffer'.
    - Returns how long the data is (size_t)
  */
  std::vector<RespObject> CheckedReadResponse(RespParser& resp_parser);
  bool CheckedSend(const std::string& package);

  void ClearBuffer();
};

#endif // REDIS_CLIENT_HPP