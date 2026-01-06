#pragma once
#include <string>
#include <vector>
#include <variant>
#include <cstdint>
#include <string_view>


//https://redis.io/docs/latest/develop/reference/protocol-spec/
enum class RespType {
  SIMPLE_STRING,    // +
  BULK_STRING,      // $<size> (-1 == null)
  ERROR,            // -
  INT,              // :
  BOOL,             // #
  DOUBLE,           // ,
  BIG_NUMBER,       // (
  NULL_VAL,         // _
  ARRAY,            // *<size> (-1 == null)
  MAP,              // %
  SET,              // ~
  ATTRIBUTE,        // |
  PUSH,             // >
  VERBATIM_STRING   // =
};

struct RespObject {
  RespType type;
  union {
    int64_t int_val;       // For :, # (integers, bools)
    double double_val;     // For , (doubles)

    struct {               // For +, -, $, ( (Strings and BigNumber)
      const char* ptr;  // Pointer into your MAIN BUFFER
      size_t len;       // Length of the string
    } str_view;
  };

  // Only Arrays/maps need dynamic memory .
  std::vector<RespObject> children;

  std::string_view AsString() const {
    return std::string_view(str_view.ptr, str_view.len);
  }
};

class RespParser{
public:
  void ParseBuffer(const char* buffer, size_t length);
  std::vector<RespObject> GetObjects();
  void PrintResp(const RespObject& obj, int indent = 0);
  std::string BuildScan(const std::string& cursor, const std::string& pattern);
  std::string BuildGet(const std::string& pattern);
  void SqlToResp(std::string &query);
  void ClearObjects() { RespObjects.clear(); }
private:
  std::vector<RespObject> RespObjects;

  template <typename T>
  auto ParseNumeric(const char*& cursor, const char* end) -> T;
  RespObject ParseNext(const char*& cursor, const char* end);

  void PrintIndent(int indent);

};