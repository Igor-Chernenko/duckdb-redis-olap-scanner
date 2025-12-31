#define DUCKDB_EXTENSION_MAIN

#include "redduck_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "transport/redis_client.hpp"

#include <mutex>
#include <openssl/opensslv.h>

namespace duckdb {

inline void RedduckScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Redduck " + name.GetString() + " 🐥");
	});
}

inline void SetNameScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	/*
	 *args is an array of argument collumn vectors func(name, age), args[0] = 2048 row vector of names, args[1] = 2048 row vector of age
	 *state represents the state of the query, used for specific purposes
	 *result is the vector that you must write out your result into
	*/
	auto &input_vector = args.data[0]; // get args[0]

	// execute runs a loop on a collum (input_vector) from row 0-2047 and outputs it into a result vector
	// collum should be of type first temple (string_t)
	// output should be of type first temple (string_t)
	UnaryExecutor::Execute<string_t, string_t>(input_vector, result, args.size(),
	    [&](string_t input_name) {
		// We just write the constant string to the result for every row.
		return StringVector::AddString(result, "name is set");
	});
}

RedisClient redis_client;
std::mutex client_mutex;
inline void SetAddressScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	std::scoped_lock<std::mutex> lock(client_mutex);
    auto &input_vector = args.data[0];

    // If the user tries: SELECT redis_connect(my_col) FROM table; This triggers FLAT_VECTOR
    if (input_vector.GetVectorType() != VectorType::CONSTANT_VECTOR) {
        throw duckdb::InvalidInputException("redis_connect() only accepts a constant string (e.g., '127.0.0.1:6379'), not a column.");
    }

    if (ConstantVector::IsNull(input_vector)) {
        throw duckdb::InvalidInputException("redis_connect() cannot be NULL");
    }

    auto input_data = ConstantVector::GetData<string_t>(input_vector);
    string_t input_val = input_data[0];

    const char* ptr = input_val.GetData();
    size_t len = input_val.GetSize();


    const char* colon_pos = (const char*)memchr(ptr, ':', len);

    if (!colon_pos) {
        throw duckdb::InvalidInputException("Invalid format. Expected 'HOST:PORT'");
    }

    size_t host_len = colon_pos - ptr;
    size_t port_len = len - host_len - 1;

    // Convert to std::string
    std::string host_str(ptr, host_len);
    std::string port_str(colon_pos + 1, port_len);

    int port_int = 0;
    try { port_int = std::stoi(port_str);
    } catch (...) {
        throw duckdb::InvalidInputException("Port must be a valid number");
    }

    redis_client.host = host_str;
    redis_client.port = port_int;
	bool conn_result = redis_client.Connect();
	if (!conn_result) {
		throw duckdb::InvalidInputException("Connection failed");
	}
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
    auto result_data = ConstantVector::GetData<string_t>(result);

    std::string success_msg = "Redis Target Set: " + host_str + ":" + port_str;

    // We must use StringVector::AddString to safely allocate memory for the result string
    result_data[0] = StringVector::AddString(result, success_msg);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto redduck_scalar_function = ScalarFunction("redduck", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RedduckScalarFun);
	auto set_name_scalar_function = ScalarFunction("set_name", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SetNameScalarFun);
	auto set_address_scalar_function = ScalarFunction("redis_connect", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SetAddressScalarFun);

	loader.RegisterFunction(redduck_scalar_function);
	loader.RegisterFunction(set_name_scalar_function);
	loader.RegisterFunction(set_address_scalar_function);
}

void RedduckExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RedduckExtension::Name() {
	return "redduck";
}

std::string RedduckExtension::Version() const {
#ifdef EXT_VERSION_REDDUCK
	return EXT_VERSION_REDDUCK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(redduck, loader) {
	duckdb::LoadInternal(loader);
}
}
