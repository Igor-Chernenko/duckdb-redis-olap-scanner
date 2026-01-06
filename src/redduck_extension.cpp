#define DUCKDB_EXTENSION_MAIN

#include "redduck_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "transport/redis_client.hpp"
#include "transport/resp_parser.hpp"

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
// -------------------------------------------------------------------------------------------------
//  redis_scan('address:port') scalar function
// -------------------------------------------------------------------------------------------------
RedisClient ScanClient;
RedisClient GetClient;

std::mutex scan_mutex;
std::mutex get_mutex;

//shared config
std::string redis_host = "127.0.0.1";
int redis_port = 6379;

inline void SetAddressScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
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

	{
    	std::scoped_lock lock(scan_mutex, get_mutex);

    	redis_host = host_str;
    	redis_port = port_int;

    	ScanClient.host = redis_host;
    	ScanClient.port = redis_port;

    	GetClient.host = redis_host;
    	GetClient.port = redis_port;

    	if (!ScanClient.Connect(redis_host.c_str(), redis_port)) {
    		throw InvalidInputException("Connection failed (scan client)");
    	}
    	if (!GetClient.Connect(redis_host.c_str(), redis_port)) {
    		throw InvalidInputException("Connection failed (get client)");
    	}
	}

    result.SetVectorType(VectorType::CONSTANT_VECTOR);
    auto result_data = ConstantVector::GetData<string_t>(result);

    std::string success_msg = "Redis Target Set: " + host_str + ":" + port_str;

    // We must use StringVector::AddString to safely allocate memory for the result string
    result_data[0] = StringVector::AddString(result, success_msg);
}
// -------------------------------------------------------------------------------------------------
//  redis_scan(pattern) table function
// -------------------------------------------------------------------------------------------------


struct RedisScanBindData : public FunctionData {
	std::string pattern;

	explicit RedisScanBindData(std::string pattern_p) : pattern(std::move(pattern_p)) {}

	unique_ptr<FunctionData> Copy() const override {
		// Bind data must be copyable because DuckDB may duplicate plans.
		return make_uniq<RedisScanBindData>(pattern);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<RedisScanBindData>();
		return pattern == other.pattern;
	}
};

struct RedisScanGlobalState : public GlobalTableFunctionState {
	std::string cursor = "0";
	bool done = false;

	std::vector<std::string> batch_keys;
	idx_t batch_pos = 0; // next index inside batch_keys to output


	// Parser object is kept here so we can reuse allocations.
	RespParser parser;

	// Force single-threaded scan:
	// This global state and the RedisClient socket/buffer are not safe for parallel scan threads.
	idx_t MaxThreads() const override {
		return 1;
	}

	~RedisScanGlobalState() override {
		// On query end, it's safe to release all parsed objects and reuse the buffer.
		// (DuckDB will not call us anymore after destruction.)
		parser.ClearObjects();
		ScanClient.ClearBuffer();
	}
};

static void FetchNextBatch(RedisScanGlobalState &state, const std::string &pattern) {

	state.batch_keys.clear();
	state.batch_pos = 0;

	state.parser.ClearObjects();
	ScanClient.ClearBuffer();

	for (;;) {
		std::string cmd = state.parser.BuildScan(state.cursor, pattern);

		if (!ScanClient.CheckedSend(cmd)) {
			throw InvalidInputException("redis_scan: send failed");
		}

		ScanClient.CheckedReadResponse(state.parser);

		// Get parsed objects (your API returns by value; fine for now).
		auto objects = state.parser.GetObjects();
		if (objects.empty()) {
			throw InvalidInputException("redis_scan: parsed 0 objects (incomplete/invalid RESP?)");
		}

		const RespObject &reply = objects.back();

		if (reply.type != RespType::ARRAY || reply.children.size() < 2) {
			throw InvalidInputException("redis_scan: unexpected SCAN reply shape (expected array[2])");
		}
		const RespObject &cursor_obj = reply.children[0];
		const RespObject &keys_obj = reply.children[1];

		std::string_view next_cursor_view = cursor_obj.AsString();
		state.cursor.assign(next_cursor_view.data(), next_cursor_view.size());

		if (keys_obj.type == RespType::ARRAY) {
			for (const auto &child : keys_obj.children) {
				auto sv = child.AsString();
				state.batch_keys.emplace_back(sv.data(), sv.size());
			}
		} else {
			throw InvalidInputException("redis_scan: keys element was not an array");
		}

		// SCAN is complete when cursor is "0"
		if (state.cursor == "0") {
			state.done = true;
		}

		// If we got keys, great — we can return them.
		if (!state.batch_keys.empty()) {
			return;
		}

		// If no keys and done == true, we are finished. Leave batch empty.
		if (state.done) {
			return;
		}

		// Otherwise: no keys but still not done, so reuse memory and try next cursor.
		state.parser.ClearObjects();
		ScanClient.ClearBuffer();
	}
}

unique_ptr<FunctionData> RedisScanBind(
    ClientContext &,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names
) {
	if (input.inputs.size() != 1) {
		throw InvalidInputException("redis_scan(pattern) expects exactly 1 argument");
	}
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("redis_scan(pattern) pattern cannot be NULL");
	}
	auto pattern = input.inputs[0].GetValue<std::string>();

	// Output schema: one VARCHAR column called key_name
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("key_name");

	return make_uniq<RedisScanBindData>(std::move(pattern));
}

unique_ptr<GlobalTableFunctionState> RedisScanInit(ClientContext &, TableFunctionInitInput &input) {
	auto state = make_uniq<RedisScanGlobalState>();
	auto &bind = input.bind_data->Cast<RedisScanBindData>();

	{
		std::scoped_lock<std::mutex> lock(scan_mutex);
		if (!ScanClient.Connect(ScanClient.host.c_str(), ScanClient.port)) {
			throw InvalidInputException("redis_scan: not connected (redis_connect failed)");
		}
	}

	// Start scan at cursor "0"
	state->cursor = "0";
	state->done = false;
	state->batch_keys.clear();
	state->batch_pos = 0;

	// fetch lazily in RedisScanFunc so we only hold buffer view for as long as needed for output.
	(void)bind;

	return std::move(state);
}

void RedisScanFunc(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	std::scoped_lock<std::mutex> lock(scan_mutex);
	auto &bind = data_p.bind_data->Cast<RedisScanBindData>();
	auto &state = data_p.global_state->Cast<RedisScanGlobalState>();

	// If we are done and no buffered keys remain, end scan.
	if (state.done && state.batch_pos >= (idx_t)state.batch_keys.size()) {
		output.SetCardinality(0);
		return;
	}

	// If current batch is exhausted, fetch the next batch from Redis.
	if (state.batch_pos >= (idx_t)state.batch_keys.size()) {
		FetchNextBatch(state, bind.pattern);

		// If fetch says done and produced no keys, end scan.
		if (state.batch_keys.empty() && state.done) {
			output.SetCardinality(0);
			return;
		}
	}

	// Produce up to STANDARD_VECTOR_SIZE rows from the current batch
	idx_t remaining = (idx_t)state.batch_keys.size() - state.batch_pos;
	idx_t count = std::min<idx_t>(STANDARD_VECTOR_SIZE, remaining);

	output.SetCardinality(count);

	auto &out_vector = output.data[0];
	out_vector.SetVectorType(VectorType::FLAT_VECTOR);
	auto out_data = FlatVector::GetData<string_t>(out_vector);

	for (idx_t i = 0; i < count; i++) {
		std::string_view key_view = state.batch_keys[state.batch_pos + i];
		out_data[i] = StringVector::AddString(out_vector, key_view.data(), key_view.size());
	}

	state.batch_pos += count;

	// If we finished the batch, it's now safe to reuse the Redis buffer.
	// We must also clear batch_keys so no dangling string_views remain in state.
	if (state.batch_pos >= (idx_t)state.batch_keys.size()) {
		state.batch_keys.clear();
		state.batch_pos = 0;

		state.parser.ClearObjects();
		ScanClient.ClearBuffer();
	}
}
// -------------------------------------------------------------------------------------------------
//  redis_get('key') scalar function
// -------------------------------------------------------------------------------------------------

inline void GetKeyScalarFun(DataChunk &args, ExpressionState &, Vector &result) {
	RespParser parser;                 // ok per-call
	auto &input_vector = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(
	    input_vector, result, args.size(),
	    [&](string_t key) {
		std::string_view sv;
		{
		    std::scoped_lock<std::mutex> lock(get_mutex);
		    sv = GetClient.RedisGet(key.GetString(), parser);
		    return StringVector::AddString(result, sv.data(), sv.size());
		}
	    }
	);
}


// -------------------------------------------------------------------------------------------------
//  SETUP
// -------------------------------------------------------------------------------------------------
static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto redduck_scalar_function = ScalarFunction("redduck", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RedduckScalarFun);
	auto set_name_scalar_function = ScalarFunction("set_name", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SetNameScalarFun);
	auto set_address_scalar_function = ScalarFunction("redis_connect", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SetAddressScalarFun);
	auto get_key_scalar_function = ScalarFunction("redis_get", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GetKeyScalarFun);
	// Register table functions
	TableFunction scan_func("redis_scan", {LogicalType::VARCHAR}, RedisScanFunc, RedisScanBind, RedisScanInit);

	loader.RegisterFunction(redduck_scalar_function);
	loader.RegisterFunction(set_name_scalar_function);
	loader.RegisterFunction(set_address_scalar_function);
	loader.RegisterFunction(get_key_scalar_function);
	loader.RegisterFunction(scan_func);
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
