#define DUCKDB_EXTENSION_MAIN

#include "redduck_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void RedduckScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Redduck " + name.GetString() + " 🐥");
	});
}

inline void RedduckOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Redduck " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto redduck_scalar_function = ScalarFunction("redduck", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RedduckScalarFun);
	loader.RegisterFunction(redduck_scalar_function);

	// Register another scalar function
	auto redduck_openssl_version_scalar_function = ScalarFunction("redduck_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, RedduckOpenSSLVersionScalarFun);
	loader.RegisterFunction(redduck_openssl_version_scalar_function);
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
