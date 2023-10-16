#include "duckdb.hpp"
#include "duckdb_node.hpp"
#include "napi.h"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include <iostream>
#include <thread>

namespace node_duckdb {

Napi::FunctionReference Connection::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t = DefineClass(
	    env, "Connection",
	    {InstanceMethod("prepare", &Connection::Prepare), InstanceMethod("exec", &Connection::Exec),
	      InstanceMethod("close", &Connection::Close)});

	exports.Set("Connection", t);

	return Napi::Persistent(t);
}

struct ConnectTask : public Task {
	ConnectTask(Connection &connection, Napi::Function callback) : Task(connection, callback) {
	}

	void DoWork() override {
		auto &connection = Get<Connection>();
		if (!connection.database_ref || !connection.database_ref->database) {
			return;
		}
		connection.connection = duckdb::make_uniq<duckdb::Connection>(*connection.database_ref->database);
		success = true;
	}
	void Callback() override {
		auto &connection = Get<Connection>();
		Napi::Env env = connection.Env();

		vector<napi_value> args;
		if (!success) {
			args.push_back(Utils::CreateError(env, "Invalid database object"));
		} else {
			args.push_back(env.Null());
		}

		Napi::HandleScope scope(env);

		callback.Value().MakeCallback(connection.Value(), args);
	}

	bool success = false;
};

struct NodeReplacementScanData : duckdb::ReplacementScanData {
	NodeReplacementScanData(Connection *con_p) : connection_ref(con_p) {};
	Connection *connection_ref;
};

Connection::Connection(const Napi::CallbackInfo &info) : Napi::ObjectWrap<Connection>(info) {
	Napi::Env env = info.Env();
	int length = info.Length();

	if (length <= 0 || !Database::HasInstance(info[0])) {
		throw Napi::TypeError::New(env, "Database object expected");
	}

	database_ref = Napi::ObjectWrap<Database>::Unwrap(info[0].As<Napi::Object>());
	database_ref->Ref();

	Napi::Function callback;
	if (info.Length() > 0 && info[1].IsFunction()) {
		callback = info[1].As<Napi::Function>();
	}

	database_ref->Schedule(env, duckdb::make_uniq<ConnectTask>(*this, callback));
}

Connection::~Connection() {
	database_ref->Unref();
	database_ref = nullptr;
}

Napi::Value Connection::Prepare(const Napi::CallbackInfo &info) {
	vector<napi_value> args;
	// push the connection as first argument
	args.push_back(Value());
	// we need to pass all the arguments onward to statement
	for (size_t i = 0; i < info.Length(); i++) {
		args.push_back(info[i]);
	}
	auto obj = Statement::NewInstance(info.Env(), args);
	auto res = Statement::Unwrap(obj);
	res->SetProcessFirstParam();
	return res->Value();
}

struct ExecTask : public Task {
	ExecTask(Connection &connection, std::string sql, Napi::Function callback)
	    : Task(connection, callback), sql(std::move(sql)) {
	}

	void DoWork() override {
		auto &connection = Get<Connection>();

		success = true;
		try {
			auto statements = connection.connection->ExtractStatements(sql);
			if (statements.empty()) {
				return;
			}

			for (duckdb::idx_t i = 0; i < statements.size(); i++) {
				auto res = connection.connection->Query(std::move(statements[i]));
				if (res->HasError()) {
					success = false;
					error = res->GetErrorObject();
					break;
				}
			}
		} catch (duckdb::ParserException &e) {
			success = false;
			error = duckdb::PreservedError(e);
			return;
		}
	}

	void Callback() override {
		auto env = object.Env();
		Napi::HandleScope scope(env);
		callback.Value().MakeCallback(object.Value(), {success ? env.Null() : Utils::CreateError(env, error)});
	};

	std::string sql;
	bool success;
	duckdb::PreservedError error;
};

struct ExecTaskWithCallback : public ExecTask {
	ExecTaskWithCallback(Connection &connection, std::string sql, Napi::Function js_callback,
	                     std::function<void(void)> cpp_callback)
	    : ExecTask(connection, sql, js_callback), cpp_callback(cpp_callback) {
	}

	void Callback() override {
		cpp_callback();
		ExecTask::Callback();
	};

	std::function<void(void)> cpp_callback;
};

struct CloseConnectionTask : public Task {
	CloseConnectionTask(Connection &connection, Napi::Function callback) : Task(connection, callback) {
	}

	void DoWork() override {
		auto &connection = Get<Connection>();
		if (connection.connection) {
			connection.connection.reset();
			success = true;
		} else {
			success = false;
		}
	}

	void Callback() override {
		auto &connection = Get<Connection>();
		auto env = connection.Env();
		Napi::HandleScope scope(env);

		auto cb = callback.Value();
		if (!success) {
			cb.MakeCallback(connection.Value(), {Utils::CreateError(env, "Connection was already closed")});
			return;
		}
		cb.MakeCallback(connection.Value(), {env.Null(), connection.Value()});
	}

	bool success = false;
};

Napi::Value Connection::Exec(const Napi::CallbackInfo &info) {
	auto env = info.Env();

	if (info.Length() < 1 || !info[0].IsString()) {
		throw Napi::TypeError::New(env, "SQL query expected");
	}

	std::string sql = info[0].As<Napi::String>();

	Napi::Function callback;
	if (info.Length() > 0 && info[1].IsFunction()) {
		callback = info[1].As<Napi::Function>();
	}

	database_ref->Schedule(info.Env(), duckdb::make_uniq<ExecTask>(*this, sql, callback));
	return Value();
}

Napi::Value Connection::Close(const Napi::CallbackInfo &info) {
	Napi::Function callback;
	if (info.Length() > 0 && info[0].IsFunction()) {
		callback = info[0].As<Napi::Function>();
	}

	database_ref->Schedule(info.Env(), duckdb::make_uniq<CloseConnectionTask>(*this, callback));

	return info.Env().Undefined();
}

Napi::Object Connection::NewInstance(const Napi::Value &db) {
	return NodeDuckDB::GetData(db.Env())->connection_constructor.New({db});
}

} // namespace node_duckdb
