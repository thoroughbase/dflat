#include "dflat.hpp"

namespace dflat {

Handle::Handle(bux::Client& client, std::string_view server_name,
               std::chrono::seconds timeout)
: server_name(server_name), timeout(timeout), client(client)
{
    client.AddHandler(DFLAT_RESPONSE, [this] (auto&, const bux::Message& msg) {
        if (!bux::ValidateJSON(msg.content, validate::RESPONSE))
            return;

        unsigned id = msg.content["request-id"];
        std::lock_guard<std::mutex> guard(queries_mutex);
        if (pending_queries.contains(id))
            pending_queries[id].promise.set_value(msg);
    });
}

auto Handle::CommandImpl(std::string_view database_name, std::string_view command,
                         const json& args, const bux::ValidationSeries& validation)
-> tb::result<json, DatabaseError>
{
    unsigned id;
    std::future<bux::Message> response;

    {
        std::lock_guard<std::mutex> guard(queries_mutex);
        id = request_id;
        pending_queries.emplace(request_id++, PendingResponse {});
        response = pending_queries[id].promise.get_future();
    }

    json msg_content = {
        { "cmd", command },
        { "database", database_name },
        { "request-id", id }
    };

    if (args.is_object())
        msg_content.update(args);

    auto error = client.Write({
        .dest = server_name,
        .type { DFLAT_QUERY },
        .content = std::move(msg_content),
        .only_first = true
    });

    tb::scoped_guard erase_query = [&] {
        std::lock_guard<std::mutex> guard(queries_mutex);
        pending_queries.erase(id);
    };

    if (error.is_error())
        return DatabaseError::NETWORK_ERROR;

    if (response.wait_for(timeout) == std::future_status::timeout)
        return DatabaseError::TIMEOUT;

    json content = std::move(response.get().content);
    if (!bux::ValidateJSON(content, validate::RESPONSE))
        return DatabaseError::INVALID_RESPONSE;

    if (content["error-code"] != DatabaseError::SUCCESS)
        return content["error-code"].get<DatabaseError>();

    if (!bux::ValidateJSON(content, validation))
        return DatabaseError::INVALID_RESPONSE;

    return content;
}

auto Handle::Create(std::string_view database_name, bool persist, size_t max_cache)
-> tb::error<DatabaseError>
{
    auto content_result = CommandImpl(database_name, "create",
        json {
            { "persist", persist },
            { "max-cache-items", max_cache }
        }, validate::CREATE_RESPONSE);

    if (content_result.is_error()) return content_result.get_error();

    return tb::ok;
}

auto Handle::ListDatabases() -> tb::result<std::vector<std::string>, DatabaseError>
{
    auto content_result = CommandImpl("dummy", CMD_DB_LIST, {},
        validate::DB_LIST_RESPONSE);

    if (content_result.is_error()) return content_result.get_error();

    return content_result.get_unchecked()["databases"].get<std::vector<std::string>>();
}

auto Handle::DeleteDatabase(std::string_view database_name)
-> tb::error<DatabaseError>
{
    auto content_result = CommandImpl(database_name, CMD_DB_DELETE, {},
        validate::DB_DELETE_RESPONSE);

    if (content_result.is_error()) return content_result.get_error();

    return tb::ok;
}

Database::Database(bux::Client& cl, std::string_view directory)
: storage_directory(directory), client(cl)
{
    client.AddHandler(DFLAT_QUERY, [&] (auto&, const bux::Message& msg) {
        if (!bux::ValidateJSON(msg.content, validate::COMMAND))
            return;

        std::string db_name = msg.content["database"];

        const auto make_msg_with = [&] (DatabaseError e, auto... args) {
            return bux::Message {
                .dest = msg.src,
                .type { DFLAT_RESPONSE },
                .content = {
                    { "error-code", e },
                    { "request-id", msg.content["request-id"] },
                    args...
                }
            };
        };

        if (bux::ValidateJSON(msg.content, validate::GET_QUERY)) {
            if (!databases.contains(db_name)) {
                client.Write(
                    make_msg_with(DatabaseError::DATABASE_NOT_FOUND)
                ).ignore_error();
                return;
            }

            json dict = json::object();
            json& eviction_queue = databases[db_name]["cache-eviction-queue"];
            json& entries = databases[db_name]["entries"];

            for (const json& key_json : msg.content["keys"]) {
                auto key = key_json.get<std::string_view>();
                // TODO: Currently only looks in cache
                if (entries.contains(key)) {
                    dict.emplace(key, entries[key]);
                    auto iter = eviction_queue.find(key);
                    if (iter != eviction_queue.end())
                        eviction_queue.erase(iter);
                    eviction_queue.emplace_back(key);
                }
            }

            client.Write(
                make_msg_with(DatabaseError::SUCCESS, json { "entries", dict })
            ).ignore_error();
        } else if (bux::ValidateJSON(msg.content, validate::PUT_QUERY)) {
            if (!databases.contains(db_name)) {
                client.Write(
                    make_msg_with(DatabaseError::DATABASE_NOT_FOUND)
                ).ignore_error();
                return;
            }

            bool replace = msg.content["replace"];
            json& eviction_queue = databases[db_name]["cache-eviction-queue"];
            json& entries = databases[db_name]["entries"];

            for (auto& [key, value] : msg.content["entries"].items()) {
                // TODO: Currently only looks in cache
                if (entries.contains(key) && replace)
                    entries.erase(key);

                entries.emplace(key, value);
                auto iter = eviction_queue.find(key);
                if (iter != eviction_queue.end())
                    eviction_queue.erase(iter);
                eviction_queue.emplace_back(key);
            }

            client.Write(make_msg_with(DatabaseError::SUCCESS)).ignore_error();

            size_t max_cache_items = databases[db_name]["max-cache-items"];

            if (eviction_queue.size() > max_cache_items) {
                auto start = eviction_queue.begin();
                auto end = start + eviction_queue.size() - max_cache_items;
                std::for_each(start, end, [&] (const json& j) {
                    entries.erase(j.get<std::string>());
                });
                eviction_queue.erase(start, end - 1);
            }
        } else if (bux::ValidateJSON(msg.content, validate::CREATE_QUERY)) {
            if (databases.contains(db_name)) {
                client.Write(
                    make_msg_with(DatabaseError::DATABASE_ALREADY_EXISTS)
                ).ignore_error();
                return;
            }

            databases.emplace(db_name, json {
                { "entries", json::object() },
                { "persist", msg.content["persist"] },
                { "cache-eviction-queue", json::array() },
                { "max-cache-items", msg.content["max-cache-items"] }
            });

            client.Write(make_msg_with(DatabaseError::SUCCESS)).ignore_error();
        } else if (bux::ValidateJSON(msg.content, validate::DB_LIST_QUERY)) {
            std::vector<std::string> names;
            names.reserve(databases.size());
            for (auto& [k, v] : databases.items())
                names.push_back(k);

            client.Write(
                make_msg_with(DatabaseError::SUCCESS, json { "databases", names })
            ).ignore_error();
        } else if (bux::ValidateJSON(msg.content, validate::DB_DELETE_QUERY)) {
            if (!databases.contains(db_name)) {
                client.Write(
                    make_msg_with(DatabaseError::DATABASE_NOT_FOUND)
                ).ignore_error();
                return;
            }

            databases.erase(db_name);
            client.Write(make_msg_with(DatabaseError::SUCCESS)).ignore_error();
        }
    });
}

}
