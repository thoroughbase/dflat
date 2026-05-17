#include "dflat.hpp"

namespace dflat
{

Handle::Handle(bux::Client& client, std::string_view server_name,
               std::chrono::seconds timeout)
: server_name(server_name), timeout(timeout), client(client)
{
    client.AddHandler(DFLAT_RESPONSE, [this] (auto&, const bux::Message& msg) {
        if (!bux::ValidateJSON(msg.content, validate::RESPONSE))
            return;

        unsigned id = msg.content["request-id"];
        std::scoped_lock queries_lock { queries_mutex };
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
        std::scoped_lock queries_lock { queries_mutex };
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

    auto write_message = [&] () -> tb::error<bux::WriteError> {
        std::scoped_lock write_lock { write_mutex };
        return client.Write({
            .dest = server_name,
            .type { DFLAT_QUERY },
            .content = std::move(msg_content),
            .only_first = true
        });
    };

    tb::scoped_guard erase_query = [&] {
        std::scoped_lock queries_lock { queries_mutex };
        pending_queries.erase(id);
    };

    if (write_message().is_error())
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

}
