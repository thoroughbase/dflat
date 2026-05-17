#pragma once

#include <buxtehude/buxtehude.hpp>

#include <nlohmann/json.hpp>

namespace dflat
{

namespace validate
{

namespace bux = buxtehude;

using nlohmann::json, nlohmann::json_pointer;

inline const auto IsValidCommand = bux::predicates::Matches({
    CMD_GET, CMD_PUT, CMD_CREATE, CMD_DB_LIST, CMD_DB_DELETE
});

constexpr auto IsStringArray = [] (const json& j) {
    if (!j.is_array()) return false;
    for (auto& element : j)
        if (!element.is_string()) return false;
    return true;
};

constexpr auto IsDict = [] (const json& j) {
    return j.is_object();
};

inline const bux::ValidationSeries COMMAND = {
    { "/cmd"_json_pointer, IsValidCommand },
    { "/database"_json_pointer, bux::predicates::NotEmpty },
    { "/request-id"_json_pointer, bux::predicates::IsNumber }
};

inline const bux::ValidationSeries RESPONSE = {
    { "/request-id"_json_pointer, bux::predicates::IsNumber },
    { "/error-code"_json_pointer, bux::predicates::IsNumber }
};

inline const bux::ValidationSeries GET_QUERY = {
    { "/cmd"_json_pointer, bux::predicates::Compare(CMD_GET) },
    { "/keys"_json_pointer, IsStringArray }
};

inline const bux::ValidationSeries GET_RESPONSE = {
    { "/entries"_json_pointer, IsDict }
};

inline const bux::ValidationSeries PUT_QUERY = {
    { "/cmd"_json_pointer, bux::predicates::Compare(CMD_PUT) },
    { "/entries"_json_pointer, IsDict },
    { "/replace"_json_pointer, bux::predicates::IsBool }
};

inline const bux::ValidationSeries PUT_RESPONSE = {};

inline const bux::ValidationSeries CREATE_QUERY = {
    { "/cmd"_json_pointer, bux::predicates::Compare(CMD_CREATE) },
    { "/persist"_json_pointer, bux::predicates::IsBool },
    { "/max-cache-items"_json_pointer, bux::predicates::IsNumber }
};

inline const bux::ValidationSeries CREATE_RESPONSE = {};

inline const bux::ValidationSeries DB_LIST_QUERY = {
    { "/cmd"_json_pointer, bux::predicates::Compare(CMD_DB_LIST) }
};

inline const bux::ValidationSeries DB_LIST_RESPONSE = {
    { "/databases"_json_pointer, IsStringArray }
};

inline const bux::ValidationSeries DB_DELETE_QUERY = {
    { "/cmd"_json_pointer, bux::predicates::Compare(CMD_DB_DELETE) },
};

inline const bux::ValidationSeries DB_DELETE_RESPONSE = {};

}

}
