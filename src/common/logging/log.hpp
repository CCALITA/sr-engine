#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <memory>
#include <string>
#include <unordered_map>

namespace sr::log {

using spdlog::trace;
using spdlog::debug;
using spdlog::info;
using spdlog::warn;
using spdlog::error;
using spdlog::critical;

void init();

void shutdown();

void info(std::string_view event, std::unordered_map<std::string, std::string> fields);

}  // namespace sr::log
