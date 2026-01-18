#include "common/logging/log.hpp"

#include <gflags/gflags.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

DECLARE_string(log_level);
DECLARE_string(log_file);
DECLARE_int32(log_max_size);
DECLARE_int32(log_max_files);

namespace {

std::shared_ptr<spdlog::sinks::sink> create_file_sink(const std::string& file_path,
                                                      size_t max_size,
                                                      int max_files) {
  if (max_files < 1) {
    max_files = 1;
  }
  if (max_size < 1024) {
    max_size = 1024;
  }
  return std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      file_path, max_size, max_files);
}

auto parse_log_level(const std::string& level) -> spdlog::level::level_enum {
  if (level == "trace") return spdlog::level::trace;
  if (level == "debug") return spdlog::level::debug;
  if (level == "info") return spdlog::level::info;
  if (level == "warn") return spdlog::level::warn;
  if (level == "error") return spdlog::level::err;
  if (level == "critical") return spdlog::level::critical;
  if (level == "off") return spdlog::level::off;
  return spdlog::level::info;
}

}  // namespace

namespace sr::log {

namespace {
  std::shared_ptr<spdlog::async_logger> g_logger;
  bool g_initialized = false;
}

void init() {
  if (g_initialized) {
    return;
  }

  const std::string log_file = FLAGS_log_file;
  const size_t max_size = static_cast<size_t>(FLAGS_log_max_size);
  const int max_files = FLAGS_log_max_files;

  spdlog::init_thread_pool(8192, 1);

  std::vector<std::shared_ptr<spdlog::sinks::sink>> sinks;

  auto file_sink = create_file_sink(log_file, max_size, max_files);
  sinks.push_back(file_sink);

  const auto level = parse_log_level(FLAGS_log_level);
  file_sink->set_level(level);

  g_logger = std::make_shared<spdlog::async_logger>(
      "sr_engine", sinks.begin(), sinks.end(), spdlog::thread_pool(),
      spdlog::async_overflow_policy::block);

  spdlog::set_default_logger(g_logger);
  spdlog::set_level(level);
  spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");

  g_initialized = true;
  spdlog::info("Logger initialized: file={}, level={}", log_file, FLAGS_log_level);
}

void shutdown() {
  if (g_logger) {
    g_logger->flush();
    spdlog::shutdown();
    g_initialized = false;
  }
}

void info(std::string_view event, std::unordered_map<std::string, std::string> fields) {
  std::string msg{event};
  for (const auto& [key, value] : fields) {
    msg += " " + key + "=" + value;
  }
  spdlog::info(msg);
}

}  // namespace sr::log
