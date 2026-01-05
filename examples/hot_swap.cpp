#include <charconv>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <format>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#ifdef __linux__
#if __has_include(<linux/io_uring.h>)
#define SR_HAVE_IO_URING 1
#include "exec/linux/io_uring_context.hpp"
#include "stdexec/execution.hpp"
#else
#define SR_HAVE_IO_URING 0
#endif
#else
#define SR_HAVE_IO_URING 0
#endif

#include "engine/types.hpp"
#include "kernel/sample_kernels.hpp"
#include "runtime/runtime.hpp"

namespace {

// Holds a parsed env key/value pair supplied on the command line.
struct EnvBinding {
  std::string key;
  sr::engine::Json value;
};

// Captures CLI options for the hot-swap demo.
struct Options {
  std::string path;
  int runs = 0;
  int poll_ms = 200;
  bool allow_replace = false;
  std::vector<EnvBinding> env;
};

#ifdef __linux__
#if !SR_HAVE_IO_URING
#error "sr_engine_hot_swap requires io_uring support on Linux."
#endif

class LinuxTimer {
 public:
  LinuxTimer() {
    io_thread_ = std::thread([this] { context_.run_until_stopped(); });
    while (!context_.is_running()) {
      std::this_thread::yield();
    }
  }

  LinuxTimer(const LinuxTimer&) = delete;
  auto operator=(const LinuxTimer&) -> LinuxTimer& = delete;

  ~LinuxTimer() {
    context_.request_stop();
    if (io_thread_.joinable()) {
      io_thread_.join();
    }
  }

  auto sleep_for(std::chrono::milliseconds duration) -> void {
    auto scheduler = context_.get_scheduler();
    (void)stdexec::sync_wait(exec::schedule_after(scheduler, duration));
  }

 private:
  exec::io_uring_context context_;
  std::thread io_thread_;
};
#endif

// Prints CLI usage for the demo.
auto print_usage(std::ostream& out) -> void {
  out << "Usage: sr_engine_hot_swap <dsl_path> [--runs=N] [--poll-ms=M] [--allow-replace] [--env key=value]\n";
  out << "  --runs=N        Number of runs before exit (0 means loop forever)\n";
  out << "  --poll-ms=M     File poll interval in milliseconds\n";
  out << "  --allow-replace Allow overwriting an existing version with a new hash\n";
  out << "  --env key=value Provide request env values (JSON literals or raw strings)\n";
}

// Parses a decimal integer into an int.
auto parse_int(std::string_view text, int& value) -> bool {
  int parsed = 0;
  auto result = std::from_chars(text.data(), text.data() + text.size(), parsed);
  if (result.ec != std::errc{}) {
    return false;
  }
  value = parsed;
  return true;
}

// Parses "key=value" into an env binding, interpreting value as JSON if possible.
auto parse_env_binding(std::string_view text) -> std::optional<EnvBinding> {
  auto pos = text.find('=');
  if (pos == std::string_view::npos || pos == 0) {
    return std::nullopt;
  }
  EnvBinding binding;
  binding.key = std::string(text.substr(0, pos));
  std::string_view value_text = text.substr(pos + 1);
  try {
    binding.value = sr::engine::Json::parse(value_text);
  } catch (const std::exception&) {
    binding.value = std::string(value_text);
  }
  return binding;
}

// Parses CLI args into Options, returning nullopt on usage errors.
auto parse_args(int argc, char** argv) -> std::optional<Options> {
  if (argc < 2) {
    return std::nullopt;
  }
  Options options;
  options.path = argv[1];
  for (int i = 2; i < argc; ++i) {
    std::string_view arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      return std::nullopt;
    }
    if (arg == "--allow-replace") {
      options.allow_replace = true;
      continue;
    }
    if (arg.rfind("--runs=", 0) == 0) {
      int runs = 0;
      if (!parse_int(arg.substr(7), runs)) {
        return std::nullopt;
      }
      options.runs = runs;
      continue;
    }
    if (arg.rfind("--poll-ms=", 0) == 0) {
      int poll = 0;
      if (!parse_int(arg.substr(10), poll)) {
        return std::nullopt;
      }
      options.poll_ms = poll;
      continue;
    }
    if (arg.rfind("--env=", 0) == 0) {
      auto binding = parse_env_binding(arg.substr(6));
      if (!binding) {
        return std::nullopt;
      }
      options.env.push_back(std::move(*binding));
      continue;
    }
    if (arg == "--env") {
      if (i + 1 >= argc) {
        return std::nullopt;
      }
      auto binding = parse_env_binding(argv[++i]);
      if (!binding) {
        return std::nullopt;
      }
      options.env.push_back(std::move(*binding));
      continue;
    }
    return std::nullopt;
  }
  return options;
}

// Copies CLI env values into the request context using registered types.
auto apply_env(sr::engine::RequestContext& ctx, const std::vector<EnvBinding>& env)
  -> sr::engine::Expected<void> {
  for (const auto& binding : env) {
    const auto& value = binding.value;
    if (value.is_boolean()) {
      ctx.set_env<bool>(binding.key, value.get<bool>());
    } else if (value.is_number_integer() || value.is_number_unsigned()) {
      ctx.set_env<int64_t>(binding.key, value.get<int64_t>());
    } else if (value.is_number_float()) {
      ctx.set_env<double>(binding.key, value.get<double>());
    } else if (value.is_string()) {
      ctx.set_env<std::string>(binding.key, value.get<std::string>());
    } else {
      return tl::unexpected(sr::engine::make_error(
        std::format("unsupported env type for key: {}", binding.key)));
    }
  }
  return {};
}

// Prints basic output values for the demo.
auto print_outputs(const sr::engine::ExecResult& result) -> void {
  auto int_type = entt::resolve<int64_t>();
  auto double_type = entt::resolve<double>();
  auto bool_type = entt::resolve<bool>();
  auto string_type = entt::resolve<std::string>();
  for (const auto& [name, slot] : result.outputs) {
    if (!slot.has_value()) {
      std::cout << std::format("{}=<missing>\n", name);
      continue;
    }
    if (slot.type == int_type) {
      std::cout << std::format("{}={}\n", name, slot.get<int64_t>());
    } else if (slot.type == double_type) {
      std::cout << std::format("{}={}\n", name, slot.get<double>());
    } else if (slot.type == bool_type) {
      std::cout << std::format("{}={}\n", name, slot.get<bool>());
    } else if (slot.type == string_type) {
      std::cout << std::format("{}={}\n", name, slot.get<std::string>());
    } else {
      std::cout << std::format("{}=<opaque>\n", name);
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  auto options = parse_args(argc, argv);
  if (!options) {
    print_usage(std::cerr);
    return 1;
  }

  sr::kernel::register_builtin_types();

  sr::engine::Runtime runtime;
  sr::kernel::register_sample_kernels(runtime.registry());

  sr::engine::StageOptions stage_options;
  stage_options.publish = true;
  stage_options.allow_replace = options->allow_replace;
  stage_options.source = options->path;

  auto snapshot = runtime.stage_file(options->path, stage_options);
  if (!snapshot) {
    std::cerr << std::format("Stage error: {}\n", snapshot.error().message);
    return 1;
  }
  std::string active_name = (*snapshot)->key.name;

  std::filesystem::file_time_type last_write{};
  std::error_code fs_error;
  last_write = std::filesystem::last_write_time(options->path, fs_error);
  if (fs_error) {
    std::cerr << std::format("Warning: failed to stat file: {}\n", fs_error.message());
  }

#ifdef __linux__
  LinuxTimer timer;
#endif

  int run_index = 0;
  while (options->runs <= 0 || run_index < options->runs) {
    bool changed = false;
    fs_error.clear();
    auto current_write = std::filesystem::last_write_time(options->path, fs_error);
    if (!fs_error && current_write != last_write) {
      changed = true;
      last_write = current_write;
    }

    if (changed) {
      auto update = runtime.stage_file(options->path, stage_options);
      if (!update) {
        std::cerr << std::format("Hot-swap error: {}\n", update.error().message);
      } else {
        active_name = (*update)->key.name;
        std::cout << std::format("Published {} v{}\n", active_name, (*update)->key.version);
      }
    }

    sr::engine::RequestContext ctx;
    if (auto env_result = apply_env(ctx, options->env); !env_result) {
      std::cerr << std::format("Env error: {}\n", env_result.error().message);
      return 1;
    }

    auto result = runtime.run(active_name, ctx);
    if (!result) {
      std::cerr << std::format("Run error: {}\n", result.error().message);
      return 1;
    }

    print_outputs(*result);
    run_index += 1;

#ifdef __linux__
    if (options->poll_ms > 0) {
      timer.sleep_for(std::chrono::milliseconds(options->poll_ms));
    }
#else
    if (options->poll_ms > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(options->poll_ms));
    }
#endif
  }

  return 0;
}
