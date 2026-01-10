#include "compat/quick_exit_compat.hpp"

#if defined(__APPLE__) && defined(__GNUC__) && !defined(__clang__)

#include <atomic>
#include <mutex>
#include <vector>

#include <unistd.h>

namespace sr::engine::compat {

namespace {
std::mutex quick_exit_mutex;
std::vector<void (*)(void)> quick_exit_handlers;
std::atomic<bool> quick_exit_started{false};
} // namespace

} // namespace sr::engine::compat

/// Register a quick-exit handler for libstdc++ on macOS.
auto at_quick_exit(void (*handler)(void)) -> int {
  if (!handler) {
    return -1;
  }
  std::lock_guard<std::mutex> lock(sr::engine::compat::quick_exit_mutex);
  if (sr::engine::compat::quick_exit_started.load(std::memory_order_acquire)) {
    return -1;
  }
  try {
    sr::engine::compat::quick_exit_handlers.push_back(handler);
  } catch (...) {
    return -1;
  }
  return 0;
}

/// Execute quick-exit handlers (LIFO) and terminate immediately.
auto quick_exit(int status) -> void {
  bool expected = false;
  if (!sr::engine::compat::quick_exit_started.compare_exchange_strong(
          expected, true, std::memory_order_acq_rel)) {
    _exit(status);
  }

  std::vector<void (*)(void)> handlers;
  {
    std::lock_guard<std::mutex> lock(sr::engine::compat::quick_exit_mutex);
    handlers = sr::engine::compat::quick_exit_handlers;
  }

  for (auto it = handlers.rbegin(); it != handlers.rend(); ++it) {
    if (*it) {
      (*it)();
    }
  }

  _exit(status);
}

#endif
