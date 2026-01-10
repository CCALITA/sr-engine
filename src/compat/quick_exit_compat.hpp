#pragma once

#if defined(__APPLE__) && defined(__GNUC__) && !defined(__clang__) && \
    defined(__cplusplus)
extern "C" {
/// Register a callback for the GCC quick_exit shim on macOS.
auto at_quick_exit(void (*handler)(void)) -> int;
/// Invoke quick-exit handlers and terminate without flushing stdio.
auto quick_exit(int status) -> void;
}
#endif
