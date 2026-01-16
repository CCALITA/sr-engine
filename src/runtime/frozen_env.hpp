#pragma once

#include <algorithm>
#include <concepts>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "engine/types.hpp"

namespace sr::engine {

namespace frozen_env::detail {

template <typename T>
concept LookupKey = std::same_as<std::remove_cvref_t<T>, std::string> ||
                    std::same_as<std::remove_cvref_t<T>, std::string_view>;

struct EnvEntry {
  std::string key;
  ValueBox value;

  [[nodiscard]] auto operator<(const EnvEntry &other) const noexcept -> bool {
    return key < other.key;
  }

  [[nodiscard]] auto less_than_key(std::string_view v) const noexcept -> bool {
    return key < v;
  }
};

} // namespace frozen_env::detail

class FrozenEnv {
  using Entry = frozen_env::detail::EnvEntry;

  std::vector<Entry> entries_;

  [[nodiscard]] auto do_find(std::string_view key) const noexcept -> const ValueBox * {
    const auto it = std::lower_bound(
        entries_.begin(), entries_.end(), key,
        [](const Entry &e, std::string_view v) { return e.key < v; });

    if (it != entries_.end() && it->key == key) {
      return &it->value;
    }
    return nullptr;
  }

public:
  FrozenEnv() = default;

  FrozenEnv(FrozenEnv &&) noexcept = default;
  FrozenEnv(const FrozenEnv &) = delete;
  auto operator=(FrozenEnv &&) noexcept -> FrozenEnv & = default;
  auto operator=(const FrozenEnv &) -> FrozenEnv & = delete;

  [[nodiscard]] auto empty() const noexcept -> bool { return entries_.empty(); }

  [[nodiscard]] auto size() const noexcept -> std::size_t { return entries_.size(); }

  template <frozen_env::detail::LookupKey K>
  [[nodiscard]] auto find(K &&key) const noexcept -> const ValueBox * {
    return do_find(std::forward<K>(key));
  }

  template <frozen_env::detail::LookupKey K>
  [[nodiscard]] auto contains(K &&key) const noexcept -> bool {
    return do_find(std::forward<K>(key)) != nullptr;
  }

  template <frozen_env::detail::LookupKey K>
  [[nodiscard]] auto at(K &&key) const -> const ValueBox & {
    if (const auto *ptr = do_find(std::forward<K>(key))) {
      return *ptr;
    }
    throw std::out_of_range(std::format("frozen_env: key not found '{}'", key));
  }

  template <frozen_env::detail::LookupKey K>
  [[nodiscard]] auto get(K &&key) const -> const ValueBox * {
    return do_find(std::forward<K>(key));
  }

  friend class FrozenEnvBuilder;

private:
  static auto build_from_context(RequestContext &ctx) -> FrozenEnv {
    FrozenEnv env;
    env.entries_.reserve(ctx.env.size());

    for (auto &kv : ctx.env) {
      env.entries_.push_back(Entry{std::move(kv.first), std::move(kv.second)});
    }

    std::sort(env.entries_.begin(), env.entries_.end(),
              [](const Entry &a, const Entry &b) { return a.key < b.key; });

    ctx.env.clear();
    return env;
  }

  friend auto prepare_env_from_context(RequestContext &ctx) -> FrozenEnv;
};

inline auto prepare_env_from_context(RequestContext &ctx) -> FrozenEnv {
  return FrozenEnv::build_from_context(ctx);
}

class FrozenEnvBuilder {
  std::vector<frozen_env::detail::EnvEntry> pending_;
  std::size_t capacity_ = 0;

public:
  explicit FrozenEnvBuilder(std::size_t reserve = 0) {
    if (reserve > 0) {
      pending_.reserve(reserve);
      capacity_ = reserve;
    }
  }

  auto reserve(std::size_t n) -> FrozenEnvBuilder & {
    if (n > capacity_) {
      pending_.reserve(n);
      capacity_ = n;
    }
    return *this;
  }

  auto insert(std::string key, ValueBox value) -> FrozenEnvBuilder & {
    pending_.push_back(frozen_env::detail::EnvEntry{std::move(key), std::move(value)});
    return *this;
  }

  auto insert(std::string key, ValueBox &&value) -> FrozenEnvBuilder & {
    pending_.push_back(frozen_env::detail::EnvEntry{std::move(key), std::move(value)});
    return *this;
  }

  template <std::input_iterator It>
  auto insert(It first, It last) -> FrozenEnvBuilder & {
    for (auto it = first; it != last; ++it) {
      pending_.push_back(std::move(*it));
    }
    return *this;
  }

  [[nodiscard]] auto build() && -> FrozenEnv {
    FrozenEnv env;
    env.entries_ = std::move(pending_);
    std::sort(env.entries_.begin(), env.entries_.end(),
              [](const frozen_env::detail::EnvEntry &a,
                 const frozen_env::detail::EnvEntry &b) { return a.key < b.key; });
    return env;
  }
};

} // namespace sr::engine
