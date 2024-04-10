#include <vector>
#include <ranges>
#include <algorithm>
#include <numeric>
#include <execution>
#include <atomic>
#include <random>
#include <chrono>
#include <iostream>

namespace stdr = std::ranges;
namespace stdv = std::views;
namespace stde = std::execution;

using stdr::begin;
using stdr::end;
using stdr::next;
using stdr::size;
using stdr::distance;

auto range_for_tile(stdr::range auto&& in,
                    std::uint32_t tile,
                    std::uint32_t num_tiles) {
  auto tile_size = (size(in) + num_tiles - 1) / num_tiles;
  auto start     = std::min(tile * tile_size, size(in));
  auto end       = std::min((tile + 1) * tile_size, size(in));
  return stdr::subrange(next(begin(in), start), next(begin(in), end));
}

template <typename T>
struct scan_tile_state {
  enum status {
    status_unavailable,
    status_local,
    status_cumulative
  };

  struct descriptor {
    T local = {};
    T cumulative = {};
    std::atomic<status> state = status_unavailable;
  };

  std::vector<descriptor> prefixes;

  scan_tile_state(std::uint32_t num_tiles) : prefixes(num_tiles) {}

  void set_local_prefix(std::uint32_t i, T local) {
    if (i == 0) {
      prefixes[i].local = local;
      prefixes[i].cumulative = local;
      prefixes[i].state.store(status_cumulative,
                              std::memory_order_release);
    } else {
      prefixes[i].local = local;
      prefixes[i].state.store(status_local,
                              std::memory_order_release);
    }
    prefixes[i].state.notify_all();
  }

  T wait_for_predecessor_prefix(std::uint32_t i) {
    T predecessor_prefix = {};
    for (auto p = i - 1; p >= 0; --p) {
      auto state = prefixes[p].state.load(std::memory_order_acquire);
      while (state == status_unavailable) {
        prefixes[p].state.wait(status_unavailable,
                               std::memory_order_acquire);
        state = prefixes[p].state.load(std::memory_order_acquire);
      }
      if (state == status_local) {
        predecessor_prefix = prefixes[p].local
                           + predecessor_prefix;
      } else if (state == status_cumulative) {
        predecessor_prefix = prefixes[p].cumulative
                           + predecessor_prefix;
        break;
      }
    }

    prefixes[i].cumulative = predecessor_prefix
                           + prefixes[i].local;
    prefixes[i].state.store(status_cumulative,
                            std::memory_order_release);
    prefixes[i].state.notify_all();

    return predecessor_prefix;
  }
};

auto chunk_by_three_pass = [] (stdr::range auto&& in,
                               auto&& out,
                               auto op,
                               std::uint32_t) {
  struct interval {
    bool flag;
    std::uint32_t index;
    std::uint32_t start;
    std::uint32_t end;
  };

  std::vector<interval> intervals(size(in) + 1);

  intervals[0] = interval{true, 0, 0, 1};

  auto adj_in = in | stdv::adjacent<2>;
  std::transform(stde::par, begin(adj_in), end(adj_in), begin(intervals) + 1,
    [&] (auto lr) { auto [l, r] = lr;
      return interval{op(l, r), 0, 0, 1};
    });

  intervals[size(in)] = interval{false, 0, 0, 1};

  std::inclusive_scan(stde::par,
    begin(intervals), end(intervals),
    begin(intervals),
    [] (auto l, auto r) {
      return interval{r.flag,
                      r.flag ? l.index + r.index : l.index + r.index + 1,
                      r.flag ? l.start + r.start : l.end,
                      l.end + r.end};
    });

  auto adj_intervals = intervals | stdv::adjacent<2>;
  std::for_each(stde::par, begin(adj_intervals), end(adj_intervals),
    [&] (auto lr) { auto [l, r] = lr;
      if (!r.flag)
        out[l.index] = stdr::subrange(next(begin(in), l.start),
                                      next(begin(in), l.end));
    });

  return stdr::subrange(begin(out), next(begin(out), intervals.back().index));
};

auto is_space = [] (auto l, auto r) { return !(l == ' ' || r == ' '); };

int main(int argc, char** argv) {
  std::uint32_t num_elements = 1024 * 1024 * 1024;
  std::uint32_t num_tiles = 1024;
  bool validate = true;

  if (argc > 1)
    num_elements = std::stoul(argv[1]);
  if (argc > 2)
    num_tiles = std::stoul(argv[2]);
  if (argc > 3)
    validate = std::string_view("true") == std::string_view(argv[3]);

  std::cout << "Number of Elements, " << num_elements << "\n";
  std::cout << "Number of Tiles, " << num_tiles << "\n";
  std::cout << "Validate, " << std::boolalpha << validate << "\n";
  std::cout << "\n";

  std::vector<char> in(num_elements);
  auto all_tiles = stdv::iota(0U, num_tiles);
  std::for_each(stde::par, begin(all_tiles), end(all_tiles),
    [&] (std::uint32_t tile) {
      auto sub_in = range_for_tile(in, tile, num_tiles);

      std::minstd_rand gen(tile);
      std::uniform_int_distribution<std::uint8_t> dis(0, 26);

      constexpr std::string_view charset = "abcdefghijklmnopqrstuvxyz ";
      stdr::generate(sub_in, [&] { return charset[dis(gen)]; });
    });

  std::vector<stdr::subrange<std::vector<char>::iterator>> out(num_elements);
  std::vector<stdr::subrange<std::vector<char>::iterator>> gold;
  if (validate) {
    gold.resize(num_elements);
    auto end = stdr::copy(in | stdv::chunk_by(is_space), begin(gold)).out;
    gold.resize(distance(begin(gold), end));
  }

  std::cout << "Benchmark, Time [s]\n";

  auto benchmark = [&] (auto f, std::string_view name) {
    auto start = std::chrono::high_resolution_clock::now();

    auto res = f(in, out, is_space, num_tiles);

    auto finish = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> diff = finish - start;
    std::cout << name << ", " << diff.count() << "\n";

    if (validate) {
      static_assert(std::same_as<stdr::range_value_t<decltype(res)>,
                                 stdr::range_value_t<decltype(gold)>>);

      if (size(res) != size(gold))
        throw int{};

      if (!stdr::equal(res, gold, [] (auto&& l, auto&& r)
                                  { return stdr::equal(l, r); }))
        throw bool{};
    }
  };

  #define BENCHMARK(f) benchmark(f, #f)

  BENCHMARK(chunk_by_three_pass);
}
