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
using stdr::size;

auto range_for_tile(stdr::range auto&& in, std::uint32_t tile, std::uint32_t num_tiles) {
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

  T get_predecessor_prefix(std::uint32_t i) {
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

void inclusive_scan(stdr::range auto&& in, std::uint32_t num_tiles) {
  scan_tile_state<stdr::range_value_t<decltype(in)>> sts(num_tiles);

  std::atomic<std::uint32_t> tile_counter(0);

  auto all_tiles = stdv::iota(0U, num_tiles);
  std::for_each(stde::par, begin(all_tiles), end(all_tiles),
    [&] (auto) {
      auto tile = tile_counter.fetch_add(1, std::memory_order_release);

      auto sub_in = range_for_tile(in, tile, num_tiles);

      sts.set_local_prefix(tile,
        *--std::inclusive_scan(begin(sub_in), end(sub_in), begin(sub_in)));

      if (tile != 0) {
        auto pred = sts.get_predecessor_prefix(tile);
        std::for_each(begin(sub_in), end(sub_in),
          [&] (auto& e) { e = pred + e; });
      }
    });
}

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

  std::cout << "num_elements, " << num_elements << "\n";
  std::cout << "num_tiles, " << num_tiles << "\n";
  std::cout << "validate, " << validate << "\n";

  std::vector<std::uint32_t> in(num_elements);

  auto all_tiles = stdv::iota(0U, num_tiles);
  std::for_each(stde::par, begin(all_tiles), end(all_tiles),
    [&] (std::uint32_t tile) {
      auto sub_in = range_for_tile(in, tile, num_tiles);

      std::minstd_rand gen(tile);
      std::uniform_int_distribution<std::uint32_t> dis(0, 100);

      stdr::generate(sub_in, [&] { return dis(gen); });
    });

  std::vector<std::uint32_t> gold;

  if (validate) {
    gold.resize(num_elements);
    std::inclusive_scan(begin(in), end(in), begin(gold));
  }

  auto start = std::chrono::high_resolution_clock::now();

  inclusive_scan(in, num_tiles);

  auto finish = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> diff = finish - start;
  std::cout << diff.count() << " [s]\n";

  if (validate) {
    if (!stdr::equal(in, gold))
      throw bool{};
  }
}

