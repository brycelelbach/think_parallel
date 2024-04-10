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

auto copy_if_three_pass = [] (stdr::range auto&& in,
                              auto&& out,
                              auto op,
                              std::uint32_t) {
  std::vector<std::uint8_t> flags(size(in));

  std::transform(stde::par, begin(in), end(in), begin(flags), op);

  std::vector<std::uint32_t> indices(size(in) + 1);

  std::transform_inclusive_scan(stde::par,
                                begin(flags), end(flags), begin(indices) + 1,
                                std::plus<>{},
                                [] (auto e) -> std::uint32_t
                                { return e; });

  auto zipped = stdv::zip(in, flags, indices);
  std::for_each(stde::par, begin(zipped), end(zipped),
    [&] (auto z) { auto [e, flag, index] = z;
      if (flag) out[index] = e;
    });

  return stdr::subrange(begin(out), next(begin(out), indices.back()));
};

auto copy_if_decoupled_lookback = [] (stdr::range auto&& in,
                                      auto&& out,
                                      auto op,
                                      std::uint32_t num_tiles) {
  scan_tile_state<stdr::range_value_t<decltype(in)>> sts(num_tiles);

  std::atomic<std::uint32_t> tile_counter(0);

  auto all_tiles = stdv::iota(0U, num_tiles);
  std::for_each(stde::par, begin(all_tiles), end(all_tiles),
    [&] (auto) {
      auto tile = tile_counter.fetch_add(1, std::memory_order_release);

      auto sub_in  = range_for_tile(in, tile, num_tiles);

      std::vector<std::uint8_t> flags(size(sub_in));
      stdr::transform(sub_in, begin(flags), op);

      std::vector<std::uint32_t> indices(size(sub_in) + 1);
      sts.set_local_prefix(tile,
        *--std::transform_inclusive_scan(begin(flags), end(flags),
                                         begin(indices) + 1,
                                         std::plus<>{},
                                         [] (auto e) -> std::uint32_t
                                         { return e; }));

      if (tile != 0) {
        auto pred = sts.wait_for_predecessor_prefix(tile);
        stdr::for_each(indices, [&] (auto& e) { e = pred + e; });
      }

      stdr::for_each(stdv::zip(sub_in, flags, indices),
        [&] (auto z) { auto [e, flag, index] = z;
          if (flag) out[index] = e;
        });
    });

  return stdr::subrange(begin(out),
    next(begin(out), sts.prefixes[num_tiles - 1].cumulative));
};

auto is_negative = [] (auto e) { return e < 0; };

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

  std::vector<std::int32_t> in(num_elements);
  auto all_tiles = stdv::iota(0U, num_tiles);
  std::for_each(stde::par, begin(all_tiles), end(all_tiles),
    [&] (std::uint32_t tile) {
      auto sub_in = range_for_tile(in, tile, num_tiles);

      std::minstd_rand gen(tile);
      std::uniform_int_distribution<std::int32_t> dis(-100, 100);

      stdr::generate(sub_in, [&] { return dis(gen); });
    });

  std::vector<std::int32_t> out(num_elements);

  std::vector<std::int32_t> gold;
  if (validate) {
    gold.resize(num_elements);
    auto end = stdr::copy_if(in, begin(gold), is_negative).out;
    gold.resize(distance(begin(gold), end));
  }

  std::cout << "Benchmark, Time [s]\n";

  auto benchmark = [&] (auto f, std::string_view name) {
    auto start = std::chrono::high_resolution_clock::now();

    auto res = f(in, out, is_negative, num_tiles);

    auto finish = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> diff = finish - start;
    std::cout << name << ", " << diff.count() << "\n";

    if (validate) {
      if (size(res) != size(gold))
        throw int{};

      if (!stdr::equal(res, gold)) {
        for (auto i : stdv::iota(0U, size(res)))
          std::cout << i << ": " << begin(res)[i] << " " << gold[i] << "\n";

        throw bool{};
      }
    }
  };

  #define BENCHMARK(f) benchmark(f, #f)

  BENCHMARK(copy_if_three_pass);
  BENCHMARK(copy_if_decoupled_lookback);
}

