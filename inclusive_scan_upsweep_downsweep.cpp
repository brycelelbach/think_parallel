#include <vector>
#include <ranges>
#include <algorithm>
#include <numeric>
#include <execution>
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

void inclusive_scan(stdr::range auto&& in, std::uint32_t num_tiles) {
  std::vector<stdr::range_value_t<decltype(in)>> partials(num_tiles);

  auto all_tiles = stdv::iota(0U, num_tiles);
  std::for_each(stde::par_unseq, begin(all_tiles), end(all_tiles),
    [&] (std::uint32_t tile) {
      auto sub_in = range_for_tile(in, tile, num_tiles);
      partials[tile] = *--std::inclusive_scan(begin(sub_in), end(sub_in), begin(sub_in));
    });

  std::inclusive_scan(begin(partials), end(partials), begin(partials));

  auto subsequent_tiles = stdv::iota(1U, num_tiles);
  std::for_each(stde::par_unseq, begin(subsequent_tiles), end(subsequent_tiles),
    [&] (std::uint32_t tile) {
      auto sub_in = range_for_tile(in, tile, num_tiles);
      stdr::for_each(sub_in, [&] (auto& e) { e = partials[tile - 1] + e; });
    });
}

int main() {
  constexpr auto N = 1024 * 1024 * 1024;

  std::mt19937_64 gen(1337);
  std::uniform_int_distribution<std::uint32_t> dis(0, 100);

  std::vector<std::uint32_t> in(N);
  stdr::generate(in, [&] { return dis(gen); });

  std::vector<std::uint32_t> gold(N);
  std::inclusive_scan(begin(in), end(in), begin(gold));

  auto start = std::chrono::high_resolution_clock::now();

  inclusive_scan(in, 1024);

  auto end = std::chrono::high_resolution_clock::now();

  if (!stdr::equal(in, gold))
    throw bool{};

  std::chrono::duration<double> diff = end - start;
  std::cout << diff.count() << " [s]\n";
}

