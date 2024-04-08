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

