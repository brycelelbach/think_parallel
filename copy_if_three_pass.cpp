#include <vector>
#include <ranges>
#include <algorithm>
#include <numeric>
#include <execution>
#include <chrono>
#include <iostream>

namespace stdr = std::ranges;
namespace stdv = std::views;
namespace stde = std::execution;

using stdr::begin;
using stdr::end;
using stdr::size;

auto copy_if(stdr::range auto&& in, auto out, auto pred) {
  std::vector<std::byte> flags(size(in));

  std::transform(stde::par, begin(in), end(in), begin(flags), pred);

  std::vector<std::uint32_t> indices(size(in) + 1);

  std::exclusive_scan(stde::par, begin(flags), end(flags), begin(indices), 0);

  auto zipped = stdv::zip(in, flags, indices);
  std::for_each(stde::par, begin(zipped), end(zipped)
    [&in] (auto z) {
      auto [e, flag, index] = z;
      if (flag) in[index] = e;
    });

  return stdr::subrange(begin(in), next(in, indices.back()));
}

int main() {
  constexpr auto N = 1024 * 1024 * 1024;

  constexpr auto is_even = [] (auto e) { return e % 2; };

  std::vector<std::uint32_t> in(N);
  std::iota(begin(in), end(in), 0);

  std::vector<std::uint32_t> gold(N);
  std::copy_if(begin(in), end(in), begin(gold), is_even);

  auto start = std::chrono::high_resolution_clock::now();

  copy_if(in, begin(in), is_even);

  auto end = std::chrono::high_resolution_clock::now();

  if (!stdr::equal(in, gold))
    throw bool{};

  std::chrono::duration<double> diff = end - start;
  std::cout << diff.count() << " [s]\n";
}

