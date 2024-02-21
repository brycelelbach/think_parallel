#include <execution>
#include <algorithm>
#include <ranges>

int main() {
  auto rng = std::views::iota(0, 100000);
  std::for_each(std::execution::par_unseq,
    std::ranges::begin(rng), std::ranges::end(rng),
    [] (auto i) {
      throw int{};
    });
}
