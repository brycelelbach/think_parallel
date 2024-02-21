#include <execution>
#include <algorithm>
#include <vector>

int main() {
  std::vector<int> vec(100000, 1);
  std::for_each(std::execution::par_unseq,
    vec.begin(), vec.end(),
    [] (auto i) {
      throw int{};
    });
}
