#include "serialization/meta_expand.hpp"
#include "serialization/annotations.hpp"

#include <cstdio>
#include <meta>

struct base {
  virtual void hello() {}
  virtual int list() const { return 0; }
  virtual ~base() = default;
};

int main() {
  [:serialization::expand(std::meta::nonstatic_data_members_of(^^base, std::meta::access_context::unchecked())):] >> [&]<auto dm> {
    if constexpr (std::meta::has_identifier(dm)) {
      std::puts(std::meta::identifier_of(dm).data());
    } else {
      std::puts("(no id data)");
    }
  };
  [:serialization::expand(std::meta::members_of(^^base, std::meta::access_context::unchecked())):] >> [&]<auto member> {
    if constexpr (std::meta::is_function(member) && std::meta::has_identifier(member)) {
      std::puts(std::meta::identifier_of(member).data());
    } else {
      std::puts("(other member)");
    }
  };
}
