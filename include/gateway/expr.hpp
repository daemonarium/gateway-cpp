#pragma once

#include "gateway/types.hpp"

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

namespace waos {
  namespace gateway {

    enum class expr_kind : std::uint64_t {
      ref = 0,
      literal = 1,
      member = 2,
      call = 3,
      switch_ = 4,
      non_empty = 5
    };

    enum class literal_kind : std::uint64_t {
      uint64 = 0,
      string = 1,
      raw = 2,
      boolean = 3
    };

    // Decoded literal carried to an endpoint. Not a wire node.
    struct literal_expr {
      literal_kind kind = literal_kind::uint64;
      binary value;
    };

    // Polymorphic expression. The wire form is shared_ptr<expr_node>:
    // flat deferred bodies carry a type index of the concrete node.
    // The base stays a plain object so it is not a typed-codec slot;
    // only the concrete nodes are dynamic.
    struct expr_node {
      virtual ~expr_node() = default;
    };

    using expr = std::shared_ptr<expr_node>;

    struct [[=serialization::dynamic()]] ref_expr_node : expr_node {
      std::uint64_t ref_id = 0;
    };

    struct [[=serialization::dynamic()]] literal_expr_node : expr_node {
      literal_kind kind = literal_kind::uint64;
      binary value;
    };

    struct [[=serialization::dynamic()]] member_expr_node : expr_node {
      expr inner;
      std::uint32_t member_id = 0;
    };

    struct [[=serialization::dynamic()]] call_expr_node : expr_node {
      std::uint32_t api_id = 0;
      std::uint64_t function_id = 0;
      std::vector<expr> args;
    };

    struct switch_case {
      std::uint64_t key = 0;
      expr branch;
    };

    struct [[=serialization::dynamic()]] switch_expr_node : expr_node {
      expr discriminant;
      std::vector<switch_case> cases;
      expr default_branch;
    };

    struct [[=serialization::dynamic()]] non_empty_expr_node : expr_node {
      expr inner;
    };

    struct direct_call {
      std::uint32_t api_id = 0;
      std::uint64_t function_id = 0;
      std::vector<literal_expr> literals;
    };

    inline binary u64_to_le(std::uint64_t value) {
      binary out(8);
      for(int i = 0; i < 8; ++i) {
        out[static_cast<std::size_t>(i)] = static_cast<std::uint8_t>(value >> (8 * i));
      }
      return out;
    }

    inline std::uint64_t u64_from_le(const binary& bytes) {
      if(bytes.size() < 8) {
        return 0;
      }
      std::uint64_t value = 0;
      std::memcpy(&value, bytes.data(), sizeof(value));
      return value;
    }

    inline expr make_call(std::uint32_t api_id, std::uint64_t function_id, std::vector<expr> args) {
      auto call = std::make_shared<call_expr_node>();
      call->api_id = api_id;
      call->function_id = function_id;
      call->args = std::move(args);
      return call;
    }

    inline expr make_literal_u64(std::uint64_t value) {
      auto lit = std::make_shared<literal_expr_node>();
      lit->kind = literal_kind::uint64;
      lit->value = u64_to_le(value);
      return lit;
    }

    inline expr make_ref(std::uint64_t ref_id) {
      auto ref = std::make_shared<ref_expr_node>();
      ref->ref_id = ref_id;
      return ref;
    }

    inline expr make_member(expr inner, std::uint32_t member_id) {
      auto member = std::make_shared<member_expr_node>();
      member->inner = std::move(inner);
      member->member_id = member_id;
      return member;
    }

    inline expr make_switch(expr discriminant, std::vector<switch_case> cases, expr default_branch = nullptr) {
      auto node = std::make_shared<switch_expr_node>();
      node->discriminant = std::move(discriminant);
      node->cases = std::move(cases);
      node->default_branch = std::move(default_branch);
      return node;
    }

    // v1.5: Call(api, fn, Literal*) only.
    // Ref as the root is unknown_expr. Ref as a Call argument is dependent_ref_not_supported.
    inline direct_call validate_direct_expr(const expr& value) {
      if(!value || dynamic_cast<ref_expr_node*>(value.get())) {
        throw std::runtime_error(error_unknown_expr);
      }
      auto* call = dynamic_cast<call_expr_node*>(value.get());
      if(!call) {
        throw std::runtime_error(error_unknown_expr);
      }
      direct_call out;
      out.api_id = call->api_id;
      out.function_id = call->function_id;
      out.literals.reserve(call->args.size());
      for(const expr& arg : call->args) {
        if(!arg) {
          throw std::runtime_error(error_unknown_expr);
        }
        if(dynamic_cast<ref_expr_node*>(arg.get())) {
          throw std::runtime_error(error_dependent_ref_not_supported);
        }
        auto* lit = dynamic_cast<literal_expr_node*>(arg.get());
        if(!lit) {
          throw std::runtime_error(error_unknown_expr);
        }
        literal_expr stored;
        stored.kind = lit->kind;
        stored.value = lit->value;
        out.literals.push_back(std::move(stored));
      }
      return out;
    }

  }
}
