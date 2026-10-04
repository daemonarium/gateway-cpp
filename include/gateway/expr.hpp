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

    struct ref_expr {
      std::uint64_t ref_id = 0;
    };

    struct literal_expr {
      literal_kind kind = literal_kind::uint64;
      binary value;
    };

    struct expr;
    struct member_expr;
    struct call_expr;
    struct switch_expr;
    struct non_empty_expr;

    // Pointer slots are nullable. Destructors are out of line so the
    // shared_ptr pointees can be incomplete inside the class body.
    struct expr {
      expr_kind kind = expr_kind::ref;
      std::shared_ptr<ref_expr> ref;
      std::shared_ptr<literal_expr> literal;
      std::shared_ptr<member_expr> member;
      std::shared_ptr<call_expr> call;
      std::shared_ptr<switch_expr> switch_expr_;
      std::shared_ptr<non_empty_expr> nonempty;

      expr();
      expr(const expr&);
      expr(expr&&) noexcept;
      expr& operator=(const expr&);
      expr& operator=(expr&&) noexcept;
      ~expr();
    };

    struct member_expr {
      expr inner;
      std::uint32_t member_id = 0;

      member_expr();
      member_expr(const member_expr&);
      member_expr(member_expr&&) noexcept;
      member_expr& operator=(const member_expr&);
      member_expr& operator=(member_expr&&) noexcept;
      ~member_expr();
    };

    struct call_expr {
      std::uint32_t api_id = 0;
      std::uint64_t function_id = 0;
      std::vector<expr> args;

      call_expr();
      call_expr(const call_expr&);
      call_expr(call_expr&&) noexcept;
      call_expr& operator=(const call_expr&);
      call_expr& operator=(call_expr&&) noexcept;
      ~call_expr();
    };

    struct switch_case {
      std::uint64_t key = 0;
      expr branch;

      switch_case();
      switch_case(const switch_case&);
      switch_case(switch_case&&) noexcept;
      switch_case& operator=(const switch_case&);
      switch_case& operator=(switch_case&&) noexcept;
      ~switch_case();
    };

    struct switch_expr {
      expr discriminant;
      std::vector<switch_case> cases;
      std::shared_ptr<expr> default_branch;

      switch_expr();
      switch_expr(const switch_expr&);
      switch_expr(switch_expr&&) noexcept;
      switch_expr& operator=(const switch_expr&);
      switch_expr& operator=(switch_expr&&) noexcept;
      ~switch_expr();
    };

    struct non_empty_expr {
      expr inner;

      non_empty_expr();
      non_empty_expr(const non_empty_expr&);
      non_empty_expr(non_empty_expr&&) noexcept;
      non_empty_expr& operator=(const non_empty_expr&);
      non_empty_expr& operator=(non_empty_expr&&) noexcept;
      ~non_empty_expr();
    };

    inline member_expr::member_expr() = default;
    inline member_expr::member_expr(const member_expr&) = default;
    inline member_expr::member_expr(member_expr&&) noexcept = default;
    inline member_expr& member_expr::operator=(const member_expr&) = default;
    inline member_expr& member_expr::operator=(member_expr&&) noexcept = default;
    inline member_expr::~member_expr() = default;

    inline call_expr::call_expr() = default;
    inline call_expr::call_expr(const call_expr&) = default;
    inline call_expr::call_expr(call_expr&&) noexcept = default;
    inline call_expr& call_expr::operator=(const call_expr&) = default;
    inline call_expr& call_expr::operator=(call_expr&&) noexcept = default;
    inline call_expr::~call_expr() = default;

    inline switch_case::switch_case() = default;
    inline switch_case::switch_case(const switch_case&) = default;
    inline switch_case::switch_case(switch_case&&) noexcept = default;
    inline switch_case& switch_case::operator=(const switch_case&) = default;
    inline switch_case& switch_case::operator=(switch_case&&) noexcept = default;
    inline switch_case::~switch_case() = default;

    inline switch_expr::switch_expr() = default;
    inline switch_expr::switch_expr(const switch_expr&) = default;
    inline switch_expr::switch_expr(switch_expr&&) noexcept = default;
    inline switch_expr& switch_expr::operator=(const switch_expr&) = default;
    inline switch_expr& switch_expr::operator=(switch_expr&&) noexcept = default;
    inline switch_expr::~switch_expr() = default;

    inline non_empty_expr::non_empty_expr() = default;
    inline non_empty_expr::non_empty_expr(const non_empty_expr&) = default;
    inline non_empty_expr::non_empty_expr(non_empty_expr&&) noexcept = default;
    inline non_empty_expr& non_empty_expr::operator=(const non_empty_expr&) = default;
    inline non_empty_expr& non_empty_expr::operator=(non_empty_expr&&) noexcept = default;
    inline non_empty_expr::~non_empty_expr() = default;

    inline expr::expr() = default;
    inline expr::expr(const expr&) = default;
    inline expr::expr(expr&&) noexcept = default;
    inline expr& expr::operator=(const expr&) = default;
    inline expr& expr::operator=(expr&&) noexcept = default;
    inline expr::~expr() = default;

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
      expr out;
      out.kind = expr_kind::call;
      auto call = std::make_shared<call_expr>();
      call->api_id = api_id;
      call->function_id = function_id;
      call->args = std::move(args);
      out.call = std::move(call);
      return out;
    }

    inline expr make_literal_u64(std::uint64_t value) {
      expr out;
      out.kind = expr_kind::literal;
      auto lit = std::make_shared<literal_expr>();
      lit->kind = literal_kind::uint64;
      lit->value = u64_to_le(value);
      out.literal = std::move(lit);
      return out;
    }

    inline expr make_ref(std::uint64_t ref_id) {
      expr out;
      out.kind = expr_kind::ref;
      auto ref = std::make_shared<ref_expr>();
      ref->ref_id = ref_id;
      out.ref = std::move(ref);
      return out;
    }

    inline expr make_member(expr inner, std::uint32_t member_id) {
      expr out;
      out.kind = expr_kind::member;
      auto member = std::make_shared<member_expr>();
      member->inner = std::move(inner);
      member->member_id = member_id;
      out.member = std::move(member);
      return out;
    }

    inline expr make_switch(expr discriminant, std::vector<switch_case> cases, std::shared_ptr<expr> default_branch = nullptr) {
      expr out;
      out.kind = expr_kind::switch_;
      auto node = std::make_shared<switch_expr>();
      node->discriminant = std::move(discriminant);
      node->cases = std::move(cases);
      node->default_branch = std::move(default_branch);
      out.switch_expr_ = std::move(node);
      return out;
    }

    // v1.5: Call(api, fn, Literal*) only.
    // Ref as the root is unknown_expr. Ref as a Call argument is dependent_ref_not_supported.
    inline direct_call validate_direct_expr(const expr& value) {
      if(value.kind == expr_kind::ref) {
        throw std::runtime_error(error_unknown_expr);
      }
      if(value.kind != expr_kind::call || !value.call) {
        throw std::runtime_error(error_unknown_expr);
      }
      direct_call out;
      out.api_id = value.call->api_id;
      out.function_id = value.call->function_id;
      out.literals.reserve(value.call->args.size());
      for(const expr& arg : value.call->args) {
        if(arg.kind == expr_kind::ref) {
          throw std::runtime_error(error_dependent_ref_not_supported);
        }
        if(arg.kind != expr_kind::literal || !arg.literal) {
          throw std::runtime_error(error_unknown_expr);
        }
        out.literals.push_back(*arg.literal);
      }
      return out;
    }

  }
}
