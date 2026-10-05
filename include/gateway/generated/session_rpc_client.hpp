#include "gateway/expr.hpp"
#include "gateway/types.hpp"
#include <serialization/rpc_client.hpp>
#include <serialization/promise.hpp>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <cstdint>

namespace waos::gateway {

template <typename rpc_client_type>
class session_rpc {
  public:
    session_rpc(std::shared_ptr<rpc_client_type> rpc_client) : rpc_client_(rpc_client) {}
    ~session_rpc() {}
    
    std::shared_ptr<serialization::promise<server_protocol>> hello(client_protocol client) {
      return rpc_client_->template call<server_protocol>(0, client);
    }

    std::shared_ptr<serialization::promise<std::vector<api_info>>> list_apis() {
      return rpc_client_->template call<std::vector<api_info>>(1);
    }

    std::shared_ptr<serialization::promise<std::vector<std::uint8_t>>> get_rpc_description(std::uint32_t api_id) {
      return rpc_client_->template call<std::vector<std::uint8_t>>(2, api_id);
    }

    std::shared_ptr<serialization::promise<std::vector<std::uint8_t>>> get_session_description() {
      return rpc_client_->template call<std::vector<std::uint8_t>>(3);
    }

    std::shared_ptr<serialization::promise<void>> set_reference(std::uint64_t ref_id, std::shared_ptr<expr_node> value) {
      return rpc_client_->template call<void>(4, ref_id, value);
    }

    std::shared_ptr<serialization::promise<void>> observe_reference(std::uint64_t ref_id) {
      return rpc_client_->template call<void>(5, ref_id);
    }

    std::shared_ptr<serialization::promise<void>> unobserve_reference(std::uint64_t ref_id) {
      return rpc_client_->template call<void>(6, ref_id);
    }

    std::shared_ptr<serialization::promise<void>> dispose_reference(std::uint64_t ref_id) {
      return rpc_client_->template call<void>(7, ref_id);
    }

    std::shared_ptr<serialization::promise<void>> fetch_reference(std::uint64_t ref_id) {
      return rpc_client_->template call<void>(8, ref_id);
    }

  private:
    std::shared_ptr<rpc_client_type> rpc_client_;
};


} // namespace waos::gateway
