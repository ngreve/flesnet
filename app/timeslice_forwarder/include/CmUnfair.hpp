#pragma once
#include "CentralManager.hpp"

class CmUnfair : public CentralManager {
public:
  CmUnfair(std::string listen_address,
           uint64_t wi_buffer_size,
           uint64_t wi_buffer_map_size,
           std::string monitoring_uri,
           std::string hostname);
  ~CmUnfair() override;

private:
  std::condition_variable eval_worker_cv_;
  std::future<void> eval_worker_;
  std::atomic_bool stop_eval_worker_ = false;

  void on_new_node(uint64_t group_id, uint64_t node_id) override;
  void on_new_buffer_status(std::shared_ptr<char> wi_serialized, uint64_t group_id, uint64_t node_id) override;
  void eval_node_status();
  void eval_thread();
};