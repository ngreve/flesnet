#pragma once
#include "CentralManager.hpp"
#include "WorkItems.hpp"
#include <cstdint>
#include <unordered_map>

class CmFair : public CentralManager {
public:
  CmFair(std::string listen_address,
         uint64_t wi_buffer_size,
         uint64_t wi_buffer_map_size,
         std::string monitoring_uri,
         std::string hostname);
  ~CmFair() override;

private:
  struct NodeBufferStatus {
    uint64_t vector_clock = 0;
    bool full = false;
    float buffer_fill_level = 0.0;
  };

  std::vector<WiTimesliceStatus> sender_status_available_;
  std::vector<WiTimesliceStatus> buffer_full_reports_;
  std::vector<WiTimesliceStatus> receiver_status_available_;

  // <node_uid, buffer status>
  std::unordered_map<uint64_t, std::shared_ptr<NodeBufferStatus>> node_status_map_;

  // <receiver node_uid, counter of concurrent receives>
  std::unordered_map<uint64_t, uint64_t> concurrent_receives_map_;

  std::vector<uint64_t> available_timeslices_on_node;

  std::condition_variable eval_worker_cv_;
  std::future<void> eval_worker_;
  std::atomic_bool stop_eval_worker_ = false;

  void on_new_node(uint64_t group_id, uint64_t node_id) override;
  void on_new_buffer_status(std::shared_ptr<char> wi_serialized, uint64_t group_id, uint64_t node_id) override;
  void eval_node_status();
  void eval_thread();
};