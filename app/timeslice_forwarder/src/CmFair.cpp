#include "CmFair.hpp"
#include "log.hpp"
#include <cstdint>
#include <exception>

using namespace std;

CmFair::CmFair(std::string listen_address,
               uint64_t wi_buffer_size,
               uint64_t wi_buffer_map_size,
               std::string monitoring_uri,
               std::string hostname) : CentralManager(listen_address,
                                                      wi_buffer_size,
                                                      wi_buffer_map_size,
                                                      monitoring_uri,
                                                      hostname) {
  eval_worker_ = async(launch::async, &CmFair::eval_thread, this);
}

void CmFair::on_new_node(uint64_t group_id, uint64_t node_id) {
  unique_lock<mutex> l(mtx_);
  auto node_uid = MAKE_UID(group_id, node_id);
  node_status_map_[node_uid] = make_shared<NodeBufferStatus>();
}

void CmFair::on_new_buffer_status(std::shared_ptr<char> wi_serialized, uint64_t group_id, uint64_t node_id) {
  auto node_uid = MAKE_UID(group_id, node_id);
  WiTimesliceStatus ts_status;
  ts_status.deserialize(wi_serialized);
  if (ts_status.sub_type == WiType::wi_timeslice_status) {
    if (group_id == SENDER_GROUP_ID) {
      unique_lock<mutex> l(mtx_);
      sender_status_available_.push_back(ts_status);
      eval_worker_cv_.notify_all();
    } else if (group_id == RECEIVER_GROUP_ID) {
      L_(trace) << "WiType::wi_work_done - node_id: " << node_id << " - group_id " << group_id << " - cnt: " << ts_status.ts_indexes.size();
      unique_lock<mutex> l(mtx_);
      receiver_status_available_.push_back(ts_status);
      // nodes_load_[node_uid] -= ts_status.ts_indexes.size();
      monitor_->QueueMetric("timeslice_forwarder_state",
                            {{"CM", "CM"},
                             {"host", hostname_},
                             {"receiver", to_string(node_id)}},
                            {{"cm_receiver_load", nodes_load_[node_uid]}});
      eval_worker_cv_.notify_all();
    } else {
      L_(error) << "Received buffer status from unknown group ID: " << group_id;
    }
  } else if (ts_status.sub_type == WiType::wi_buffer_full_report) {
    buffer_full_reports_.push_back(ts_status);
  } else {
    L_(error) << "WiTimesliceStatus unknown subtype: " << ts_status.sub_type << endl;
  }
}

void CmFair::eval_node_status() {

  // We don't continuously lock, but interrupt it to not starve other threads.
  {
    unique_lock<mutex> l(mtx_);
    for (auto& wi : receiver_status_available_) {
      auto receiver_node_uid = MAKE_UID(wi.origin_group_id, wi.origin_node_id);
      auto receiver_status = node_status_map_[receiver_node_uid];
      if (receiver_status->vector_clock < wi.vector_clock) {
        receiver_status->buffer_fill_level = wi.buffer_level;
        receiver_status->full = false;
        receiver_status->vector_clock = wi.vector_clock;
      }
    }
    receiver_status_available_.clear();
  }

  {
    unique_lock<mutex> l(mtx_);
    for (auto& wi : buffer_full_reports_) {
      auto sender_node_uid = MAKE_UID(wi.origin_group_id, wi.origin_node_id);
      auto receiver_node_uid = wi.node_uids[0];
      auto receiver_status = node_status_map_[receiver_node_uid];
      auto sender_status = node_status_map_[sender_node_uid];
      if (receiver_status->vector_clock < wi.vector_clock) {
        receiver_status->buffer_fill_level = wi.rem_buffer_level;
        receiver_status->full = true;
        receiver_status->vector_clock = wi.vector_clock;
      }
      sender_status->buffer_fill_level = wi.buffer_level;
    }
    buffer_full_reports_.clear();
  }

  {
    unique_lock<mutex> l(mtx_);
    auto tx_it = sender_status_available_.begin();
    while (tx_it != sender_status_available_.end()) {
      auto& wi = *tx_it;
      auto sender_uid = MAKE_UID(wi.origin_group_id, wi.origin_node_id);
      auto connections = connection_manager_.get_connections(sender_uid);
      if (connections.empty()) { // node is not connected to any other node
        return;
      }

      // find receiver node with lowest load
      uint64_t node_uid_lowest_load;
      uint64_t lowest_load_value = UINT64_MAX;
      for (auto& conn : connections) {
        if (nodes_load_[conn] < lowest_load_value) {
          lowest_load_value = nodes_load_[conn];
          node_uid_lowest_load = conn;
          if (nodes_load_[conn] == 0) { // 0 is the lowest load value possible -> exit early
            break;
          }
        }
      }

      if (lowest_load_value == UINT64_MAX) {
        continue;
      }

      L_(debug) << "Eval node status"
                << "Send from: " << NODE_ID(sender_uid) << " - to: " << NODE_ID(node_uid_lowest_load);
      nodes_load_[node_uid_lowest_load]++;
      monitor_->QueueMetric("timeslice_forwarder_state",
                            {{"CM", "CM"},
                             {"host", hostname_},
                             {"receiver", to_string(NODE_ID(node_uid_lowest_load))}},
                            {{"cm_receiver_load", nodes_load_[node_uid_lowest_load]}});
      auto wi_tx = make_shared<WiTransmission>();
      wi_tx->node_uid = node_uid_lowest_load;
      wi_tx->type = WorkItem::transmission;
      Node::send_work_item(uid_address_map_[sender_uid], wi_tx);
      tx_it = sender_status_available_.erase(tx_it);
    }
  }
}

void CmFair::eval_thread() {
  while (!stop_eval_worker_) {
    {
      unique_lock<mutex> l(mtx_);
      eval_worker_cv_.wait_for(l, std::chrono::milliseconds(200), [&]() {
        return !sender_status_available_.empty() || static_cast<bool>(stop_eval_worker_);
      });
    }
    eval_node_status();
  }
}

CmFair::~CmFair() {
  stop_eval_worker_ = true;
  try {
    eval_worker_.wait();
  } catch (exception e) {
  }
}
