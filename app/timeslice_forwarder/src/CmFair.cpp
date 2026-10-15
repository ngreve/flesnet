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
};

void CmFair::on_new_buffer_status(std::shared_ptr<char> wi_serialized, uint64_t group_id, uint64_t node_id) {
  auto node_uid = MAKE_UID(group_id, node_id);
  if (group_id == SENDER_GROUP_ID) {
    unique_lock<mutex> l(mtx_);
    WiTimesliceStatus ts_status;
    ts_status.deserialize(wi_serialized);
    sender_status_available_.push_back(ts_status);
    eval_worker_cv_.notify_all();
  } else if (group_id == RECEIVER_GROUP_ID) {
    WiTimesliceStatus wi_done;
    wi_done.deserialize(wi_serialized);
    L_(trace) << "WiType::wi_work_done - node_id: " << node_id << " - group_id " << group_id << " - cnt: " << wi_done.ts_indexes.size();
    unique_lock<mutex> l(mtx_);
    nodes_load_[node_uid] -= wi_done.ts_indexes.size();
    monitor_->QueueMetric("timeslice_forwarder_state",
                          {{"CM", "CM"},
                           {"host", hostname_},
                           {"receiver", to_string(node_id)}},
                          {{"cm_receiver_load", nodes_load_[node_uid]}});
    eval_worker_cv_.notify_all();
  } else {
    L_(error) << "Received buffer status from unknown group ID: " << group_id;
  }
}

void CmFair::eval_node_status() {
  unique_lock<mutex> l(mtx_);
  auto it = sender_status_available_.begin();
  while (it != sender_status_available_.end()) {
    auto& wi = *it;
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
    it = sender_status_available_.erase(it);
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
