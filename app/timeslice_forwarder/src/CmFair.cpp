#include "CmFair.hpp"
#include "log.hpp"
#include <cstdint>
#include <exception>
#include <ostream>
#include <vector>

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
  L_(debug) << "on_new_node start";
  unique_lock<mutex> l(mtx_);
  auto node_uid = MAKE_UID(group_id, node_id);
  node_status_map_[node_uid] = make_shared<NodeBufferStatus>();
  L_(debug) << "on_new_node end";
}

void CmFair::on_new_buffer_status(std::shared_ptr<char> wi_serialized, uint64_t group_id, uint64_t node_id) {
  // auto node_uid = MAKE_UID(group_id, node_id);
  L_(debug) << "on_new_buffer_status start";
  WiTimesliceStatus ts_status;
  ts_status.deserialize(wi_serialized);
  if (ts_status.sub_type == WiType::wi_timeslice_status) {
    if (group_id == SENDER_GROUP_ID) {
      L_(debug) << "Received buffer status of sender...";
      unique_lock<mutex> l(mtx_);
      sender_status_available_.push_back(ts_status);
      eval_worker_cv_.notify_all();
    } else if (group_id == RECEIVER_GROUP_ID) {
      L_(debug) << "Received buffer status of receiver ...";

      // L_(trace) << "WiType::wi_work_done - node_id: " << node_id << " - group_id " << group_id << " - cnt: " << ts_status.ts_indexes.size();
      unique_lock<mutex> l(mtx_);
      receiver_status_available_.push_back(ts_status);
      // nodes_load_[node_uid] -= ts_status.ts_indexes.size();
      // monitor_->QueueMetric("timeslice_forwarder_state",
      //                       {{"CM", "CM"},
      //                        {"host", hostname_},
      //                        {"receiver", to_string(node_id)}},
      //                       {{"cm_receiver_load", nodes_load_[node_uid]}});
      eval_worker_cv_.notify_all();
    } else {
      L_(error) << "Received buffer status from unknown group ID: " << group_id;
    }
  } else if (ts_status.sub_type == WiType::wi_buffer_full_report) {
    L_(debug) << "Received buffer full report";
    buffer_full_reports_.push_back(ts_status);
  } else {
    L_(error) << "WiTimesliceStatus unknown subtype: " << ts_status.sub_type << endl;
  }
  L_(debug) << "on_new_buffer_status end";
}

void CmFair::eval_node_status() {
  // We don't continuously lock, but interrupt it to not starve other threads.
  L_(debug) << "eval_node_status start";
  std::vector<uint64_t> processed_ts_indexes;
  {
    unique_lock<mutex> l(mtx_);
    for (auto& wi : receiver_status_available_) {
      auto receiver_node_uid = MAKE_UID(wi.origin_group_id, wi.origin_node_id);
      auto receiver_status = node_status_map_[receiver_node_uid];
      L_(debug) << "Receiver Processed: " << endl
                << "Node ID: " << wi.origin_node_id << endl
                << "TS Count: " << wi.ts_indexes.size();
      if (receiver_status->vector_clock < wi.vector_clock) {
        receiver_status->buffer_fill_level = wi.buffer_level;
        receiver_status->full = false;
        receiver_status->vector_clock = wi.vector_clock;
      } else {
        L_(debug) << "RECEIVER status: Ignored status update";
      }
      processed_ts_indexes.insert(
          std::end(processed_ts_indexes),
          std::begin(wi.ts_indexes),
          std::end(wi.ts_indexes));
    }
    receiver_status_available_.clear();
  }
  L_(debug) << "eval_node_status start 1";

  std::vector<uint64_t> failed_senders_;
  {
    unique_lock<mutex> l(mtx_);
    for (auto& wi : buffer_full_reports_) {
      auto sender_node_uid = MAKE_UID(wi.origin_group_id, wi.origin_node_id);
      auto receiver_node_uid = wi.node_uids[0];
      auto receiver_status = node_status_map_[receiver_node_uid];
      auto sender_status = node_status_map_[sender_node_uid];

      L_(debug) << "Buffer Full Report: " << endl
                << "From Node ID: " << wi.origin_node_id << endl
                << "To Node ID: " << NODE_ID(wi.node_uids[0]);
      if (receiver_status->vector_clock < wi.vector_clock) {
        receiver_status->buffer_fill_level = wi.rem_buffer_level;
        receiver_status->full = true;
        receiver_status->vector_clock = wi.vector_clock;
      } else {
        L_(debug) << "BUFFER FULL status: Ignored status update";
      }
      sender_status->buffer_fill_level = wi.buffer_level;
      failed_senders_.push_back(sender_node_uid);
    }
    buffer_full_reports_.clear();
  }

  L_(debug) << "eval_node_status start 2";
  for (auto& ts_index : processed_ts_indexes) {
    auto found_idx = false;
    unique_lock<mutex> l(mtx_);
    for (auto it = ts_sending_status_by_age_.begin(); it != ts_sending_status_by_age_.end(); it++) {
      auto& idx = (*it)->ts_index;
      auto& receiver_uid = (*it)->receiver_node_uid;
      auto& ts_size = (*it)->ts_size;
      if (idx == ts_index) {
        nodes_load_[receiver_uid] -= ts_size;
        ts_sending_status_by_age_.erase(it);
        found_idx = true;
        break;
      }
    }
    if (!found_idx) {
      L_(error) << "Could not found ts index to removed it from queued timeslices";
    }
  }

  L_(debug) << "eval_node_status start 3";
  for (auto& failed_sender_uid : failed_senders_) {
    auto found_node = false;
    unique_lock<mutex> l(mtx_);
    for (auto& send_status : ts_sending_status_by_age_) {
      auto& node_uid = send_status->sender_node_uid;
      if (node_uid == failed_sender_uid) {
        found_node = true;
        send_status->order_given = false;
        send_status->failed_sending_attempts++;
        break;
      }
    }
    if (!found_node) {
      L_(error) << "Could not found node to process buffer full repor";
    } else {
      L_(info) << "Processed buffer full report";
    }
  }

  L_(debug) << "eval_node_status start 4";
  {
    unique_lock<mutex> l(mtx_);
    for (auto& sender_status : sender_status_available_) {

      auto sender_uid = MAKE_UID(sender_status.origin_group_id, sender_status.origin_node_id);
      auto ts_idx = sender_status.ts_indexes[0];
      auto ts_size = sender_status.ts_sizes[0];
      auto sender_buffer_level = sender_status.buffer_level;

      auto sending_status = make_shared<SendingStatus>();
      sending_status->sender_node_uid = sender_uid;
      sending_status->ts_index = ts_idx;
      sending_status->ts_size = ts_size;

      node_status_map_[sender_uid]->buffer_fill_level = sender_buffer_level;
      ts_sending_status_by_age_.push_back(sending_status);
    }
    sender_status_available_.clear();
  }

  L_(debug) << "eval_node_status start 5";
  {
    unique_lock<mutex> l(mtx_);
    for (auto& ts_status : ts_sending_status_by_age_) {
      auto sender_uid = ts_status->sender_node_uid;
      if (ts_status->order_given) {
        L_(debug) << "order given: " << NODE_ID(sender_uid);
        continue;
      }

      L_(debug) << "Calc destination for sender: " << NODE_ID(sender_uid);
      auto connections = connection_manager_.get_connections(sender_uid);
      if (connections.empty()) { // node is not connected to any other node
        L_(debug) << "sender: " << NODE_ID(sender_uid) << " - no connections available";
        return;
      }

      // find receiver node with lowest load
      uint64_t node_uid_lowest_load;
      uint64_t lowest_load_value = UINT64_MAX;
      for (auto& conn : connections) {
        if (nodes_load_[conn] < lowest_load_value && !node_status_map_[conn]->full) {
          lowest_load_value = nodes_load_[conn];
          node_uid_lowest_load = conn;
          if (nodes_load_[conn] == 0) { // 0 is the lowest load value possible -> exit early
            break;
          }
        }
      }

      if (lowest_load_value == UINT64_MAX) {
        L_(debug) << "sender: " << NODE_ID(sender_uid) << " - lowest load exit";
        ts_status->order_given = false;
        continue;
      }

      L_(debug) << "Eval node status: "
                << "Send from: " << NODE_ID(sender_uid) << " - to: " << NODE_ID(node_uid_lowest_load);
      nodes_load_[node_uid_lowest_load] += ts_status->ts_size;
      monitor_->QueueMetric("timeslice_forwarder_state",
                            {{"CM", "CM"},
                             {"host", hostname_},
                             {"receiver", to_string(NODE_ID(node_uid_lowest_load))}},
                            {{"cm_receiver_load", nodes_load_[node_uid_lowest_load]}});
      auto wi_tx = make_shared<WiTransmission>();
      wi_tx->node_uid = node_uid_lowest_load;
      wi_tx->type = WorkItem::transmission;
      ts_status->order_given = true;
      ts_status->receiver_node_uid = node_uid_lowest_load;
      Node::send_work_item(uid_address_map_[sender_uid], wi_tx);
    }
  }
  L_(debug) << "eval_node_status end";

  // {
  //   unique_lock<mutex> l(mtx_);
  //   auto tx_it = sender_status_available_.begin();
  //   while (tx_it != sender_status_available_.end()) {
  //     auto& wi = *tx_it;
  //     auto sender_uid = MAKE_UID(wi.origin_group_id, wi.origin_node_id);
  //     auto connections = connection_manager_.get_connections(sender_uid);
  //     if (connections.empty()) { // node is not connected to any other node
  //       return;
  //     }

  //     // find receiver node with lowest load
  //     uint64_t node_uid_lowest_load;
  //     uint64_t lowest_load_value = UINT64_MAX;
  //     for (auto& conn : connections) {
  //       if (nodes_load_[conn] < lowest_load_value) {
  //         lowest_load_value = nodes_load_[conn];
  //         node_uid_lowest_load = conn;
  //         if (nodes_load_[conn] == 0) { // 0 is the lowest load value possible -> exit early
  //           break;
  //         }
  //       }
  //     }

  //     if (lowest_load_value == UINT64_MAX) {
  //       continue;
  //     }

  //     L_(debug) << "Eval node status"
  //               << "Send from: " << NODE_ID(sender_uid) << " - to: " << NODE_ID(node_uid_lowest_load);
  //     nodes_load_[node_uid_lowest_load]++;
  //     monitor_->QueueMetric("timeslice_forwarder_state",
  //                           {{"CM", "CM"},
  //                            {"host", hostname_},
  //                            {"receiver", to_string(NODE_ID(node_uid_lowest_load))}},
  //                           {{"cm_receiver_load", nodes_load_[node_uid_lowest_load]}});
  //     auto wi_tx = make_shared<WiTransmission>();
  //     wi_tx->node_uid = node_uid_lowest_load;
  //     wi_tx->type = WorkItem::transmission;
  //     Node::send_work_item(uid_address_map_[sender_uid], wi_tx);
  //     tx_it = sender_status_available_.erase(tx_it);
  //   }
  // }
}

void CmFair::eval_thread() {
  while (!stop_eval_worker_) {
    {
      unique_lock<mutex> l(mtx_);
      // eval_worker_cv_.wait_for(l, std::chrono::milliseconds(200), [&]() {
      //   return !sender_status_available_.empty() || static_cast<bool>(stop_eval_worker_);
      // });
      eval_worker_cv_.wait(l);
      if (stop_eval_worker_) {
        break;
      }
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
