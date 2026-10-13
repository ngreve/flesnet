#pragma once
#include "CentralManager.hpp"

class CmUnfair : public CentralManager {
public:
  CmUnfair(std::string listen_address,
           uint64_t wi_buffer_size,
           uint64_t wi_buffer_map_size,
           std::string monitoring_uri,
           std::string hostname) : CentralManager(listen_address,
                                                  wi_buffer_size,
                                                  wi_buffer_map_size,
                                                  monitoring_uri,
                                                  hostname) {};

private:
  void on_new_buffer_status(std::shared_ptr<char> wi_serialized, uint64_t group_id, uint64_t node_id) override;
};