/**
 * @file
 * @author Dirk Hutter <hutter@compeng.uni-frankfurt.de>
 * Derived form ALICE CRORC Project written by
 * Heiko Engel <hengel@cern.ch>
 */
#include <cassert>

#include <registers.h>
#include <dma_channel.hpp>
#include <data_structures.hpp>

namespace flib {

  dma_channel::dma_channel(register_file* rfpkt,
                           pda::device* parent_device,
                           pda::dma_buffer* data_buffer,
                           pda::dma_buffer* desc_buffer,
                           size_t dma_transfer_size)
    : m_rfpkt(rfpkt),
      m_parent_device(parent_device),
      m_data_buffer(data_buffer),
      m_desc_buffer(desc_buffer),
      m_dma_transfer_size(dma_transfer_size)
  {
    m_reg_dmactrl_cached =  m_rfpkt->reg(RORC_REG_DMA_CTRL);
    // ensure HW is disabled
    if (is_enabled()) {
      throw FlibException("DMA Engine already enabled");
    }
    configure();
    enable();
  }

  dma_channel::~dma_channel() {
    disable();
  }
  
void dma_channel::set_sw_read_pointers(uint64_t data_offset, uint64_t desc_offset) {
  assert(data_offset % m_dma_transfer_size == 0);
  assert(desc_offset % sizeof(MicrosliceDescriptor) == 0);

  sw_read_pointers_t offsets;
  offsets.data_low  = get_lo_32(data_offset);
  offsets.data_high = get_hi_32(data_offset);
  offsets.desc_low  = get_lo_32(desc_offset);
  offsets.desc_high = get_hi_32(desc_offset);

  // TODO: hack to save read-modify-write on dma_ctrl register
  // move sync pointers to exclusive HW register to avoid this
  // no need to chache sync pointers bit because it is pulse only
  offsets.dma_ctrl =
    m_reg_dmactrl_cached |
    (1<<BIT_DMACTRL_SYNC_SWRDPTRS);

  m_rfpkt->set_mem(RORC_REG_EBDM_SW_READ_POINTER_L, &offsets,
                   sizeof(offsets) >> 2);
}
  
// PRIVATE MEMBERS /////////////////////////////////////

void dma_channel::configure() {
  configure_sg_manager(data_sg_bram);
  configure_sg_manager(desc_sg_bram);  
  set_dma_transfer_size();
  uint64_t data_buffer_offset = m_data_buffer->size() - m_dma_transfer_size;
  uint64_t desc_buffer_offset = m_desc_buffer->size() - sizeof(MicrosliceDescriptor);
  set_sw_read_pointers(data_buffer_offset, desc_buffer_offset);
}

void dma_channel::configure_sg_manager(const sg_bram_t buf_sel) {
  pda::dma_buffer* buffer = buf_sel ? m_desc_buffer : m_data_buffer;
  // convert list
  std::vector<sg_entry_hw_t> sg_list_hw = convert_sg_list(buffer->sg_list());

  // check that sg list fits into HW bram
  if (sg_list_hw.size() > get_max_sg_entries(buf_sel)) {
    throw FlibException("Number of SG entries exceeds availabel HW memory");
  }
  // write sg list to bram
  write_sg_list_to_device(sg_list_hw, buf_sel);
  set_configured_buffer_size(buf_sel);
}               

  // TODO make vector a reference
std::vector<sg_entry_hw_t> dma_channel::convert_sg_list(const std::vector<pda::sg_entry_t> sg_list) {

  // convert pda scatter gather list into FLIB usable list
  std::vector<sg_entry_hw_t> sg_list_hw;
  sg_entry_hw_t hw_entry;
  uint64_t cur_addr;
  uint64_t cur_length;

  for (const auto &entry : sg_list) {
    cur_addr = reinterpret_cast<uint64_t>(entry.pointer);
    cur_length = entry.length;

    // split entries larger than 4GB (addr >32Bit) 
    while (cur_length>>32 != 0) {
      hw_entry.addr_low  = get_lo_32(cur_addr);
      hw_entry.addr_high = get_hi_32(cur_addr);
      hw_entry.length = (UINT64_C(1) << 32) - PAGE_SIZE; // TODO: why -page_size?
      sg_list_hw.push_back(hw_entry);

      cur_addr += hw_entry.length;
      cur_length -= hw_entry.length;
    }
    hw_entry.addr_low  = get_lo_32(cur_addr);
    hw_entry.addr_high = get_hi_32(cur_addr);
    hw_entry.length = static_cast<uint32_t>(cur_length);
    sg_list_hw.push_back(hw_entry);
  }
  
  return sg_list_hw;
}
  
  void dma_channel::write_sg_list_to_device(const std::vector<sg_entry_hw_t> sg_list,
                                            const sg_bram_t buf_sel) {
  uint32_t buf_addr = 0;
  for (const auto &entry : sg_list) {
    write_sg_entry_to_device(entry, buf_sel, buf_addr);
    ++buf_addr;
  }
  // clear trailing sg entry in bram
  sg_entry_hw_t clear = {0}; 
  write_sg_entry_to_device(clear, buf_sel, buf_addr);  

  // set number of configured sg entries
  // this HW registers do not influence the dma engine
  // they are for debug pourpouse only
  set_configured_sg_entries(buf_sel, sg_list.size());
}

void dma_channel::write_sg_entry_to_device(const sg_entry_hw_t entry,
                              const sg_bram_t buf_sel,
                              const uint32_t buf_addr) {
  
  uint32_t sg_ctrl = (1 << BIT_SGENTRY_CTRL_WRITE_EN) | buf_addr;
  sg_ctrl |= (buf_sel << BIT_SGENTRY_CTRL_TARGET);
  
  // write entry en block to mailbox register
  m_rfpkt->set_mem(RORC_REG_SGENTRY_ADDR_LOW, &entry,sizeof(entry) >> 2);
  // push mailbox to bram
  m_rfpkt->set_reg(RORC_REG_SGENTRY_CTRL, sg_ctrl);
}

size_t dma_channel::get_max_sg_entries(const sg_bram_t buf_sel) {

  sys_bus_addr addr = (buf_sel) ? RORC_REG_RBDM_N_SG_CONFIG : RORC_REG_EBDM_N_SG_CONFIG;
  // N_SG_CONFIG:
  // [15:0] : actual number of sg entries in RAM
  // [31:16]: maximum number of entries (read only)
  return (m_rfpkt->reg(addr) >> 16);  
}

size_t dma_channel::get_configured_sg_entries(const sg_bram_t buf_sel) {

  sys_bus_addr addr = (buf_sel) ? RORC_REG_RBDM_N_SG_CONFIG : RORC_REG_EBDM_N_SG_CONFIG;
  // N_SG_CONFIG:
  // [15:0] : actual number of sg entries in RAM
  // [31:16]: maximum number of entries (read only)
  return (m_rfpkt->reg(addr) & 0x0000ffff);  
}

void dma_channel::set_configured_sg_entries(const sg_bram_t buf_sel,
                               const uint16_t num_entries) {

  sys_bus_addr addr = (buf_sel) ? RORC_REG_RBDM_N_SG_CONFIG : RORC_REG_EBDM_N_SG_CONFIG;
  // N_SG_CONFIG:
  // [15:0] : actual number of sg entries in RAM
  // [31:16]: maximum number of entries (read only)
  m_rfpkt->set_reg(addr, num_entries);  
}

void dma_channel::set_configured_buffer_size(const sg_bram_t buf_sel) {
  // this HW registers do not influence the dma engine
  // they are for debug pourpouse only
  pda::dma_buffer* buffer = (buf_sel) ? m_desc_buffer : m_data_buffer;
  sys_bus_addr addr = (buf_sel) ? RORC_REG_RBDM_BUFFER_SIZE_L : RORC_REG_EBDM_BUFFER_SIZE_L;
  uint64_t size = buffer->size();
  m_rfpkt->set_mem(addr, &size, sizeof(size)>>2);  
}

void dma_channel::set_dma_transfer_size() {
  // dma_transfer_size must be a multiple of 4 byte
  assert ((m_dma_transfer_size & 0x3) == 0);
  // dma_transfer_size must smaler absolute limit
  assert (m_dma_transfer_size <= 1024);
  if (m_dma_transfer_size > m_parent_device->max_payload_size()) {
    throw FlibException("DMA transfer size exceeds PCI MaxPayload");
  }
  // set DMA_TRANSFER_SIZE size (has to be provided as #DWs)
  set_dmactrl((m_dma_transfer_size >> 2) << BIT_DMACTRL_TRANS_SIZE_LSB,
                0x3ff<<BIT_DMACTRL_TRANS_SIZE_LSB);
}

inline void dma_channel::enable() {
  set_dmactrl((0<<BIT_DMACTRL_FIFO_RST |
               1<<BIT_DMACTRL_EBDM_EN |
               1<<BIT_DMACTRL_RBDM_EN |
               1<<BIT_DMACTRL_DMA_EN),
              (1<<BIT_DMACTRL_FIFO_RST |
               1<<BIT_DMACTRL_EBDM_EN |
               1<<BIT_DMACTRL_RBDM_EN |
               1<<BIT_DMACTRL_DMA_EN));
}

void dma_channel::disable(size_t timeout) {
  // disable data buffer
  set_dmactrl((0<<BIT_DMACTRL_EBDM_EN),
              (1<<BIT_DMACTRL_EBDM_EN));
  // wait till ongoing transfer is finished
  while (is_busy() && timeout!=0) {
    usleep(100);
    --timeout;
  }
  // disable everything else put fifo to reset
  set_dmactrl((0<<BIT_DMACTRL_RBDM_EN |
               0<<BIT_DMACTRL_DMA_EN |
               1<<BIT_DMACTRL_FIFO_RST),
              (1<<BIT_DMACTRL_RBDM_EN |
               1<<BIT_DMACTRL_DMA_EN |
               1<<BIT_DMACTRL_FIFO_RST));
}

inline void dma_channel::reset_fifo(bool enable) {
  set_dmactrl((enable<<BIT_DMACTRL_FIFO_RST),
              (1<<BIT_DMACTRL_FIFO_RST));
}
  
inline bool dma_channel::is_enabled() {
  uint32_t mask =
    1<<BIT_DMACTRL_EBDM_EN |
    1<<BIT_DMACTRL_RBDM_EN | 
    1<<BIT_DMACTRL_DMA_EN;
  return (m_reg_dmactrl_cached & mask);  
}
  
inline bool dma_channel::is_busy() {
  return m_rfpkt->bit(RORC_REG_DMA_CTRL, BIT_DMACTRL_BUSY);
}

void dma_channel::set_dmactrl(uint32_t reg, uint32_t mask) {
  // acces to dma_ctrl register
  // ensures proper caching of register value
  // never cache BIT_DMACTRL_SYNC_SWRDPTRS
  mask &= ~(1<<BIT_DMACTRL_SYNC_SWRDPTRS);
  m_reg_dmactrl_cached = set_bits(m_reg_dmactrl_cached, reg, mask);
  m_rfpkt->set_reg(RORC_REG_DMA_CTRL, m_reg_dmactrl_cached);
}

inline uint32_t dma_channel::set_bits(uint32_t old_val, uint32_t new_val, uint32_t mask) {
    // clear all unselected bits in new_val
    new_val &= mask;
    // clear all selects bits and set according to new_val
    old_val &= ~mask;
    old_val |= new_val;
    return old_val;
}

inline uint32_t dma_channel::get_lo_32(uint64_t val) {
  return val;
}

inline uint32_t dma_channel::get_hi_32(uint64_t val) {
  return (val >> 32);
}


} // namespace
