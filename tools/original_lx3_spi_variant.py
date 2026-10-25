"""Generate ONE shared candidate source; profile is a compiler constant.

No Panda firmware, CAN policy, authority payload, state scheduling or arbiter.
The ready profile is experimental and gated on parked cadence measurements.
"""


SUPPORT = r'''
#ifndef LX3_SPI_READY_PROFILE
#define LX3_SPI_READY_PROFILE 0
#endif
#ifndef LX3_SPI_SOURCE_SHA
#define LX3_SPI_SOURCE_SHA "inert-unbound"
#endif
static constexpr const char *spi_profile_name = LX3_SPI_READY_PROFILE ? "turnaround" : "observed";
static thread_local double spi_uncharged_wait_ms = 0.0;
static thread_local unsigned spi_endpoint_index = 3U;
struct SpiProfileStats {
  std::array<std::atomic<uint64_t>,4> attempts{}, header_nack{}, data_nack{}, checksum{}, lock_us_max{};
  std::atomic<uint64_t> requested_us{0}, actual_us{0}, wait_max_us{0}, overshoot_max_us{0}, call_wall_max_us{0};
};
static SpiProfileStats spi_profile_stats;
static double spi_last_activity_ms = -1000.0;
[[maybe_unused]] static unsigned spi_next_ready_us = 0U;
static unsigned spi_endpoint_slot(uint8_t endpoint) { return endpoint==0U ? 0U : (endpoint==1U||endpoint==0x81U) ? 1U : endpoint==3U ? 2U : 3U; }
static void spi_observe_max(std::atomic<uint64_t> &target,uint64_t value) {
  uint64_t before=target.load(std::memory_order_relaxed);
  while(before<value && !target.compare_exchange_weak(before,value,std::memory_order_relaxed)) {}
}
static unsigned long long spi_count(const std::atomic<uint64_t> &value) {
  return value.load(std::memory_order_relaxed);
}
[[maybe_unused]] static bool spi_wait_ready(unsigned required_us,const std::atomic<bool> &connected) {
  const double begin=millis_since_boot();
  if(begin<spi_last_activity_ms)spi_last_activity_ms=begin;
  const double requested=std::max(0.0,required_us-(begin-spi_last_activity_ms)*1000.0);
  if(requested<=0.0)return connected;
  double now=begin;
  while(connected && (now-spi_last_activity_ms)*1000.0<required_us) {
    const double remaining=required_us-(now-spi_last_activity_ms)*1000.0;
    usleep(static_cast<unsigned>(std::clamp(std::ceil(remaining),1.0,1000.0)));
    now=millis_since_boot(); // Recheck interrupted sleeps, never busy-spin.
    if(now<begin)break;
  }
  const double actual=std::max(0.0,(now-begin)*1000.0);
  spi_uncharged_wait_ms+=actual/1000.0;
  spi_profile_stats.requested_us.fetch_add(static_cast<uint64_t>(std::ceil(requested)),std::memory_order_relaxed);
  spi_profile_stats.actual_us.fetch_add(static_cast<uint64_t>(std::ceil(actual)),std::memory_order_relaxed);
  spi_observe_max(spi_profile_stats.wait_max_us,static_cast<uint64_t>(std::ceil(actual)));
  spi_observe_max(spi_profile_stats.overshoot_max_us,static_cast<uint64_t>(std::ceil(std::max(0.0,actual-requested))));
  return connected && now>=begin;
}
static void report_spi_profile() {
  LOGW("SPI_PROFILE_STATS profile=%s source_sha=%s attempts_ctrl=%llu attempts_rx=%llu attempts_tx=%llu attempts_other=%llu header_nack_ctrl=%llu header_nack_rx=%llu header_nack_tx=%llu header_nack_other=%llu data_nack_ctrl=%llu data_nack_rx=%llu data_nack_tx=%llu data_nack_other=%llu checksum_ctrl=%llu checksum_rx=%llu checksum_tx=%llu checksum_other=%llu lock_us_ctrl_max=%llu lock_us_rx_max=%llu lock_us_tx_max=%llu lock_us_other_max=%llu ready_requested_us=%llu ready_actual_us=%llu ready_max_us=%llu ready_overshoot_max_us=%llu call_wall_max_us=%llu",
    spi_profile_name,LX3_SPI_SOURCE_SHA,
    spi_count(spi_profile_stats.attempts[0]),spi_count(spi_profile_stats.attempts[1]),spi_count(spi_profile_stats.attempts[2]),spi_count(spi_profile_stats.attempts[3]),
    spi_count(spi_profile_stats.header_nack[0]),spi_count(spi_profile_stats.header_nack[1]),spi_count(spi_profile_stats.header_nack[2]),spi_count(spi_profile_stats.header_nack[3]),
    spi_count(spi_profile_stats.data_nack[0]),spi_count(spi_profile_stats.data_nack[1]),spi_count(spi_profile_stats.data_nack[2]),spi_count(spi_profile_stats.data_nack[3]),
    spi_count(spi_profile_stats.checksum[0]),spi_count(spi_profile_stats.checksum[1]),spi_count(spi_profile_stats.checksum[2]),spi_count(spi_profile_stats.checksum[3]),
    spi_count(spi_profile_stats.lock_us_max[0]),spi_count(spi_profile_stats.lock_us_max[1]),spi_count(spi_profile_stats.lock_us_max[2]),spi_count(spi_profile_stats.lock_us_max[3]),
    spi_count(spi_profile_stats.requested_us),spi_count(spi_profile_stats.actual_us),spi_count(spi_profile_stats.wait_max_us),spi_count(spi_profile_stats.overshoot_max_us),spi_count(spi_profile_stats.call_wall_max_us));
}
'''


def generate(text):
  def replace(before,after):
    nonlocal text
    assert text.count(before)==1,before[:120]
    text=text.replace(before,after,1)
  replace('static SpiDiagnostics spi_diagnostics;\n','static SpiDiagnostics spi_diagnostics;\n'+SUPPORT+'\n')
  replace('  HEADER_NACK = -4,','  BAD_LENGTH = -5,\n  HEADER_NACK = -4,')
  replace('  if (!spi_diagnostics.report_due(now)) return;','  if (!spi_diagnostics.report_due(now)) return;\n  report_spi_profile();')
  replace('PandaSpiHandle::PandaSpiHandle(std::string serial) {',
    'PandaSpiHandle::PandaSpiHandle(std::string serial) {\n'
    '  static std::atomic_flag logged = ATOMIC_FLAG_INIT;\n'
    '  if (!logged.test_and_set()) LOGW("SPI_BUILD profile=%s source_sha=%s",spi_profile_name,LX3_SPI_SOURCE_SHA);')
  replace('  unsigned attempts = 0;','  unsigned attempts = 0;\n  unsigned bad_lengths = 0;')
  replace('    const double attempt_started = millis_since_boot();','    const double attempt_started = millis_since_boot();\n'
    '    spi_uncharged_wait_ms=0.0;\n    spi_endpoint_index=spi_endpoint_slot(endpoint);')
  replace('      if (!can_backpressure) failure_time_ms += std::max(0.0, millis_since_boot() - attempt_started);',
    '      if (!can_backpressure) failure_time_ms += std::max(0.0,millis_since_boot()-attempt_started-spi_uncharged_wait_ms);\n'
    '      if (ret==SpiError::BAD_LENGTH) ++bad_lengths;\n'
    '      if (bad_lengths>=2U) break; // At most two malformed replies per call, even if NACKs alternate.')
  replace('  spi_diagnostics.observe_call(attempts,',
    '  spi_observe_max(spi_profile_stats.call_wall_max_us,static_cast<uint64_t>(std::ceil(std::max(0.0,millis_since_boot()-start_time)*1000.0)));\n'
    '  spi_diagnostics.observe_call(attempts,')
  replace('  if (ret == SpiError::HEADER_NACK) ret = SpiError::NACK;',
    '  if (ret == SpiError::HEADER_NACK) ret = SpiError::NACK;\n  if (ret == SpiError::BAD_LENGTH) ret = -1;')
  replace('if (ack == SPI_HACK) spi_diagnostics.count(SpiStage::HEADER_NACK);',
    'if (ack == SPI_HACK) { spi_diagnostics.count(SpiStage::HEADER_NACK); spi_profile_stats.header_nack[spi_endpoint_index].fetch_add(1,std::memory_order_relaxed); }')
  replace('if (ack == SPI_DACK) spi_diagnostics.count(SpiStage::DATA_NACK);',
    'if (ack == SPI_DACK) { spi_diagnostics.count(SpiStage::DATA_NACK); spi_profile_stats.data_nack[spi_endpoint_index].fetch_add(1,std::memory_order_relaxed); }')
  begin=text.index('int PandaSpiHandle::lltransfer(');end=text.index('int PandaSpiHandle::spi_transfer(',begin)
  part=text[begin:end]
  assert part.count('  return ret;\n')==1
  part=part.replace('  return ret;\n','  spi_last_activity_ms=millis_since_boot();\n  return ret;\n')
  text=text[:begin]+part+text[end:]
  replace('  LockEx lock(spi_fd, hw_lock);','  const double lock_started=millis_since_boot();\n'
    '  LockEx lock(spi_fd, hw_lock);\n'
    '  const double lock_wait=std::max(0.0,millis_since_boot()-lock_started);\n'
    '  spi_uncharged_wait_ms+=lock_wait;\n'
    '  spi_observe_max(spi_profile_stats.lock_us_max[spi_endpoint_index],static_cast<uint64_t>(std::ceil(lock_wait*1000.0)));\n'
    '  spi_profile_stats.attempts[spi_endpoint_index].fetch_add(1,std::memory_order_relaxed);\n'
    '  if constexpr (LX3_SPI_READY_PROFILE != 0) {\n'
    '    if (!spi_wait_ready(std::max(spi_next_ready_us,endpoint==0U ? 0U : 400U),connected)) return -1;\n'
    '    spi_next_ready_us=endpoint==0U && tx_data!=nullptr && tx_len>=sizeof(ControlPacket_t) && tx_data[0]==0xdcU ? 1000U : 0U;\n'
    '  }')
  replace('  // Send data\n','  // Experimental DMA-ready wait, not stock control-burst scheduling.\n'
    '  if constexpr (LX3_SPI_READY_PROFILE != 0) { if (!spi_wait_ready(400U,connected)) return -1; }\n\n  // Send data\n')
  replace('    spi_diagnostics.count(SpiStage::BAD_LENGTH);','    ret=SpiError::BAD_LENGTH;\n    spi_diagnostics.count(SpiStage::BAD_LENGTH);')
  replace('    spi_diagnostics.count(SpiStage::BAD_CHECKSUM);','    spi_diagnostics.count(SpiStage::BAD_CHECKSUM);\n'
    '    spi_profile_stats.checksum[spi_endpoint_index].fetch_add(1,std::memory_order_relaxed);')
  return text
