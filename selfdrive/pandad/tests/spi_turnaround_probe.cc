// Exercise the actual host driver against an inert DMA-ready-time model.
// This verifies timing semantics, not the physical cause of a vehicle fault.
#define main original_spi_boundary_main
#include "spi_host_probe.cc"
#undef main

enum class ModelPhase { IDLE, HEADER_ACK, PAYLOAD, DATA_ACK, RESPONSE };
static ModelPhase phase = ModelPhase::IDLE;
static bool check_header_gap = false, check_phase_gap = false;
static bool reject_header = false, reject_data = false;
static bool safety_control = false;
static unsigned header_early = 0, data_early = 0, transactions = 0;
static double completed_ms = -1000, hack_ms = 0, next_gap_ms = 0.4;
static unsigned active_endpoint = 0, payload_length = 0;
static double min_hack_gap_ms = 1000, min_header_margin_ms = 1000;

static bool model(spi_ioc_transfer *t) {
  auto *tx = reinterpret_cast<uint8_t *>(t->tx_buf);
  auto *rx = reinterpret_cast<uint8_t *>(t->rx_buf);
  const double started_ms = fake_ms - IO_MS;
  if (tx[0] == 0x14 && t->len == SPI_BUF_SIZE / 2) {
    phase = ModelPhase::IDLE;
    completed_ms = fake_ms;
    next_gap_ms = 0.4;
    return false;
  }
  if (tx[0] == 0x5a && t->len == 7) {
    active_endpoint = tx[1];
    payload_length = uint16_t(tx[2]) | (uint16_t(tx[3]) << 8);
    const double needed = std::max(next_gap_ms, active_endpoint == 0 ? 1.0 : 0.4);
    const double margin = started_ms - completed_ms - needed;
    min_header_margin_ms = std::min(min_header_margin_ms, margin);
    reject_header = check_header_gap && margin < -0.000001;
    if (reject_header) ++header_early;
    phase = ModelPhase::HEADER_ACK;
  } else if (phase == ModelPhase::HEADER_ACK && tx[0] == 0x11 && t->len == 1) {
    if (reject_header) { rx[0] = SPI_NACK; return true; }
    hack_ms = fake_ms;
    phase = ModelPhase::PAYLOAD;
  } else if (phase == ModelPhase::PAYLOAD && t->len == payload_length + 1) {
    const double gap = started_ms - hack_ms;
    min_hack_gap_ms = std::min(min_hack_gap_ms, gap);
    reject_data = check_phase_gap && gap < 0.399999;
    if (reject_data) ++data_early;
    safety_control = active_endpoint == 0 && payload_length >= sizeof(ControlPacket_t) && tx[0] == 0xdc;
    phase = ModelPhase::DATA_ACK;
  } else if (phase == ModelPhase::DATA_ACK && tx[0] == 0x13 && t->len == 3) {
    if (reject_data) { rx[0] = SPI_NACK; return true; }
    phase = ModelPhase::RESPONSE;
  } else if (phase == ModelPhase::RESPONSE) {
    completed_ms = fake_ms;
    next_gap_ms = safety_control ? 1.0 : 0.4;
    ++transactions;
    phase = ModelPhase::IDLE;
  }
  return false;
}

int main(int argc, char **argv) {
  PandaSpiHandle panda("");
  const std::string mode = argc > 1 ? argv[1] : "both";
  check_header_gap = mode == "header" || mode == "both" || mode == "interrupt";
  check_phase_gap = mode == "phase" || mode == "both" || mode == "interrupt";
  assert(check_header_gap || check_phase_gap || mode == "no_delay");
  // The device has just completed a preceding transaction. The first control
  // read must also wait; subsequent cases exercise CAN and safety transitions.
  completed_ms = fake_ms;
  next_gap_ms = 0.4;
  override_transfer = model;
  unsigned short_sleeps = 0;
  if (mode == "interrupt") elapsed_sleep_us = [&short_sleeps](unsigned us) {
    return ++short_sleeps % 3 == 0 && us > 1 ? us / 2 : us;
  };
  uint8_t buffer[16] = {};
  unsigned completed = 0;
  const double started = fake_ms;
  try { for (unsigned i = 0; i < 25; ++i) {
    if (panda.control_read(0x42, 0, 0, buffer, sizeof(buffer), 1) != 16) break;
    if (panda.bulk_read(0x81, buffer, sizeof(buffer), 1) != 16) break;
    if (panda.control_write(0xdc, 0, 0, 1) != 0) break;
    if (panda.bulk_read(0x81, buffer, sizeof(buffer), 1) != 16) break;
    ++completed;
  }} catch (const std::exception &error) {
    override_transfer = {}; elapsed_sleep_us = {};
    std::printf("{\"passed\":false,\"fixture_stop\":\"%s\",\"header_early\":%u,\"data_early\":%u}\n",
                error.what(), header_early, data_early);
    return 42;
  }
  override_transfer = {};
  elapsed_sleep_us = {};
  const bool good = completed == 25 && header_early == 0 && data_early == 0 && transactions == 100;
  std::printf("{\"passed\":%s,\"model\":\"%s\",\"transactions\":%u,\"header_early\":%u,\"data_early\":%u,"
              "\"min_hack_gap_ms\":%.6f,\"min_header_margin_ms\":%.6f,\"simulated_elapsed_ms\":%.6f,"
              "\"attempts\":%llu,\"retries\":%llu,\"scope\":\"inert ready-time model, no hardware\"}\n",
              good ? "true" : "false", mode.c_str(), transactions, header_early, data_early,
              min_hack_gap_ms, min_header_margin_ms, fake_ms - started,
              static_cast<unsigned long long>(spi_diagnostics.get(SpiStage::ATTEMPTS)),
              static_cast<unsigned long long>(spi_diagnostics.get(SpiStage::RETRIES)));
  return good ? 0 : 1;
}
