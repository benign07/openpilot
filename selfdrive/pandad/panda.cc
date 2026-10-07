#include "selfdrive/pandad/panda.h"

#include <unistd.h>

#include <cassert>
#include <stdexcept>
#include <vector>
#include <random>

#include "cereal/messaging/messaging.h"
#include "common/swaglog.h"
#include "common/util.h"

const bool PANDAD_MAXOUT = getenv("PANDAD_MAXOUT") != nullptr;

Panda::Panda(std::string serial) {
  handle = std::make_unique<PandaSpiHandle>(serial);
  LOGW("connected to %s over SPI", serial.c_str());

  hw_type = get_hw_type();
  can_reset_communications();
}

bool Panda::connected() {
  return handle->connected;
}

bool Panda::comms_healthy() {
  return handle->comms_healthy;
}

std::string Panda::hw_serial() {
  return handle->hw_serial;
}

std::vector<std::string> Panda::list() {
  return PandaSpiHandle::list();
}

void Panda::set_safety_model(cereal::CarParams::SafetyModel safety_model, uint16_t safety_param) {
  lx3_guard_ = false;
  lx3_epoch_ = 0;
  lx3_sequence_ = 0;
  const bool guarded = safety_model == cereal::CarParams::SafetyModel::HYUNDAI_CANFD && (safety_param & 2048U) != 0U;
  if (guarded && !get_lx3_status()) {
    // Old firmware ignores unknown safety bits. Never send it 190|2048 and
    // accidentally activate the legacy unrestricted policy.
    LOGE("LX3 authority v3 firmware capability missing");
    handle->control_write(0xdc, (uint16_t)cereal::CarParams::SafetyModel::NO_OUTPUT, 0);
    return;
  }
  handle->control_write(0xdc, (uint16_t)safety_model, safety_param);
  if (guarded) {
    const auto initial = get_lx3_status();
    if (!initial || initial->profile != 1U) {
      LOGE("LX3 authority profile rejected");
      handle->control_write(0xdc, (uint16_t)cereal::CarParams::SafetyModel::NO_OUTPUT, 0);
      return;
    }
    std::random_device random;
    uint64_t epoch = 0;
    do { epoch = (uint64_t(random()) << 32U) | uint64_t(random()); } while (epoch == 0U);
    handle->control_write(LX3_EPOCH_HIGH_REQUEST, epoch >> 48U, epoch >> 32U);
    handle->control_write(LX3_EPOCH_LOW_REQUEST, epoch >> 16U, epoch);
    const auto sealed = get_lx3_status();
    if (!sealed || sealed->epoch != epoch) {
      LOGE("LX3 authority transport incarnation failed");
      handle->control_write(0xdc, (uint16_t)cereal::CarParams::SafetyModel::NO_OUTPUT, 0);
      return;
    }
    lx3_guard_ = true;
    lx3_epoch_ = epoch;
  }
}

std::optional<lx3_status_t> Panda::get_lx3_status() {
  lx3_status_t status{};
  const int n = handle->control_read(LX3_STATUS_REQUEST, 0, 0, reinterpret_cast<unsigned char*>(&status), sizeof(status));
  return lx3_status_valid(&status, n) ? std::make_optional(status) : std::nullopt;
}

void Panda::send_lx3_state(cereal::Lx3Authority::Reader a, bool fresh) {
  if (!lx3_guard_) return;
  fresh = fresh && a.getVersion() == LX3_PROTOCOL_VERSION && a.getEpoch() == lx3_epoch_;
  const uint16_t key = fresh ? a.getDecisionKey() : 0U;
  const uint8_t button = key >> 8U;
  const uint8_t kind = button == 128U ? 1U : button == 8U ? 2U : button == 1U ? 3U : button == 2U ? 4U : 0U;
  const uint16_t value = fresh ? ((key & 255U) << 8U) | (kind << 4U) | (a.getAutoResume() ? 4U : 0U) | a.getIntent() : 0x80U;
  const uint16_t generation = fresh ? a.getPendingGeneration() : 0U;
  const uint16_t config = fresh ? a.getConfig() : 0U;
  const uint16_t long_ms = fresh ? a.getLongPressMs() : 700U;
  const uint16_t revision = fresh ? a.getObservedLongRevision() : 0U;
  if (lx3_sequence_ == UINT32_MAX) {
    set_safety_model(cereal::CarParams::SafetyModel::NO_OUTPUT);
    return;
  }
  const uint32_t sequence = ++lx3_sequence_;
  const uint64_t binding = lx3_state_binding(lx3_epoch_, sequence, value, generation, config, long_ms, revision);
  handle->control_write(LX3_CONFIG_REQUEST, config, long_ms);
  handle->control_write(LX3_SEQUENCE_REQUEST, sequence >> 16U, sequence);
  handle->control_write(LX3_REVOKE_ACK_REQUEST, revision, 0U);
  handle->control_write(LX3_BINDING_HIGH_REQUEST, binding >> 48U, binding >> 32U);
  handle->control_write(LX3_BINDING_LOW_REQUEST, binding >> 16U, binding);
  lx3_status_t status{};
  const int n = handle->control_read(LX3_STATE_REQUEST, value, generation,
    reinterpret_cast<unsigned char*>(&status), sizeof(status));
  if (!lx3_status_valid(&status, n) || status.epoch != lx3_epoch_ || status.sequence != sequence) {
    LOGE("LX3 STATE not acknowledged");
  }
}

void Panda::set_alternative_experience(uint16_t alternative_experience) {
  handle->control_write(0xdf, alternative_experience, 0);
}

std::string Panda::serial_read(int port_number) {
  std::string ret;
  char buffer[USBPACKET_MAX_SIZE] = {};

  while (true) {
    int bytes_read = handle->control_read(0xe0, port_number, 0, (unsigned char *)buffer, USBPACKET_MAX_SIZE);
    if (bytes_read <= 0) {
      break;
    }
    ret.append(buffer, bytes_read);
  }

  return ret;
}

void Panda::set_uart_baud(int uart, int rate) {
  handle->control_write(0xe4, uart, int(rate / 300));
}

cereal::PandaState::PandaType Panda::get_hw_type() {
  unsigned char hw_query[1] = {0};

  handle->control_read(0xc1, 0, 0, hw_query, 1);
  return (cereal::PandaState::PandaType)(hw_query[0]);
}

void Panda::set_fan_speed(uint16_t fan_speed) {
  handle->control_write(0xb1, fan_speed, 0);
}

uint16_t Panda::get_fan_speed() {
  uint16_t fan_speed_rpm = 0;
  handle->control_read(0xb2, 0, 0, (unsigned char*)&fan_speed_rpm, sizeof(fan_speed_rpm));
  return fan_speed_rpm;
}

void Panda::set_ir_pwr(uint16_t ir_pwr) {
  handle->control_write(0xb0, ir_pwr, 0);
}

std::optional<health_t> Panda::get_state() {
  health_t health {0};
  int err = handle->control_read(0xd2, 0, 0, (unsigned char*)&health, sizeof(health));
  return err >= 0 ? std::make_optional(health) : std::nullopt;
}

std::optional<can_health_t> Panda::get_can_state(uint16_t can_number) {
  can_health_t can_health {0};
  int err = handle->control_read(0xc2, can_number, 0, (unsigned char*)&can_health, sizeof(can_health));
  return err >= 0 ? std::make_optional(can_health) : std::nullopt;
}

void Panda::set_loopback(bool loopback) {
  handle->control_write(0xe5, loopback, 0);
}

std::optional<std::vector<uint8_t>> Panda::get_firmware_version() {
  std::vector<uint8_t> fw_sig_buf(128);
  int read_1 = handle->control_read(0xd3, 0, 0, &fw_sig_buf[0], 64);
  int read_2 = handle->control_read(0xd4, 0, 0, &fw_sig_buf[64], 64);
  return ((read_1 == 64) && (read_2 == 64)) ? std::make_optional(fw_sig_buf) : std::nullopt;
}

std::optional<std::string> Panda::get_serial() {
  char serial_buf[17] = {'\0'};
  int err = handle->control_read(0xd0, 0, 0, (uint8_t*)serial_buf, 16);
  return err >= 0 ? std::make_optional(serial_buf) : std::nullopt;
}

bool Panda::up_to_date() {
  if (auto fw_sig = get_firmware_version()) {
    for (auto fn : { "panda.bin.signed", "panda_h7.bin.signed" }) {
      auto content = util::read_file(std::string("../../panda/board/obj/") + fn);
      if (content.size() >= fw_sig->size() &&
          memcmp(content.data() + content.size() - fw_sig->size(), fw_sig->data(), fw_sig->size()) == 0) {
        return true;
      }
    }
  }
  return false;
}

void Panda::set_power_saving(bool power_saving) {
  handle->control_write(0xe7, power_saving, 0);
}

void Panda::enable_deepsleep() {
  handle->control_write(0xfb, 0, 0);
}

void Panda::send_heartbeat(bool engaged) {
  handle->control_write(0xf3, engaged, 0);
}

void Panda::set_can_speed_kbps(uint16_t bus, uint16_t speed) {
  handle->control_write(0xde, bus, (speed * 10));
}

void Panda::set_can_fd_auto(uint16_t bus, bool enabled) {
  handle->control_write(0xe8, bus, enabled);
}

void Panda::set_data_speed_kbps(uint16_t bus, uint16_t speed) {
  handle->control_write(0xf9, bus, (speed * 10));
}

void Panda::set_canfd_non_iso(uint16_t bus, bool non_iso) {
  handle->control_write(0xfc, bus, non_iso);
}

static uint8_t len_to_dlc(uint8_t len) {
  if (len <= 8) {
    return len;
  }
  if (len <= 24) {
    return 8 + ((len - 8) / 4) + ((len % 4) ? 1 : 0);
  } else {
    return 11 + (len / 16) + ((len % 16) ? 1 : 0);
  }
}

void Panda::pack_can_buffer(const capnp::List<cereal::CanData>::Reader &can_data_list,
                            std::function<void(uint8_t *, size_t)> write_func) {
  int32_t pos = 0;
  uint8_t send_buf[2 * USB_TX_SOFT_LIMIT];

  for (const auto &cmsg : can_data_list) {
    // check if the message is intended for this panda
    uint8_t bus = cmsg.getSrc();
    if (bus >= PANDA_BUS_OFFSET) {
      continue;
    }
    auto can_data = cmsg.getDat();
    uint8_t data_len_code = len_to_dlc(can_data.size());
    assert(can_data.size() <= 64);
    assert(can_data.size() == dlc_to_len[data_len_code]);

    can_header header = {};
    header.addr = cmsg.getAddress();
    header.extended = (cmsg.getAddress() >= 0x800) ? 1 : 0;
    header.data_len_code = data_len_code;
    header.bus = bus;
    header.checksum = 0;

    memcpy(&send_buf[pos], (uint8_t *)&header, sizeof(can_header));
    memcpy(&send_buf[pos + sizeof(can_header)], (uint8_t *)can_data.begin(), can_data.size());
    uint32_t msg_size = sizeof(can_header) + can_data.size();

    // set checksum
    ((can_header *) &send_buf[pos])->checksum = calculate_checksum(&send_buf[pos], msg_size);

    const auto ci = cmsg.getLx3Identity();
    if (lx3_tx_guarded_address(cmsg.getAddress()) && ci.getValid()) {
      const lx3_tx_identity_t identity{ci.getEpoch(), ci.getGeneration(), ci.getAxis(), true};
      uint8_t prefix[8], epoch[8];
      lx3_tx_encode(&identity, &send_buf[pos], msg_size, prefix, epoch);
      constexpr size_t marker_size = sizeof(can_header) + 8U;
      memmove(&send_buf[pos + 2U * marker_size], &send_buf[pos], msg_size);
      for (unsigned i = 0U; i < 2U; i++) {
        can_header marker{};
        marker.bus = LX3_TX_MARKER_BUS;
        marker.addr = i == 0U ? LX3_TX_PREFIX_ADDR : LX3_TX_EPOCH_ADDR;
        marker.extended = 1U;
        marker.data_len_code = 8U;
        uint8_t *start = &send_buf[pos + i * marker_size];
        memcpy(start, &marker, sizeof(marker));
        memcpy(start + sizeof(marker), i == 0U ? prefix : epoch, 8U);
        reinterpret_cast<can_header*>(start)->checksum = calculate_checksum(start, marker_size);
      }
      pos += 2U * marker_size;
    }

    pos += msg_size;

    if (pos >= USB_TX_SOFT_LIMIT) {
      write_func(send_buf, pos);
      pos = 0;
    }
  }

  // send remaining packets
  if (pos > 0) write_func(send_buf, pos);
}

void Panda::can_send(const capnp::List<cereal::CanData>::Reader &can_data_list) {
  pack_can_buffer(can_data_list, [=](uint8_t* data, size_t size) {
    handle->bulk_write(3, data, size, 5);
  });
}

bool Panda::can_receive(std::vector<can_frame>& out_vec) {
  // Check if enough space left in buffer to store RECV_SIZE data
  assert(receive_buffer_size + RECV_SIZE <= sizeof(receive_buffer));

  int recv = handle->bulk_read(0x81, &receive_buffer[receive_buffer_size], RECV_SIZE);
  if (!comms_healthy()) {
    return false;
  }

  if (PANDAD_MAXOUT) {
    static uint8_t junk[RECV_SIZE];
    handle->bulk_read(0xab, junk, RECV_SIZE - recv);
  }

  bool ret = true;
  if (recv > 0) {
    receive_buffer_size += recv;
    ret = unpack_can_buffer(receive_buffer, receive_buffer_size, out_vec);
  }
  return ret;
}

void Panda::can_reset_communications() {
  handle->control_write(0xc0, 0, 0);
}

bool Panda::unpack_can_buffer(uint8_t *data, uint32_t &size, std::vector<can_frame> &out_vec) {
  int pos = 0;

  while (pos <= size - sizeof(can_header)) {
    can_header header;
    memcpy(&header, &data[pos], sizeof(can_header));

    const uint8_t data_len = dlc_to_len[header.data_len_code];
    if (pos + sizeof(can_header) + data_len > size) {
      // we don't have all the data for this message yet
      break;
    }

    if (calculate_checksum(&data[pos], sizeof(can_header) + data_len) != 0) {
      LOGE("Panda CAN checksum failed");
      size = 0;
      can_reset_communications();
      return false;
    }

    can_frame &canData = out_vec.emplace_back();
    canData.address = header.addr;
    canData.src = header.bus;
    if (header.rejected) {
      canData.src += CAN_REJECTED_BUS_OFFSET;
    }
    if (header.returned) {
      canData.src += CAN_RETURNED_BUS_OFFSET;
    }

    canData.dat.assign((char *)&data[pos + sizeof(can_header)], data_len);

    pos += sizeof(can_header) + data_len;
  }

  // move the overflowing data to the beginning of the buffer for the next round
  memmove(data, &data[pos], size - pos);
  size -= pos;

  return true;
}

uint8_t Panda::calculate_checksum(uint8_t *data, uint32_t len) {
  uint8_t checksum = 0U;
  for (uint32_t i = 0U; i < len; i++) {
    checksum ^= data[i];
  }
  return checksum;
}
