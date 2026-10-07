#define CATCH_CONFIG_MAIN
#define CATCH_CONFIG_ENABLE_BENCHMARKING

#include <climits>
#include <array>

#include "catch2/catch.hpp"
#include "cereal/messaging/messaging.h"
#include "common/util.h"
#include "selfdrive/pandad/panda.h"

struct PandaTest : public Panda {
  using Panda::pack_can_buffer;
  using Panda::calculate_checksum;
  PandaTest(int can_list_size, cereal::PandaState::PandaType hw_type);
  void test_can_send();
  void test_can_recv(uint32_t chunk_size = 0);
  void test_chunked_can_recv();

  std::map<int, std::string> test_data;
  int can_list_size = 0;
  int total_pakets_size = 0;
  MessageBuilder msg;
  capnp::List<cereal::CanData>::Reader can_data_list;
};

PandaTest::PandaTest(int can_list_size_, cereal::PandaState::PandaType hw_type_) : can_list_size(can_list_size_), Panda() {
  this->hw_type = hw_type_;
  int data_limit = ((hw_type == cereal::PandaState::PandaType::RED_PANDA) ? std::size(dlc_to_len) : 8);
  // prepare test data
  for (int i = 0; i < data_limit; ++i) {
    std::random_device rd;
    std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char> rbe(rd());

    int data_len = dlc_to_len[i];
    std::string bytes(data_len, '\0');
    std::generate(bytes.begin(), bytes.end(), std::ref(rbe));
    test_data[data_len] = bytes;
  }

  // generate can messages for this panda
  auto can_list = msg.initEvent().initSendcan(can_list_size);
  for (uint8_t i = 0; i < can_list_size; ++i) {
    auto can = can_list[i];
    uint32_t id = util::random_int(0, std::size(dlc_to_len) - 1);
    const std::string &dat = test_data[dlc_to_len[id]];
    can.setAddress(i);
    can.setSrc(util::random_int(0, 2));
    can.setDat(kj::ArrayPtr((uint8_t *)dat.data(), dat.size()));
    total_pakets_size += sizeof(can_header) + dat.size();
  }

  can_data_list = can_list.asReader();
  INFO("test " << can_list_size << " packets, total size " << total_pakets_size);
}

void PandaTest::test_can_send() {
  std::vector<uint8_t> unpacked_data;
  this->pack_can_buffer(can_data_list, [&](uint8_t *chunk, size_t size) {
    unpacked_data.insert(unpacked_data.end(), chunk, &chunk[size]);
  });
  REQUIRE(unpacked_data.size() == total_pakets_size);

  int cnt = 0;
  INFO("test can message integrity");
  for (int pos = 0, pckt_len = 0; pos < unpacked_data.size(); pos += pckt_len) {
    can_header header;
    memcpy(&header, &unpacked_data[pos], sizeof(can_header));
    const uint8_t data_len = dlc_to_len[header.data_len_code];
    pckt_len = sizeof(can_header) + data_len;

    REQUIRE(header.addr == cnt);
    REQUIRE(test_data.find(data_len) != test_data.end());
    const std::string &dat = test_data[data_len];
    REQUIRE(memcmp(dat.data(), &unpacked_data[pos + sizeof(can_header)], dat.size()) == 0);
    ++cnt;
  }
  REQUIRE(cnt == can_list_size);
}

void PandaTest::test_can_recv(uint32_t rx_chunk_size) {
  std::vector<can_frame> frames;
  this->pack_can_buffer(can_data_list, [&](uint8_t *data, uint32_t size) {
    if (rx_chunk_size == 0) {
      REQUIRE(this->unpack_can_buffer(data, size, frames));
    } else {
      this->receive_buffer_size = 0;
      uint32_t pos = 0;

      while (pos < size) {
        uint32_t chunk_size = std::min(rx_chunk_size, size - pos);
        memcpy(&this->receive_buffer[this->receive_buffer_size], &data[pos], chunk_size);
        this->receive_buffer_size += chunk_size;
        pos += chunk_size;

        REQUIRE(this->unpack_can_buffer(this->receive_buffer, this->receive_buffer_size, frames));
      }
    }
  });

  REQUIRE(frames.size() == can_list_size);
  for (int i = 0; i < frames.size(); ++i) {
    REQUIRE(frames[i].address == i);
    REQUIRE(test_data.find(frames[i].dat.size()) != test_data.end());
    const std::string &dat = test_data[frames[i].dat.size()];
    REQUIRE(memcmp(dat.data(), frames[i].dat.data(), dat.size()) == 0);
  }
}

TEST_CASE("send/recv CAN 2.0 packets") {
  auto can_list_size = GENERATE(1, 3, 5, 10, 30, 60, 100, 200);
  PandaTest test(can_list_size, cereal::PandaState::PandaType::DOS);

  SECTION("can_send") {
    test.test_can_send();
  }
  SECTION("can_receive") {
    test.test_can_recv();
  }
  SECTION("chunked_can_receive") {
    test.test_can_recv(0x40);
  }
}

TEST_CASE("send/recv CAN FD packets") {
  auto can_list_size = GENERATE(1, 3, 5, 10, 30, 60, 100, 200);
  PandaTest test(can_list_size, cereal::PandaState::PandaType::RED_PANDA);

  SECTION("can_send") {
    test.test_can_send();
  }
  SECTION("can_receive") {
    test.test_can_recv();
  }
  SECTION("chunked_can_receive") {
    test.test_can_recv(0x40);
  }
}

TEST_CASE("LX3 identities bind one payload without changing CAN packet v4") {
  PandaTest panda(0, cereal::PandaState::PandaType::RED_PANDA);
  MessageBuilder message;
  auto frames = message.initEvent().initSendcan(4);
  const std::array<unsigned, 4> addresses{0xCB, 0x12A, 0x1A0, 0xEA};
  const std::array<unsigned, 4> lengths{24, 16, 32, 24};
  for (unsigned i = 0; i < 4; i++) {
    auto frame = frames[i];
    frame.setAddress(addresses[i]); frame.setSrc(i == 3 ? 2 : 0);
    std::vector<uint8_t> payload(lengths[i], i + 1);
    frame.setDat(kj::ArrayPtr(payload.data(), payload.size()));
    if (i < 3) {
      auto identity = frame.initLx3Identity();
      identity.setEpoch(0x3141592653589793ULL); identity.setGeneration(i + 10);
      identity.setAxis(i == 2 ? 2 : 1); identity.setValid(true);
    }
  }
  std::vector<uint8_t> bytes;
  panda.pack_can_buffer(frames.asReader(), [&](uint8_t *p, size_t n) { bytes.insert(bytes.end(), p, p + n); });
  unsigned pos = 0;
  for (unsigned i = 0; i < 4; i++) {
    std::array<uint8_t, 8> prefix{}, epoch_bytes{};
    if (i < 3) {
      for (unsigned marker = 0; marker < 2; marker++) {
        can_header header{}; memcpy(&header, &bytes[pos], sizeof(header));
        REQUIRE(header.bus == LX3_TX_MARKER_BUS);
        REQUIRE(header.addr == (marker == 0 ? LX3_TX_PREFIX_ADDR : LX3_TX_EPOCH_ADDR));
        REQUIRE(header.data_len_code == 8);
        REQUIRE(panda.calculate_checksum(&bytes[pos], sizeof(header) + 8) == 0);
        memcpy((marker == 0 ? prefix : epoch_bytes).data(), &bytes[pos + sizeof(header)], 8);
        pos += sizeof(header) + 8;
      }
    }
    can_header header{}; memcpy(&header, &bytes[pos], sizeof(header));
    const unsigned size = sizeof(header) + lengths[i];
    REQUIRE(header.addr == addresses[i]); REQUIRE(panda.calculate_checksum(&bytes[pos], size) == 0);
    if (i < 3) {
      const auto identity = lx3_tx_decode(prefix.data(), epoch_bytes.data());
      REQUIRE(identity.valid); REQUIRE(identity.epoch == 0x3141592653589793ULL);
      REQUIRE(identity.generation == i + 10);
      const uint16_t binding = uint16_t(prefix[6]) | (uint16_t(prefix[7]) << 8U);
      REQUIRE(binding == lx3_tx_binding(prefix.data(), epoch_bytes.data(), &bytes[pos], size));
      bytes[pos + sizeof(header)] ^= 1U;
      REQUIRE(binding != lx3_tx_binding(prefix.data(), epoch_bytes.data(), &bytes[pos], size));
    }
    pos += size;
  }
  REQUIRE(pos == bytes.size());
}

TEST_CASE("LX3 capability rejects same-size obsolete firmware before arming") {
  lx3_status_t status{};
  status.version = LX3_PROTOCOL_VERSION;
  REQUIRE(lx3_status_valid(&status, sizeof(status)));
  REQUIRE_FALSE(lx3_status_valid(&status, sizeof(status) - 1));
  status.version = 3;  // Previous 62-byte candidate, no refusal-episode stage.
  REQUIRE_FALSE(lx3_status_valid(&status, sizeof(status)));
  status.version = 0;
  REQUIRE_FALSE(lx3_status_valid(&status, sizeof(status)));
}
