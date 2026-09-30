#define CATCH_CONFIG_MAIN
#define CATCH_CONFIG_ENABLE_BENCHMARKING

#include <climits>
#include <array>
#include <fstream>

#include "catch2/catch.hpp"
#include "cereal/messaging/messaging.h"
#include "common/util.h"
#include "selfdrive/pandad/panda.h"

struct PandaTest : public Panda {
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

struct PandaTransportTest : public Panda {
  using Panda::pack_can_buffer;
  using Panda::calculate_checksum;
  using Panda::pack_heartbeat;
};

TEST_CASE("LX3 actual heartbeat packing preserves committed boot incarnation", "[lx3]") {
  constexpr uint64_t epoch = 0x123456789ABCDEF0ULL;
  // Capnp builders are generated from the production schema, not a test model.
  capnp::MallocMessageBuilder builder;
  auto ss = builder.initRoot<cereal::SelfdriveState>();
  ss.setEnabled(true); ss.setLx3AckValid(true); ss.setLx3AckMode(1U);
  ss.setLx3AckGeneration(2U); ss.setLx3AckPhysicalCounter(2U); ss.setLx3AckTransportEpoch(epoch);
  std::vector<uint8_t> stream;
  auto writer = [&](uint8_t request, uint16_t value, uint16_t index) {
    stream.insert(stream.end(), {request, static_cast<uint8_t>(value), static_cast<uint8_t>(value >> 8U),
                                static_cast<uint8_t>(index), static_cast<uint8_t>(index >> 8U)});
  };
  auto heartbeat = [&]() {
    auto reader = ss.asReader();
    const bool ack = reader.getLx3AckValid();
    const uint64_t selected = lx3_host_heartbeat_epoch(true, reader.getEnabled(), ack,
                               reader.getLx3AckTransportEpoch(), reader.getLx3AcceptedTransportEpoch());
    PandaTransportTest::pack_heartbeat(reader.getEnabled(), true,
      ack ? reader.getLx3AckMode() : 0U, ack ? reader.getLx3AckGeneration() : 0U,
      ack ? reader.getLx3AckPhysicalCounter() : 0U, selected, writer);
  };
  heartbeat();
  REQUIRE(stream.size() == 15U);
  ss.setLx3AckValid(false); ss.setLx3AckMode(0U); ss.setLx3AckGeneration(0U);
  ss.setLx3AckPhysicalCounter(0U); ss.setLx3AckTransportEpoch(0U);
  ss.setLx3AcceptedTransportEpoch(epoch);
  for (unsigned int i = 0U; i < 20U; i++) heartbeat();
  REQUIRE(stream.size() == 21U * 15U);
  // Enabled without either identity revokes; it may not inherit the latest Panda.
  ss.setLx3AcceptedTransportEpoch(0U); heartbeat();
  if (const char *output = getenv("LX3_HEARTBEAT_STREAM_OUT")) {
    std::ofstream file(output, std::ios::binary); REQUIRE(file.good());
    file.write(reinterpret_cast<const char *>(stream.data()), stream.size()); REQUIRE(file.good());
  }
  stream.clear();
  PandaTransportTest::pack_heartbeat(true, true, 1U, 2U, 2U, epoch ^ 1U, writer);
  if (const char *output = getenv("LX3_WRONG_EPOCH_STREAM_OUT")) {
    std::ofstream file(output, std::ios::binary); REQUIRE(file.good());
    file.write(reinterpret_cast<const char *>(stream.data()), stream.size()); REQUIRE(file.good());
  }
  for (bool enabled : {false, true}) {
    stream.clear();
    PandaTransportTest::pack_heartbeat(enabled, false, 2U, 65535U, 255U, epoch, writer);
    REQUIRE((stream == std::vector<uint8_t>{0xF3U, static_cast<uint8_t>(enabled), 0U, 0U, 0U}));
  }
}

TEST_CASE("LX3 immutable producer identity in actual Panda pack", "[lx3]") {
  static_assert(sizeof(can_header) == CANPACKET_HEAD_SIZE);
  MessageBuilder message;
  auto can_list = message.initEvent().initSendcan(40);
  std::array<uint8_t, 32> payload{};
  for (auto item : can_list) {
    item.setAddress(0x161U); item.setSrc(0U);
    item.setDat(kj::arrayPtr(payload.data(), payload.size()));
    item.setLx3IdentityValid(true); item.setLx3Generation(2U); item.setLx3PhysicalCounter(2U);
    item.setLx3Mode(2U); item.setLx3TransportEpoch(0x123456789ABCDEF0ULL);
  }
  PandaTransportTest panda;
  std::vector<uint8_t> stream;
  unsigned int chunks = 0U;
  panda.pack_can_buffer(can_list.asReader(), [&](uint8_t *data, size_t size) {
    REQUIRE(size <= 2U * USB_TX_SOFT_LIMIT);
    stream.insert(stream.end(), data, data + size); chunks++;
  });
  REQUIRE(chunks > 1U);
  REQUIRE(stream.size() == 40U * (28U + sizeof(can_header) + payload.size()));
  size_t position = 0U;
  for (unsigned int i = 0U; i < 40U; i++) {
    can_header prefix, epoch, target;
    memcpy(&prefix, &stream[position], sizeof(prefix));
    memcpy(&epoch, &stream[position + 14U], sizeof(epoch));
    memcpy(&target, &stream[position + 28U], sizeof(target));
    REQUIRE(prefix.bus == LX3_TX_MARKER_BUS); REQUIRE(prefix.addr == LX3_TX_PREFIX_ADDR);
    REQUIRE(epoch.bus == LX3_TX_MARKER_BUS); REQUIRE(epoch.addr == LX3_TX_EPOCH_ADDR);
    REQUIRE(prefix.extended == 1U); REQUIRE(epoch.extended == 1U);
    REQUIRE(prefix.data_len_code == 8U); REQUIRE(epoch.data_len_code == 8U);
    REQUIRE(target.addr == 0x161U); REQUIRE(target.bus == 0U);
    const uint8_t *p = &stream[position + sizeof(can_header)];
    const uint8_t *e = &stream[position + 14U + sizeof(can_header)];
    const auto identity = lx3_tx_decode(p, e);
    REQUIRE(identity.generation == 2U); REQUIRE(identity.counter == 2U); REQUIRE(identity.mode == 2U);
    REQUIRE(identity.epoch == 0x123456789ABCDEF0ULL);
    REQUIRE(lx3_tx_binding(p, e, &stream[position + 28U], sizeof(can_header) + payload.size()) ==
            (static_cast<uint16_t>(p[6]) | (static_cast<uint16_t>(p[7]) << 8U)));
    REQUIRE(panda.calculate_checksum(&stream[position], 14U) == 0U);
    REQUIRE(panda.calculate_checksum(&stream[position + 14U], 14U) == 0U);
    REQUIRE(panda.calculate_checksum(&stream[position + 28U], sizeof(can_header) + payload.size()) == 0U);
    REQUIRE(memcmp(&stream[position + 28U + sizeof(can_header)], payload.data(), payload.size()) == 0);
    position += 28U + sizeof(can_header) + payload.size();
  }
  if (const char *output = getenv("LX3_TRANSPORT_STREAM_OUT")) {
    std::ofstream file(output, std::ios::binary);
    REQUIRE(file.good());
    file.write(reinterpret_cast<const char *>(stream.data()), stream.size());
    REQUIRE(file.good());
  }
  // Old producers, malformed identity and diagnostic frames keep raw v4 ABI.
  for (unsigned int variant = 0U; variant < 4U; variant++) {
    for (auto item : can_list) {
      item.setLx3IdentityValid(variant != 0U);
      item.setLx3TransportEpoch(variant == 1U ? 0U : 0x123456789ABCDEF0ULL);
      item.setAddress(variant == 2U ? 0x730U : 0x161U);
      item.setSrc(variant == 3U ? PANDA_BUS_OFFSET : 0U);
    }
    size_t bytes = 0U;
    panda.pack_can_buffer(can_list.asReader(), [&](uint8_t *data, size_t size) { (void)data; bytes += size; });
    REQUIRE(bytes == (variant == 3U ? 0U : 40U * (sizeof(can_header) + payload.size())));
  }
}
