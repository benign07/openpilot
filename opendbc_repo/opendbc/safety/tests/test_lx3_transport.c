// Real firmware CAN stream decoder + actual safety policy; no hardware I/O.
#define main lx3_existing_policy_regressions
#include "test_lx3_native.c"
#undef main

static unsigned int accepted_can;
static unsigned int rejected_can;
void can_reject(CANPacket_t *pkt) { (void)pkt; rejected_can++; }
static int can_rx_q;
bool can_pop(int *queue, CANPacket_t *packet_out) { (void)queue; (void)packet_out; return false; }
void can_send(CANPacket_t *pkt, uint8_t bus, bool skip) {
  (void)bus;
  assert(!skip);
  safety_tx_buffered_for_fwd = false;
  if (safety_tx_hook(pkt)) accepted_can++; else rejected_can++;
  safety_tx_buffered_for_fwd = false;
}
bool can_tx_check_min_slots_free(uint32_t count) { (void)count; return true; }
void can_tx_comms_resume_usb(void) { }
void can_tx_comms_resume_spi(void) { }
void refresh_can_tx_slots_available(void);
#define MAX_CAN_MSGS_PER_USB_BULK_TRANSFER 64U
#define MAX_CAN_MSGS_PER_SPI_BULK_TRANSFER 64U
#ifndef ENTER_CRITICAL
#define ENTER_CRITICAL() do { } while (0)
#define EXIT_CRITICAL() do { } while (0)
#endif
#include "../../../../panda/board/can_comms.h"
_Static_assert(__builtin_offsetof(CANPacket_t, data) == CANPACKET_HEAD_SIZE, "firmware CAN wire header must be 6 bytes");

static void combined_session(void) {
  reset(lx3_param()); physical_baseline();
  physical_button(1U); physical_button(0U); acknowledge_request();
  assert(controls_allowed && lx3_mode == 2);
  comms_can_reset(); accepted_can = 0U; rejected_can = 0U;
}

static lx3_tx_identity_t current_identity(void) {
  const lx3_permission_t state = safety_lx3_permission();
  const lx3_tx_identity_t id = {state.transport_epoch, state.generation, state.physical_counter,
                              state.accepted_mode, true};
  return id;
}

static uint32_t encode_pair(CANPacket_t p, lx3_tx_identity_t id, uint8_t out[100]) {
  CANPacket_t first = packet(LX3_TX_PREFIX_ADDR, LX3_TX_MARKER_BUS, 8U);
  CANPacket_t second = packet(LX3_TX_EPOCH_ADDR, LX3_TX_MARKER_BUS, 8U);
  first.extended = 1U; second.extended = 1U;
  can_set_checksum(&p);
  const uint32_t length = CANPACKET_HEAD_SIZE + GET_LEN(&p);
  lx3_tx_encode(&id, (const uint8_t *)&p, length, first.data, second.data);
  can_set_checksum(&first); can_set_checksum(&second);
  memcpy(out, &first, 14U); memcpy(&out[14], &second, 14U); memcpy(&out[28], &p, length);
  return length + 28U;
}

static void feed(const uint8_t *data, uint32_t length, uint32_t fragment) {
  for (uint32_t i = 0U; i < length; i += fragment) {
    comms_can_write(&data[i], MIN(fragment, length - i));
  }
}

static void stage_heartbeat(uint64_t epoch, uint16_t value, uint16_t generation) {
  const uint64_t binding = lx3_heartbeat_binding(epoch, value, generation);
  safety_lx3_stage_transport_ack(true, (uint16_t)(binding >> 48U), (uint16_t)(binding >> 32U));
  safety_lx3_stage_transport_ack(false, (uint16_t)(binding >> 16U), (uint16_t)binding);
}

static void heartbeat_transport(void) {
  pending_lateral();
  lx3_permission_t state = safety_lx3_permission();
  uint16_t value = lx3_heartbeat_value(true, 1U, state.generation, state.physical_counter);
  stage_heartbeat(state.transport_epoch, value, state.generation);
  safety_transport_heartbeat(value, state.generation);
  assert(controls_allowed);
  // Committed sessions clear ACK fields. Their steady heartbeat authenticates
  // the accepted incarnation and must KEEP permission without making a request.
  assert(lx3_host_heartbeat_epoch(true, true, false, 0U, state.transport_epoch) == state.transport_epoch);
  assert(lx3_host_heartbeat_epoch(true, true, true, state.transport_epoch, 0U) == state.transport_epoch);
  assert(lx3_host_heartbeat_epoch(false, true, true, state.transport_epoch, state.transport_epoch) == 0U);
  value = lx3_heartbeat_value(true, 0U, 0U, 0U);
  stage_heartbeat(lx3_host_heartbeat_epoch(true, true, false, 0U, state.transport_epoch), value, 0U);
  safety_transport_heartbeat(value, 0U);
  assert(controls_allowed && lx3_mode == 1 && !lx3_pending);
  // Missing/replayed one-shot binding cannot authorize a pending gesture.
  pending_lateral(); state = safety_lx3_permission();
  value = lx3_heartbeat_value(true, 1U, state.generation, state.physical_counter);
  safety_transport_heartbeat(value, state.generation);
  assert(!controls_allowed && !lx3_pending);
  for (unsigned int variant = 0U; variant < 5U; variant++) {
    pending_lateral(); state = safety_lx3_permission();
    value = lx3_heartbeat_value(true, 1U, state.generation, state.physical_counter);
    const uint16_t staged_value = variant == 1U ? (uint16_t)(value ^ 2U) : value;
    const uint16_t staged_gen = variant == 2U ? (uint16_t)(state.generation + 1U) : state.generation;
    stage_heartbeat(state.transport_epoch ^ (variant == 0U ? 1U : 0U), staged_value, staged_gen);
    if (variant == 3U) comms_can_reset();
    if (variant == 4U) safety_lx3_stage_transport_ack(false, 0U, 0U);
    safety_transport_heartbeat(value, state.generation);
    assert(!controls_allowed && !lx3_pending);
  }
  reset(190U); safety_transport_heartbeat(1U, 0U); assert(heartbeat_engaged);
  puts("PASS: epoch/field-bound one-shot heartbeat, malformed/reset/replay rejection and legacy ABI");
}

static void can_transport(void) {
  combined_session();
  CANPacket_t p = packet(0x161U, 0U, 13U);
  hyundai_canfd_update_checksum(&p); can_set_checksum(&p);
  assert(safety_tx_hook(&p));  // A valid policy fixture, not unrelated DLC rejection.
  uint8_t data[100]; const uint32_t length = encode_pair(p, current_identity(), data);
  for (uint32_t split = 1U; split < length; split++) {
    comms_can_reset(); accepted_can = 0U; rejected_can = 0U;
    feed(data, length, split);
    if ((accepted_can != 1U) || (rejected_can != 0U)) {
      const lx3_tx_identity_t decoded = lx3_tx_decode(lx3_tx_prefix, lx3_tx_epoch);
      const lx3_permission_t native = safety_lx3_permission();
      fprintf(stderr, "fragment=%u accepted=%u rejected=%u mode=%u native_mode=%u gen=%u native_gen=%u epoch=%llu native_epoch=%llu\n",
              split, accepted_can, rejected_can, decoded.mode, native.accepted_mode, decoded.generation,
              native.generation, (unsigned long long)decoded.epoch, (unsigned long long)native.transport_epoch);
    }
    assert(accepted_can == 1U && rejected_can == 0U);
  }
  for (uint32_t cut = 1U; cut < length; cut++) {
    comms_can_reset(); accepted_can = 0U; rejected_can = 0U;
    comms_can_write(data, cut); comms_can_write(&data[cut], length - cut);
    assert(accepted_can == 1U && rejected_can == 0U);
  }
  comms_can_reset(); accepted_can = 0U; rejected_can = 0U;
  comms_can_write((const uint8_t *)&p, CANPACKET_HEAD_SIZE + GET_LEN(&p));
  assert(accepted_can == 0U && rejected_can == 1U);
  for (unsigned int field = 0U; field < 7U; field++) {
    lx3_tx_identity_t id = current_identity();
    if (field == 0U) id.epoch ^= 1U;
    if (field == 1U) id.generation++;
    if (field == 2U) id.counter++;
    if (field == 3U) id.mode = 1U;
    if (field == 4U) id.epoch = 0U;
    if (field == 5U) id.generation = 0U;
    if (field == 6U) id.mode = 0U;
    const uint32_t size = encode_pair(p, id, data);
    comms_can_reset(); accepted_can = 0U; rejected_can = 0U;
    feed(data, size, 1U);
    assert(accepted_can == 0U && rejected_can == 1U && controls_allowed);
  }
  // OFF -> new grant: old bytes keep their original identity, never restamped.
  const lx3_tx_identity_t old = current_identity();
  const uint32_t old_length = encode_pair(p, old, data);
  physical_button(128U); physical_button(0U);
  physical_button(1U); physical_button(0U); acknowledge_request();
  assert(controls_allowed && lx3_request_generation != old.generation);
  accepted_can = 0U; rejected_can = 0U;
  feed(data, old_length, 1U); assert(accepted_can == 0U && rejected_can == 1U);
  const uint32_t fresh_length = encode_pair(p, current_identity(), data);
  feed(data, fresh_length, 1U); assert(accepted_can == 1U);
  // Every guarded ID is denied without a marker; metadata never creates grant.
  const unsigned int addresses[] = {0xCBU, 0x12AU, 0x1A0U, 0x161U, 0x162U, 0x1E0U, 0x1EAU, 0x200U, 0x362U, 0x2A4U};
  for (unsigned int i = 0U; i < 10U; i++) {
    CANPacket_t target = packet(addresses[i], 0U, 8U);
    can_set_checksum(&target);
    const unsigned int before = accepted_can;
    comms_can_write((const uint8_t *)&target, 14U);
    assert(accepted_can == before && controls_allowed);
  }
  puts("PASS: actual CAN decoder fragmentation, missing/old/wrong identity, OFF/regrant and all ten owned IDs");
}

static void pairing_integrity(void) {
  combined_session();
  CANPacket_t p = angle_command(true); hyundai_canfd_update_checksum(&p);
  uint8_t original[100], changed[100]; const uint32_t length = encode_pair(p, current_identity(), original);
  for (unsigned int fault = 0U; fault < 6U; fault++) {
    memcpy(changed, original, length);
    if (fault == 0U) changed[5] ^= 1U; // Prefix USB checksum.
    if (fault == 1U) changed[19] ^= 1U; // Epoch USB checksum.
    if (fault == 2U) changed[33] ^= 1U; // CAN USB checksum.
    if (fault == 3U) { changed[12] ^= 1U; changed[5] ^= 1U; } // Valid checksum, bad binding.
    if (fault == 4U) { changed[6] ^= 1U; changed[5] ^= 1U; } // Valid checksum, wrong magic.
    if (fault == 5U) { changed[1] ^= 1U; changed[5] ^= 1U; } // Rejected flag in marker.
    comms_can_reset(); accepted_can = 0U; rejected_can = 0U;
    feed(changed, length, 1U);
    assert(accepted_can == 0U && rejected_can == 1U && controls_allowed);
  }
  comms_can_reset(); accepted_can = 0U; rejected_can = 0U;
  comms_can_write(original, 28U); assert(accepted_can == 0U && rejected_can == 0U);
  comms_can_reset(); comms_can_write(&original[28], length - 28U);
  assert(accepted_can == 0U && rejected_can == 1U);
  // Marker consumed by an unrelated packet, even when that packet is rejected.
  comms_can_reset(); accepted_can = 0U; rejected_can = 0U;
  comms_can_write(original, 28U);
  CANPacket_t unrelated = packet(0x555U, 0U, 8U); can_set_checksum(&unrelated);
  comms_can_write((const uint8_t *)&unrelated, 14U);
  comms_can_write(&original[28], length - 28U); assert(accepted_can == 0U);
  // Back-to-back prefixes replace the first, no authority or pair is inherited.
  comms_can_reset(); accepted_can = 0U; rejected_can = 0U;
  comms_can_write(original, 14U); feed(original, length, 1U);
  assert(accepted_can == 1U && rejected_can == 0U);
  // The next raw CAN cannot reuse a consumed marker.
  comms_can_write(&original[28], length - 28U); assert(accepted_can == 1U && rejected_can == 1U);
  // Legacy CAN v4 still needs no marker; exact markers never go on vehicle CAN.
  reset(190U); comms_can_reset(); accepted_can = 0U; rejected_can = 0U;
  CANPacket_t ui = packet(0x161U, 0U, 13U); can_set_checksum(&ui);
  feed((const uint8_t *)&ui, CANPACKET_HEAD_SIZE + GET_LEN(&ui), 1U);
  assert(accepted_can == 1U && rejected_can == 0U);
  comms_can_write(original, 28U); assert(accepted_can == 1U && rejected_can == 0U);
  puts("PASS: checksum/binding/magic/reset/one-shot/marker replacement and legacy CAN v4 compatibility");
}

static void boot_boundary(void) {
  combined_session();
  CANPacket_t p = packet(0x161U, 0U, 13U);
  uint8_t old_bytes[100]; const lx3_tx_identity_t old = current_identity();
  const uint32_t length = encode_pair(p, old, old_bytes);
  const uint64_t epoch = safety_lx3_transport_epoch();
  assert(!safety_lx3_set_transport_epoch(true, 0U, 1U));
  assert(!safety_lx3_set_transport_epoch(false, 0U, 2U));
  reset(lx3_param()); comms_can_reset(); assert(safety_lx3_transport_epoch() == epoch);
  // Explicit test-only cold reboot: fresh random incarnation, same other tuple.
  lx3_transport_epoch = epoch ^ 0x0101010101010101ULL;
  lx3_request_generation = (uint16_t)(old.generation - 2U);
  combined_session();
  assert(lx3_request_generation == old.generation && lx3_request_counter == old.counter && lx3_mode == old.mode);
  feed(old_bytes, length, 1U); assert(accepted_can == 0U && rejected_can == 1U);
  uint8_t fresh[100]; const uint32_t size = encode_pair(p, current_identity(), fresh);
  feed(fresh, size, 1U); assert(accepted_can == 1U);
  puts("PASS: sealed boot incarnation retained on mode/reset, old boot rejected even with identical generation/counter/mode");
}

static void cpp_heartbeat_stream(const char *path, bool wrong_epoch) {
  pending_lateral();
  assert(lx3_request_generation == 2U && lx3_request_counter == 2U);
  FILE *f = fopen(path, "rb"); assert(f != NULL);
  uint8_t record[5]; unsigned int heartbeats = 0U;
  size_t bytes;
  while ((bytes = fread(record, 1U, sizeof(record), f)) != 0U) {
    assert(bytes == sizeof(record));
    const uint16_t value = (uint16_t)record[1] | ((uint16_t)record[2] << 8U);
    const uint16_t index = (uint16_t)record[3] | ((uint16_t)record[4] << 8U);
    if (record[0] == LX3_ACK_HIGH_REQUEST) safety_lx3_stage_transport_ack(true, value, index);
    else if (record[0] == LX3_ACK_LOW_REQUEST) safety_lx3_stage_transport_ack(false, value, index);
    else {
      assert(record[0] == 0xF3U);
      safety_transport_heartbeat(value, index); heartbeats++;
      assert(controls_allowed == (!wrong_epoch && (heartbeats <= 21U)));
      if (controls_allowed) assert(lx3_mode == 1 && !lx3_pending);
      else assert(lx3_mode == 0 && !lx3_pending);
    }
  }
  assert(!ferror(f)); fclose(f);
  assert(heartbeats == (wrong_epoch ? 1U : 22U));
  puts(wrong_epoch ? "PASS: actual C++ heartbeat control words, wrong incarnation cannot grant"
                  : "PASS: actual C++ heartbeat control words -> native policy: grant, 20 committed heartbeats, identity-less revoke");
}

int main(int argc, char **argv) {
  if (argc == 3) {
    assert((strcmp(argv[1], "--heartbeat") == 0) || (strcmp(argv[1], "--wrong-epoch") == 0));
    cpp_heartbeat_stream(argv[2], strcmp(argv[1], "--wrong-epoch") == 0);
    return 0;
  }
  if (argc == 2) {
    combined_session();
    assert(lx3_request_generation == 2U && lx3_request_counter == 2U);
    FILE *f = fopen(argv[1], "rb"); assert(f != NULL);
    uint8_t byte; while (fread(&byte, 1U, 1U, f) == 1U) comms_can_write(&byte, 1U);
    assert(!ferror(f)); fclose(f);
    assert(accepted_can == 40U && rejected_can == 0U);
    puts("PASS: actual generated C++ Panda pack -> actual firmware stream decoder -> actual native policy, 40 packets");
    return 0;
  }
  heartbeat_transport(); can_transport(); pairing_integrity(); boot_boundary();
  puts("PASS: software transport tests only; not CAN-wire/EPS or vehicle qualification");
  return 0;
}
