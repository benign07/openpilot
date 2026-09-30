/*
  CAN transactions to and from the host come in the form of
  a certain number of CANPacket_t. The transaction is split
  into multiple transfers or chunks.

  * comms_can_read outputs this buffer in chunks of a specified length.
    chunks are always the given length, except the last one.
  * comms_can_write reads in this buffer in chunks.
  * both functions maintain an overflow buffer for a partial CANPacket_t that
    spans multiple transfers/chunks.
  * the overflow buffers are reset by a dedicated control transfer handler,
    which is sent by the host on each start of a connection.
*/

#include "lx3_transport.h"

// This stream has one writer (SPI on tici; USB on USB hosts). State belongs to
// the stream decoder, survives transfer fragments, and is consumed exactly once.
static uint8_t lx3_tx_prefix[8];
static uint8_t lx3_tx_epoch[8];
static uint8_t lx3_tx_stage = 0U;

static void comms_can_dispatch(CANPacket_t *pkt) {
  const bool prefix = (pkt->bus == LX3_TX_MARKER_BUS) && (pkt->addr == LX3_TX_PREFIX_ADDR);
  const bool epoch = (pkt->bus == LX3_TX_MARKER_BUS) && (pkt->addr == LX3_TX_EPOCH_ADDR);
  if (prefix || epoch) {
    const bool valid = (GET_LEN(pkt) == 8U) && pkt->extended && !pkt->returned &&
                       !pkt->rejected && can_check_checksum(pkt);
    if (prefix) {
      lx3_tx_stage = valid ? 1U : 0U;
      if (valid) (void)memcpy(lx3_tx_prefix, pkt->data, 8U);
    } else if (valid && (lx3_tx_stage == 1U)) {
      (void)memcpy(lx3_tx_epoch, pkt->data, 8U);
      lx3_tx_stage = 2U;
    } else {
      lx3_tx_stage = 0U;
    }
    return;  // Marker packets never enter vehicle TX or rejected-echo queues.
  }
  const lx3_tx_identity_t identity = lx3_tx_decode(lx3_tx_prefix, lx3_tx_epoch);
  const uint16_t expected = (uint16_t)lx3_tx_prefix[6] | ((uint16_t)lx3_tx_prefix[7] << 8U);
  const bool paired = (lx3_tx_stage == 2U) && can_check_checksum(pkt) &&
    (expected == lx3_tx_binding(lx3_tx_prefix, lx3_tx_epoch, (const uint8_t *)pkt,
                               CANPACKET_HEAD_SIZE + GET_LEN(pkt)));
  lx3_tx_stage = 0U;  // Consume even for an unrelated, malformed or rejected CAN.
  ENTER_CRITICAL();
  if (safety_lx3_guarded() && lx3_tx_guarded_address(pkt->addr)) {
    const lx3_permission_t state = safety_lx3_permission();
    if (!paired || !lx3_tx_matches(&identity, &state)) can_reject(pkt);
    else can_send(pkt, pkt->bus, false);
  } else {
    can_send(pkt, pkt->bus, false);  // Existing non-guarded/legacy policy.
  }
  EXIT_CRITICAL();
}

typedef struct {
  uint32_t ptr;
  uint32_t tail_size;
  uint8_t data[72];
} asm_buffer;

static asm_buffer can_read_buffer = {.ptr = 0U, .tail_size = 0U};

int comms_can_read(uint8_t *data, uint32_t max_len) {
  uint32_t pos = 0U;

  // Send tail of previous message if it is in buffer
  if (can_read_buffer.ptr > 0U) {
    uint32_t overflow_len = MIN(max_len - pos, can_read_buffer.ptr);
    (void)memcpy(&data[pos], can_read_buffer.data, overflow_len);
    pos += overflow_len;
    (void)memcpy(can_read_buffer.data, &can_read_buffer.data[overflow_len], can_read_buffer.ptr - overflow_len);
    can_read_buffer.ptr -= overflow_len;
  }

  if (can_read_buffer.ptr == 0U) {
    // Fill rest of buffer with new data
    CANPacket_t can_packet;
    while ((pos < max_len) && can_pop(&can_rx_q, &can_packet)) {
      uint32_t pckt_len = CANPACKET_HEAD_SIZE + dlc_to_len[can_packet.data_len_code];
      if ((pos + pckt_len) <= max_len) {
        (void)memcpy(&data[pos], (uint8_t*)&can_packet, pckt_len);
        pos += pckt_len;
      } else {
        (void)memcpy(&data[pos], (uint8_t*)&can_packet, max_len - pos);
        can_read_buffer.ptr += pckt_len - (max_len - pos);
        // cppcheck-suppress objectIndex
        (void)memcpy(can_read_buffer.data, &((uint8_t*)&can_packet)[(max_len - pos)], can_read_buffer.ptr);
        pos = max_len;
      }
    }
  }

  return pos;
}

static asm_buffer can_write_buffer = {.ptr = 0U, .tail_size = 0U};

// send on CAN
void comms_can_write(const uint8_t *data, uint32_t len) {
  uint32_t pos = 0U;

  // Assembling can message with data from buffer
  if (can_write_buffer.ptr != 0U) {
    if (can_write_buffer.tail_size <= (len - pos)) {
      // we have enough data to complete the buffer
      CANPacket_t to_push = {0};
      (void)memcpy(&can_write_buffer.data[can_write_buffer.ptr], &data[pos], can_write_buffer.tail_size);
      can_write_buffer.ptr += can_write_buffer.tail_size;
      pos += can_write_buffer.tail_size;

      // send out
      (void)memcpy((uint8_t*)&to_push, can_write_buffer.data, can_write_buffer.ptr);
      comms_can_dispatch(&to_push);

      // reset overflow buffer
      can_write_buffer.ptr = 0U;
      can_write_buffer.tail_size = 0U;
    } else {
      // maybe next time
      uint32_t data_size = len - pos;
      (void) memcpy(&can_write_buffer.data[can_write_buffer.ptr], &data[pos], data_size);
      can_write_buffer.tail_size -= data_size;
      can_write_buffer.ptr += data_size;
      pos += data_size;
    }
  }

  // rest of the message
  while (pos < len) {
    uint32_t pckt_len = CANPACKET_HEAD_SIZE + dlc_to_len[(data[pos] >> 4U)];
    if ((pos + pckt_len) <= len) {
      CANPacket_t to_push = {0};
      (void)memcpy((uint8_t*)&to_push, &data[pos], pckt_len);
      comms_can_dispatch(&to_push);
      pos += pckt_len;
    } else {
      (void)memcpy(can_write_buffer.data, &data[pos], len - pos);
      can_write_buffer.ptr = len - pos;
      can_write_buffer.tail_size = pckt_len - can_write_buffer.ptr;
      pos += can_write_buffer.ptr;
    }
  }

  refresh_can_tx_slots_available();
}

void comms_can_reset(void) {
  lx3_tx_stage = 0U;
  safety_lx3_reset_transport_ack();
  can_write_buffer.ptr = 0U;
  can_write_buffer.tail_size = 0U;
  can_read_buffer.ptr = 0U;
  can_read_buffer.tail_size = 0U;
}

// TODO: make this more general!
void refresh_can_tx_slots_available(void) {
  if (can_tx_check_min_slots_free(MAX_CAN_MSGS_PER_USB_BULK_TRANSFER)) {
    can_tx_comms_resume_usb();
  }
  if (can_tx_check_min_slots_free(MAX_CAN_MSGS_PER_SPI_BULK_TRANSFER)) {
    can_tx_comms_resume_spi();
  }
}
