#pragma once

#include "safety_declarations.h"
#include "safety_hyundai_common.h"
#include "lx3_permission.h"

// Explicit opt-in: the legacy flag combination (e.g. 190) also identifies
// other vehicles. This development policy must not be inferred from it.
const uint16_t HYUNDAI_PARAM_LX3_ENGAGEMENT_GUARD = 1024U;
const int HYUNDAI_LX3_MAX_ANGLE = 1750;
const int HYUNDAI_LX3_MAX_TORQUE = 250;
static bool hyundai_canfd_lx3_guard = false;
static bool lx3_mdps_seen = false;
static bool lx3_mdps_fault = false;
static uint32_t lx3_mdps_us = 0U;
static int lx3_measured_angle = 0;
static bool lx3_angle_active_prev = false;
static int lx3_angle_accepted = 0;
static uint32_t lx3_angle_accepted_us = 0U;
// USB acceptance and insertion into an original vehicle frame are distinct.
// Track the output budget separately so unsent queued goals cannot spend it.
static bool lx3_angle_forwarded_active_prev = false;
static int lx3_angle_forwarded = 0;
static uint32_t lx3_angle_forwarded_us = 0U;
static int lx3_mode = 0;  // 0 OFF, 1 lateral only, 2 combined; same as host.
static int lx3_requested_mode = 0;
static bool lx3_pending = false;
// Never reset this on a safety-mode change: old ACKs must not match a new request.
static uint16_t lx3_request_generation = 0U;
static bool lx3_generation_exhausted = false;
static uint8_t lx3_request_counter = 0U;
static uint32_t lx3_request_us = 0U;
static bool lx3_button_seen = false;
static bool lx3_button_ready = false;
static uint8_t lx3_button_counter = 0U;
static uint32_t lx3_button_us = 0U;
static unsigned int lx3_neutral_samples = 0U;
static bool lx3_main_held = false;
static uint32_t lx3_main_us = 0U;
static uint8_t lx3_main_release_counter = 0U;
static int lx3_button_prev = 0;

// Software envelope derived from this port's host maximum of 2 deg/10ms.
// EPS/OEM qualification and speed-dependent lateral acceleration remain separate.
static bool lx3_angle_context_valid(int torque) {
  const uint32_t now = microsecond_timer_get();
  const bool driver_override = (torque_driver.min < -250) || (torque_driver.max > 250);
  return lx3_mdps_seen && !lx3_mdps_fault && (now - lx3_mdps_us <= 50000U) &&
         (!driver_override || (torque <= 25));
}

static bool lx3_angle_violation(int angle, int active, int torque) {
  if (active != 2) return false;
  if (!lx3_angle_context_valid(torque)) return true;
  const uint32_t now = microsecond_timer_get();
  const int reference = lx3_angle_active_prev ? lx3_angle_accepted : lx3_measured_angle;
  const uint32_t elapsed = lx3_angle_active_prev ? MIN(now - lx3_angle_accepted_us, 10000U) : 10000U;
  const int max_delta = (int)((elapsed * 20U) / 10000U) + 1;
  return ABS(angle - reference) > max_delta;
}

static bool hyundai_canfd_actuator_addr(int addr) {
  return (addr == 0xCB) || (addr == 0x12A) || (addr == 0x1A0);
}

static bool hyundai_canfd_lx3_display_addr(int addr) {
  return (addr == 0x161) || (addr == 0x162) || (addr == 0x1E0) || (addr == 0x1EA) || (addr == 0x200);
}

static bool hyundai_canfd_actuator_active(const CANPacket_t *pkt) {
  const int addr = GET_ADDR(pkt);
  if (addr == 0xCB) {
    return ((GET_BYTE(pkt, 3) >> 4) & 0x3U) == 2U;
  }
  if (addr == 0x12A) {
    return GET_BIT(pkt, 52U);
  }
  return (addr == 0x1A0) && (((GET_BYTE(pkt, 8) >> 4) & 0x7U) != 0U);
}

static bool lx3_angle_forward_violation(const CANPacket_t *pkt) {
  if (((GET_BYTE(pkt, 3) >> 4) & 0x3U) != 2U) return false;
  const int angle = to_signed(GET_BYTES(pkt, 4, 2) & 0x3FFFU, 14);
  const uint32_t now = microsecond_timer_get();
  const bool continuous = lx3_angle_forwarded_active_prev && (now - lx3_angle_forwarded_us < 30000U);
  const int reference = continuous ? lx3_angle_forwarded : lx3_measured_angle;
  const uint32_t elapsed = continuous ? MIN(now - lx3_angle_forwarded_us, 10000U) : 10000U;
  const int max_delta = (int)((elapsed * 20U) / 10000U) + 1;
  return ABS(angle - reference) > max_delta;
}

static bool lx3_neutral_gas_override(const CANPacket_t *pkt) {
  const int raw = (((GET_BYTE(pkt, 17) & 0x7U) << 8) | GET_BYTE(pkt, 16)) - 1023U;
  const int val = ((GET_BYTE(pkt, 18) << 4) | (GET_BYTE(pkt, 17) >> 4)) - 1023U;
  return (GET_ADDR(pkt) == 0x1A0) && gas_pressed &&
         (((GET_BYTE(pkt, 8) >> 4) & 0x7U) == 2U) &&
         (raw == 0) && (val == 0) && ((GET_BYTE(pkt, 23) & 0x3U) == 0U);
}

const TorqueSteeringLimits HYUNDAI_CANFD_STEERING_LIMITS = {
  .max_steer = 512, //270,
  .max_rt_delta = 112,
  .max_rt_interval = 250000,
  .max_rate_up = 2,
  .max_rate_down = 3,
  .driver_torque_allowance = 250,
  .driver_torque_multiplier = 2,
  .type = TorqueDriverLimited,

  // the EPS faults when the steering angle is above a certain threshold for too long. to prevent this,
  // we allow setting torque actuation bit to 0 while maintaining the requested torque value for two consecutive frames
  .min_valid_request_frames = 89,
  .max_invalid_request_frames = 2,
  .min_valid_request_rt_interval = 810000,  // 810ms; a ~10% buffer on cutting every 90 frames
  .has_steer_req_tolerance = true,
};

const CanMsg HYUNDAI_CANFD_HDA2_TX_MSGS[] = {
  {0x50, 0, 16},  // LKAS
  {0x1CF, 1, 8},  // CRUISE_BUTTON
  {0x2A4, 0, 24}, // CAM_0x2A4
};

const CanMsg HYUNDAI_CANFD_HDA2_ALT_STEERING_TX_MSGS[] = {
  {0x110, 0, 32}, // LKAS_ALT
  {0x1CF, 1, 8},  // CRUISE_BUTTON
  {0x362, 0, 32}, // CAM_0x362
  {0x1AA, 1, 16}, // CRUISE_ALT_BUTTONS , carrot
};

const CanMsg HYUNDAI_CANFD_HDA2_LONG_TX_MSGS[] = {
  {0x50, 0, 16},  // LKAS
  {0x1CF, 0, 8},  // CRUISE_BUTTON
  {0x1CF, 1, 8},  // CRUISE_BUTTON
  {0x1CF, 2, 8},  // CRUISE_BUTTON
  {0x1AA, 0, 16}, // CRUISE_ALT_BUTTONS , carrot
  {0x1AA, 1, 16}, // CRUISE_ALT_BUTTONS , carrot
  {0x1AA, 2, 16}, // CRUISE_ALT_BUTTONS , carrot
  {0x2A4, 0, 24}, // CAM_0x2A4
  {0x51, 0, 32},  // ADRV_0x51
  {0x730, 1, 8},  // tester present for ADAS ECU disable
  {0x12A, 1, 16}, // LFA
  {0x160, 1, 16}, // ADRV_0x160
  {0x1E0, 1, 16}, // LFAHDA_CLUSTER
  {0x1A0, 1, 32}, // CRUISE_INFO
  {0x1EA, 1, 32}, // ADRV_0x1ea
  {0x200, 1, 8},  // ADRV_0x200
  {0x345, 1, 8},  // ADRV_0x345
  {0x1DA, 1, 32}, // ADRV_0x1da

  {0x12A, 0, 16}, // LFA
  {0x1E0, 0, 16}, // LFAHDA_CLUSTER
  {0x160, 0, 16}, // ADRV_0x160
  {0x1EA, 0, 32}, // ADRV_0x1ea
  {0x200, 0, 8},  // ADRV_0x200
  {0x1A0, 0, 32}, // CRUISE_INFO
  {0x345, 0, 8},  // ADRV_0x345
  {0x1DA, 0, 32}, // ADRV_0x1da

  {0x362, 0, 32}, // CAM_0x362
  {0x362, 1, 32}, // CAM_0x362
  {0x2a4, 1, 24}, // CAM_0x2a4

  {0x110, 0, 32}, // LKAS_ALT (272)
  {0x110, 1, 32}, // LKAS_ALT (272)

  {0x50, 1, 16}, // 
  {0x51, 1, 32}, // 

  {353, 0, 32}, // ADRV_353
  {354, 0, 32}, // CORNER_RADAR_HIGHWAY
  {512, 0, 8}, // ADRV_0x200
  {1187, 2, 8}, // 4A3
  {1204, 2, 8}, // 4B4

  {203, 0, 24}, // CB
  {373, 2, 24}, // TCS(0x175)
  {506, 2, 32}, // CLUSTER_SPEED_LIMIT
  {234, 2, 24}, // MDPS
  {687, 0, 8}, // STEER_TOUCH_2AF on ECAN (LX3_HEV: carrot fafdb3e sends to ECAN)
  {687, 2, 8}, // STEER_TOUCH_2AF on CAM (stock)

  {0x4BE, 2, 8}, // NEW_MSG_4BE (may be corner radar enabler x)
  {0x4B9, 2, 8}, // NEW_MSG_4B9 (may be corner radar enabler)
};

const CanMsg HYUNDAI_CANFD_HDA1_TX_MSGS[] = {
  {0x12A, 0, 16}, // LFA
  {0x1A0, 0, 32}, // CRUISE_INFO
  {0x1CF, 0, 8},  // CRUISE_BUTTON
  {0x1CF, 2, 8},  // CRUISE_BUTTON
  {0x1E0, 0, 16}, // LFAHDA_CLUSTER
  {0x160, 0, 16}, // ADRV_0x160
  {0x7D0, 0, 8},  // tester present for radar ECU disable
  {0x1AA, 2, 16}, // CRUISE_ALT_BUTTONS , carrot
  {203, 0, 24}, // CB
  {373, 2, 24}, // TCS(0x175)

  {353, 0, 32}, // ADRV_353
  {354, 0, 32}, // CORNER_RADAR_HIGHWAY
  {512, 0, 8}, // ADRV_0x200
  {1187, 2, 8}, // 4A3
  {1204, 2, 8}, // 4B4
  {373, 2, 24}, // TCS(0x175)
  {234, 2, 24}, // MDPS
  {687, 0, 8}, // STEER_TOUCH_2AF on ECAN (LX3_HEV: carrot fafdb3e sends to ECAN)
  {687, 2, 8}, // STEER_TOUCH_2AF on CAM (stock)

};


// *** Addresses checked in rx hook ***
// EV, ICE, HYBRID: ACCELERATOR (0x35), ACCELERATOR_BRAKE_ALT (0x100), ACCELERATOR_ALT (0x105)
#define HYUNDAI_CANFD_COMMON_RX_CHECKS(pt_bus)                                                                              \
  {.msg = {{0x35, (pt_bus), 32, .max_counter = 0xffU, .frequency = 100U},                   \
           {0x100, (pt_bus), 32, .max_counter = 0xffU, .frequency = 100U},                  \
           {0x105, (pt_bus), 32, .max_counter = 0U, .frequency = 100U, .ignore_counter = true, .ignore_checksum = true}}},                \
  {.msg = {{0x175, (pt_bus), 24, .max_counter = 0xffU, .frequency = 50U}, { 0 }, { 0 }}},  \
  {.msg = {{0xa0, (pt_bus), 24, .max_counter = 0xffU, .frequency = 100U}, { 0 }, { 0 }}},   \
  {.msg = {{0xea, (pt_bus), 24, .max_counter = 0xffU, .frequency = 100U}, { 0 }, { 0 }}},   \

#define HYUNDAI_CANFD_BUTTONS_ADDR_CHECK(pt_bus)                                                                            \
  {.msg = {{0x1cf, (pt_bus), 8, .ignore_checksum = true, .max_counter = 0xfU, .frequency = 50U}, { 0 }, { 0 }}}, \

#define HYUNDAI_CANFD_ALT_BUTTONS_ADDR_CHECK(pt_bus)                                                                            \
  {.msg = {{0x1aa, (pt_bus), 16, .ignore_checksum = true, .max_counter = 0xffU, .frequency = 50U}, { 0 }, { 0 }}},   \

// SCC_CONTROL (from ADAS unit or camera)
#define HYUNDAI_CANFD_SCC_ADDR_CHECK(scc_bus)                                                                                 \
  {.msg = {{0x1a0, (scc_bus), 32, .max_counter = 0xffU, .frequency = 50U}, { 0 }, { 0 }}}, \

//static bool hyundai_canfd_alt_buttons = false;
//static bool hyundai_canfd_hda2_alt_steering = false;

// *** Non-HDA2 checks ***
// Camera sends SCC messages on HDA1.
// Both button messages exist on some platforms, so we ensure we track the correct one using flag
RxCheck hyundai_canfd_rx_checks[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
  HYUNDAI_CANFD_BUTTONS_ADDR_CHECK(0)
  HYUNDAI_CANFD_SCC_ADDR_CHECK(2)
};
RxCheck hyundai_canfd_alt_buttons_rx_checks[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
  HYUNDAI_CANFD_ALT_BUTTONS_ADDR_CHECK(0)
  HYUNDAI_CANFD_SCC_ADDR_CHECK(2)
};

// Longitudinal checks for HDA1
RxCheck hyundai_canfd_long_rx_checks[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
  HYUNDAI_CANFD_BUTTONS_ADDR_CHECK(0)
};
RxCheck hyundai_canfd_long_alt_buttons_rx_checks[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
  HYUNDAI_CANFD_ALT_BUTTONS_ADDR_CHECK(0)
};

// Radar sends SCC messages on these cars instead of camera
RxCheck hyundai_canfd_radar_scc_rx_checks[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
  HYUNDAI_CANFD_BUTTONS_ADDR_CHECK(0)
  HYUNDAI_CANFD_SCC_ADDR_CHECK(0)
};
RxCheck hyundai_canfd_radar_scc_alt_buttons_rx_checks[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
  HYUNDAI_CANFD_ALT_BUTTONS_ADDR_CHECK(0)
  HYUNDAI_CANFD_SCC_ADDR_CHECK(0)
};


// *** HDA2 checks ***
// E-CAN is on bus 1, ADAS unit sends SCC messages on HDA2.
// Does not use the alt buttons message
RxCheck hyundai_canfd_hda2_rx_checks[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(1)
  HYUNDAI_CANFD_BUTTONS_ADDR_CHECK(1)  // TODO: carrot: canival no 0x1cf
  HYUNDAI_CANFD_SCC_ADDR_CHECK(1)
};
RxCheck hyundai_canfd_hda2_rx_checks_scc2[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
  HYUNDAI_CANFD_BUTTONS_ADDR_CHECK(0)  // TODO: carrot: canival no 0x1cf
  HYUNDAI_CANFD_SCC_ADDR_CHECK(2)
};
RxCheck hyundai_canfd_hda2_alt_buttons_rx_checks[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(1)
  HYUNDAI_CANFD_ALT_BUTTONS_ADDR_CHECK(1)
  HYUNDAI_CANFD_SCC_ADDR_CHECK(1)
};
RxCheck hyundai_canfd_hda2_alt_buttons_rx_checks_scc2[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
  HYUNDAI_CANFD_ALT_BUTTONS_ADDR_CHECK(0)
  HYUNDAI_CANFD_SCC_ADDR_CHECK(2)
};
RxCheck hyundai_canfd_hda2_long_rx_checks[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(1)
  HYUNDAI_CANFD_BUTTONS_ADDR_CHECK(1)  // TODO: carrot: canival no 0x1cf
};
RxCheck hyundai_canfd_hda2_long_rx_checks_scc2[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
  HYUNDAI_CANFD_BUTTONS_ADDR_CHECK(0)  
};
RxCheck hyundai_canfd_hda2_long_alt_buttons_rx_checks[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(1)
  HYUNDAI_CANFD_ALT_BUTTONS_ADDR_CHECK(1)
};
RxCheck hyundai_canfd_hda2_long_alt_buttons_rx_checks_scc2[] = {
  HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
  HYUNDAI_CANFD_ALT_BUTTONS_ADDR_CHECK(0)
};


const int HYUNDAI_PARAM_CANFD_ALT_BUTTONS = 32;
const int HYUNDAI_PARAM_CANFD_HDA2_ALT_STEERING = 128;
bool hyundai_canfd_alt_buttons = false;
bool hyundai_canfd_hda2_alt_steering = false;
bool hyundai_canfd_buffered_fwd = false;

int hyundai_canfd_hda2_get_lkas_addr(void) {
  return hyundai_canfd_hda2_alt_steering ? 0x110 : 0x50;
}

static uint8_t hyundai_canfd_get_counter(const CANPacket_t* to_push) {
  uint8_t ret = 0;
  if (GET_LEN(to_push) == 8U) {
    ret = GET_BYTE(to_push, 1) >> 4;
  }
  else {
    ret = GET_BYTE(to_push, 2);
  }
  return ret;
}

static uint32_t hyundai_canfd_get_checksum(const CANPacket_t* to_push) {
  uint32_t chksum = GET_BYTE(to_push, 0) | (GET_BYTE(to_push, 1) << 8);
  return chksum;
}


typedef struct {
  int addr;
  int bus;              // forwarding block ��� tx bus: 0 or 2
  int hz;
  uint32_t timeout_us;
  uint32_t last_tx_us;
  bool tx_active;
} CanfdTxState;

// forwarding block��: bus 0,2�� ���
CanfdTxState canfd_tx_states[] = {
  {0x50,  0, 100, 0U, 0U, false}, // 80:  LKAS
  {0x51,  0, 100, 0U, 0U, false}, // 81:  ADRV_0x51
  {0x110, 0, 100, 0U, 0U, false}, // 272: LKAS_ALT
  {0x12A, 0, 100, 0U, 0U, false}, // 298: LFA
  {0x160, 0, 50,  0U, 0U, false}, // 352: ADRV_0x160
  {0x161, 0, 20,  0U, 0U, false}, // 353: ADRV_0x161
  {0x162, 0, 20,  0U, 0U, false}, // 354: CCNC_0x162
  {0x1A0, 0, 50,  0U, 0U, false}, // 416: SCC_CONTROL
  {0x1DA, 0, 1,   0U, 0U, false}, // 474: ADRV_0x1da
  {0x1E0, 0, 20,  0U, 0U, false}, // 480: LFAHDA_CLUSTER
  {0x1EA, 0, 20,  0U, 0U, false}, // 490: ADRV_0x1ea
  {0x200, 0, 20,  0U, 0U, false}, // 512: ADRV_0x200
  {0x2A4, 0, 20,  0U, 0U, false}, // 676: CAM_0x2a4
  {0x345, 0, 5,   0U, 0U, false}, // 837: ADRV_0x345
  {0x362, 0, 10,  0U, 0U, false}, // 866: CAM_0x362
  {0x0CB, 0, 100, 0U, 0U, false}, // 203: LFA_ALT

  {0x175, 2, 50,  0U, 0U, false}, // 373: TCS
  {0x1AA, 2, 50,  0U, 0U, false}, // 426: CRUISE_ALT_BUTTONS
  {0x1CF, 2, 50,  0U, 0U, false}, // 463: CRUISE_BUTTON
  {0x1FA, 2, 10,  0U, 0U, false}, // 506: CLUSTER_SPEED_LIMIT
  {0x0EA, 2, 100, 0U, 0U, false}, // 234: MDPS
  {0x2AF, 2, 10,  0U, 0U, false}, // 687: STEER_TOUCH_2AF
  {0x4A3, 2, 5,   0U, 0U, false}, // 1187: HDA_INFO_4A3
  {0x4B4, 2, 10,  0U, 0U, false}, // 1204: NEW_MSG_4B4
  {0x4BE, 2, 10,  0U, 0U, false}, // 1214: NEW_MSG_4BE
  {0x4B9, 2, 10,  0U, 0U, false}, // 1209: NEW_MSG_4B9

  {0, 0, 0, 0U, 0U, false},
};

static CanfdTxState* find_canfd_tx_state(int bus, int addr) {
  for (int i = 0; canfd_tx_states[i].addr > 0; i++) {
    if ((canfd_tx_states[i].addr == addr) && (canfd_tx_states[i].bus == bus)) {
      return &canfd_tx_states[i];
    }
  }
  return NULL;
}


static void hyundai_canfd_set_counter(CANPacket_t* to_push, uint8_t counter) {
  if (GET_LEN(to_push) == 8U) {
    to_push->data[1] = (to_push->data[1] & 0x0FU) | ((counter & 0x0FU) << 4);
  }
  else {
    to_push->data[2] = counter;
  }
}

static void hyundai_canfd_set_checksum(CANPacket_t* to_push, uint16_t checksum) {
  to_push->data[0] = (uint8_t)(checksum & 0xFFU);
  to_push->data[1] = (uint8_t)((checksum >> 8U) & 0xFFU);
}

static void hyundai_canfd_update_checksum(CANPacket_t* to_push) {
  to_push->data[0] = 0U;
  to_push->data[1] = 0U;
  uint32_t checksum = hyundai_common_canfd_compute_checksum(to_push);
  hyundai_canfd_set_checksum(to_push, (uint16_t)checksum);
}
static void canfd_apply_counter_and_update_checksum(CANPacket_t* dst, uint8_t counter) {
  hyundai_canfd_set_counter(dst, counter);
  hyundai_canfd_update_checksum(dst);
}
static void canfd_record_tx_time(int bus, int addr, bool tx) {
  CanfdTxState* st = find_canfd_tx_state(bus, addr);
  if (st != NULL) {
    st->last_tx_us = tx ? microsecond_timer_get() : 0U;
    st->tx_active = tx;
  }
}

static bool canfd_should_block_fwd(int tx_bus, int addr, uint32_t now) {
  CanfdTxState* st = find_canfd_tx_state(tx_bus, addr);
  if (st == NULL) {
    return false;
  }
  if (hyundai_canfd_lx3_guard && !st->tx_active) {
    return false;
  }
  return (now - st->last_tx_us) < st->timeout_us;
}

#define CANFD_BFWD_MAX_QUEUE 2
#define CANFD_BFWD_REUSE_MAX 2

typedef struct {
  int addr;
  int dst_bus;
  bool enabled;

  bool started;
  uint8_t head;
  uint8_t tail;
  uint8_t count;

  uint8_t reuse_left;
  bool has_last_pkt;
  CANPacket_t last_pkt;
  uint32_t last_pkt_us;

  CANPacket_t q[CANFD_BFWD_MAX_QUEUE];
  uint32_t q_us[CANFD_BFWD_MAX_QUEUE];
} CanfdBufferedFwd;

CanfdBufferedFwd canfd_bfwd[] = {
  {.addr = 0x1A0, .dst_bus = 0, .enabled = true },  // SCC_CONTROL
  {.addr = 0x12A, .dst_bus = 0, .enabled = true },  // LFA
  {.addr = 0x0CB, .dst_bus = 0, .enabled = true },  // LFA_ALT
  {.addr = 0x0EA, .dst_bus = 2, .enabled = true },  // MDPS
  {.addr = 0x1AA, .dst_bus = 2, .enabled = true },  // CRUISE_ALT_BUTTONS
  // {.addr = 0x1CF, .dst_bus = 2, .enabled = true },  // CRUISE_BUTTON
  {.addr = 0x175, .dst_bus = 2, .enabled = true },  // TCS
  { 0 },
};

static void canfd_copy_packet(CANPacket_t* dst, const CANPacket_t* src) {
  dst->fd = src->fd;
  dst->returned = 0U;
  dst->rejected = 0U;
  dst->extended = src->extended;
  dst->addr = src->addr;
  dst->bus = src->bus;
  dst->data_len_code = src->data_len_code;
  (void)memcpy(dst->data, src->data, dlc_to_len[src->data_len_code]);
}
static CanfdBufferedFwd* canfd_bfwd_find(int addr, int dst_bus) {
  for (int i = 0; canfd_bfwd[i].addr > 0; i++) {
    if (canfd_bfwd[i].enabled &&
      (canfd_bfwd[i].addr == addr) &&
      (canfd_bfwd[i].dst_bus == dst_bus)) {
      return &canfd_bfwd[i];
    }
  }
  return NULL;
}

static void canfd_bfwd_reset(CanfdBufferedFwd* st) {
  st->started = false;
  st->head = 0U;
  st->tail = 0U;
  st->count = 0U;
  st->reuse_left = 0U;
  st->has_last_pkt = false;
  st->last_pkt_us = 0U;
  (void)memset(&st->last_pkt, 0, sizeof(st->last_pkt));
}

static bool canfd_bfwd_guarded(const CanfdBufferedFwd* st) {
  return hyundai_canfd_lx3_guard && hyundai_canfd_actuator_addr(st->addr);
}

static bool canfd_bfwd_expired(const CanfdBufferedFwd* st, uint32_t accepted_us) {
  const CanfdTxState* tx_state = find_canfd_tx_state(st->dst_bus, st->addr);
  // Keep the original timestamp across pop/reuse. Arrival of an OEM frame
  // cannot refresh an old OP command. Unsigned subtraction handles timer wrap.
  return (tx_state == NULL) || ((microsecond_timer_get() - accepted_us) >= tx_state->timeout_us);
}

static bool canfd_bfwd_authorized(const CanfdBufferedFwd* st) {
  return controls_allowed && !safety_rx_checks_invalid && !relay_malfunction &&
         ((st->addr != 0x1A0) || (lx3_mode == 2));
}

static void lx3_clear_session(void);

static bool canfd_bfwd_packet_authorized(const CanfdBufferedFwd* st, const CANPacket_t* pkt) {
  if (!canfd_bfwd_authorized(st)) return false;
  if (st->addr == 0xCB) {
    if (!lx3_angle_context_valid(GET_BYTE(pkt, 6))) return false;
    if (lx3_angle_forward_violation(pkt)) {
      // Do not leave the host looking active while every advanced goal is
      // silently discarded. Companion reporting exposes this permission loss.
      lx3_clear_session();
      return false;
    }
  }
  return (st->addr != 0x1A0) || get_longitudinal_allowed() || lx3_neutral_gas_override(pkt);
}

static void canfd_bfwd_revoke_actuators(void) {
  lx3_angle_active_prev = false;
  lx3_angle_forwarded_active_prev = false;
  for (int i = 0; canfd_bfwd[i].addr > 0; i++) {
    if (canfd_bfwd_guarded(&canfd_bfwd[i])) {
      canfd_bfwd_reset(&canfd_bfwd[i]);
      canfd_record_tx_time(canfd_bfwd[i].dst_bus, canfd_bfwd[i].addr, false);
    }
  }
  // Display ownership must end with the accepted session, including when no
  // later USB packet arrives to update its previous forwarding-block timer.
  for (int i = 0; canfd_tx_states[i].addr > 0; i++) {
    if (hyundai_canfd_lx3_guard && hyundai_canfd_lx3_display_addr(canfd_tx_states[i].addr)) {
      canfd_tx_states[i].tx_active = false;
      canfd_tx_states[i].last_tx_us = 0U;
    }
  }
}

static void lx3_clear_session(void) {
  controls_allowed = false;
  lx3_mode = 0;
  lx3_requested_mode = 0;
  lx3_pending = false;
  canfd_bfwd_revoke_actuators();
}

static void lx3_next_generation(void) {
  if (lx3_request_generation == UINT16_MAX) {
    lx3_generation_exhausted = true;
    lx3_clear_session();
  } else {
    lx3_request_generation++;
  }
}

static void lx3_revoke_permission(void) {
  lx3_clear_session();
  lx3_button_ready = false;
  lx3_neutral_samples = 0U;
  lx3_main_held = false;
  lx3_button_prev = 0;
  canfd_bfwd_revoke_actuators();
}

static bool lx3_request_context_valid(void);

static void lx3_permission_maintenance(void) {
  if ((controls_allowed && (!lx3_button_ready || !lx3_button_seen)) ||
      (lx3_button_ready && (microsecond_timer_get() - lx3_button_us > 200000U)) ||
      safety_rx_checks_invalid || relay_malfunction || (!controls_allowed && (lx3_mode != 0)) ||
      (lx3_pending && !lx3_request_context_valid())) {
    lx3_revoke_permission();
  } else if (lx3_pending && (microsecond_timer_get() - lx3_request_us >= LX3_REQUEST_TIMEOUT_US)) {
    // Host denial/timeout is not a corrupt physical input stream.
    lx3_clear_session();
  }
}

static bool lx3_request_context_valid(void) {
  return !lx3_generation_exhausted && (safety_lx3_transport_epoch() != 0U) &&
         hyundai_camera_scc && hyundai_canfd_hda2 && hyundai_hybrid_gas_signal && hyundai_longitudinal &&
         lx3_button_seen && lx3_button_ready && (microsecond_timer_get() - lx3_button_us <= 200000U) &&
         lx3_angle_context_valid(0) && !safety_rx_checks_invalid && !relay_malfunction &&
         !brake_pressed && !regen_braking &&
         (!gas_pressed || (alternative_experience & ALT_EXP_DISABLE_DISENGAGE_ON_GAS));
}

static void lx3_request_mode(int mode, uint8_t counter) {
  lx3_next_generation();
  lx3_request_counter = counter;
  lx3_request_us = microsecond_timer_get();
  if (mode == 0) {
    // Driver OFF is not evidence of a corrupt physical input stream.
    lx3_clear_session();
  } else if (((mode == 1) || (mode == 2)) && lx3_request_context_valid()) {
    lx3_clear_session();
    lx3_requested_mode = mode;
    lx3_pending = true;
    // Physical RX creates a request, never actuator permission. Host must
    // acknowledge this generation after normal entry checks have passed.
  } else {
    lx3_clear_session();
  }
}

static void lx3_heartbeat(uint16_t value, uint16_t generation) {
  const bool tagged = (value & 0x00F8U) == LX3_HEARTBEAT_TAG;
  heartbeat_engaged = tagged && ((value & 1U) != 0U);
  lx3_permission_maintenance();
  if (!tagged) {
    // An old host cannot grant permission under the guarded policy.
    lx3_clear_session();
    return;
  }
  const int mode = (value >> 1U) & 3U;
  const bool matches = (generation != 0U) && (generation == lx3_request_generation) &&
                       ((value >> 8U) == lx3_request_counter);
  if (!heartbeat_engaged) {
    // A disabled heartbeat queued before a new request must not cancel it.
    // Revoke accepted permission immediately; explicit matching OFF also
    // consumes a pending request. Neither path can create authority.
    controls_allowed = false;
    lx3_mode = 0;
    canfd_bfwd_revoke_actuators();
    if (!lx3_pending) lx3_clear_session();
    if (matches && (mode == 0)) lx3_clear_session();
    return;
  }
  if (matches && (mode == 0)) {
    lx3_clear_session();
  } else if (matches && lx3_pending && (mode == lx3_requested_mode) &&
             (microsecond_timer_get() - lx3_request_us < LX3_REQUEST_TIMEOUT_US) &&
             lx3_request_context_valid()) {
    lx3_mode = mode;
    lx3_pending = false;
    controls_allowed = true;
    // This rising edge happens in USB/SPI, outside safety_rx_hook's edge reset.
    heartbeat_engaged_mismatches = 0U;
  }
}

static lx3_permission_t lx3_permission_snapshot(void) {
  lx3_permission_maintenance();
  const uint32_t age_ms = (microsecond_timer_get() - lx3_request_us) / 1000U;
  const lx3_permission_t state = {
    .version = LX3_PERMISSION_VERSION,
    .requested_mode = (uint8_t)lx3_requested_mode,
    .accepted_mode = (uint8_t)lx3_mode,
    .physical_counter = lx3_request_counter,
    .generation = lx3_request_generation,
    .age_ms = (uint16_t)MIN(age_ms, 65535U),
    .controls_allowed = controls_allowed ? 1U : 0U,
    .phase = lx3_pending ? 1U : ((controls_allowed && (lx3_mode != 0)) ? 2U : 0U),
    .reserved = 0U,
    .transport_epoch = safety_lx3_transport_epoch(),
  };
  return state;
}

static void lx3_physical_buttons_rx(const CANPacket_t *pkt) {
  const uint32_t now = microsecond_timer_get();
  if ((GET_LEN(pkt) != 16U) || (hyundai_canfd_get_checksum(pkt) != hyundai_common_canfd_compute_checksum(pkt))) {
    lx3_revoke_permission();
    return;
  }
  const uint8_t counter = GET_BYTE(pkt, 2);
  if (lx3_button_seen) {
    const uint8_t delta = counter - lx3_button_counter;
    const uint32_t elapsed = now - lx3_button_us;
    if ((delta != 2U) || (elapsed < 10000U) || (elapsed > 200000U)) {
      if (delta != 0U) { lx3_button_counter = counter; lx3_button_us = now; }
      lx3_revoke_permission();
      return;  // Duplicate does not refresh freshness or synthesize release.
    }
  }
  lx3_button_seen = true;
  lx3_button_counter = counter;
  lx3_button_us = now;
  const int raw = GET_BYTE(pkt, 10) & 0xFU;
  const bool lfa = GET_BIT(pkt, 87U);
  if (((raw > 4) && (raw != 8)) || (lfa && (raw != 0) && (raw != 4))) {
    lx3_revoke_permission();
    return;
  }
  if (raw == HYUNDAI_BTN_CANCEL) {
    lx3_revoke_permission();
    return;
  }
  if (!lx3_button_ready) {
    if ((raw != 0) || lfa) lx3_neutral_samples = 0U;
    else lx3_neutral_samples++;
    lx3_button_ready = lx3_neutral_samples >= 3U;
    return;  // A held startup/recovery press cannot become an enable edge.
  }
  const int button = lfa ? 16 : raw;
  if (raw == 8) {
    lx3_main_held = true;
    lx3_main_us = now;
    // Stable byte identity independent of USB batching and the 300ms decision.
    lx3_main_release_counter = (uint8_t)(counter + 2U);
  } else if (lx3_main_held && (button != 0)) {
    lx3_revoke_permission();
    return;
  } else if (lx3_main_held && (now - lx3_main_us >= 300000U)) {
    lx3_main_held = false;
    lx3_request_mode((lx3_pending || (controls_allowed && (lx3_mode == 2))) ? 0 : 2, lx3_main_release_counter);
  }
  if ((button == 0) && (lx3_button_prev == 16)) {
    lx3_request_mode((lx3_pending || controls_allowed) ? 0 : 1, counter);
  } else if ((button == 0) && ((lx3_button_prev == HYUNDAI_BTN_RESUME) || (lx3_button_prev == HYUNDAI_BTN_SET))) {
    if (!lx3_pending && !(controls_allowed && (lx3_mode == 2))) lx3_request_mode(2, counter);
  }
  lx3_button_prev = (raw == 8) ? 0 : button;
}

static void canfd_bfwd_push(CanfdBufferedFwd* st, const CANPacket_t* pkt) {
  if ((st == NULL) || !st->enabled) return;
  if (GET_BUS(pkt) != st->dst_bus) return;

  if (canfd_bfwd_guarded(st) && !hyundai_canfd_actuator_active(pkt)) {
    // An accepted neutral command supersedes every pending/reusable active
    // command, even when the queue is full.
    canfd_bfwd_reset(st);
  }

  if (st->count >= CANFD_BFWD_MAX_QUEUE) {
    if (!canfd_bfwd_guarded(st)) return;
    st->head = (st->head + 1U) % CANFD_BFWD_MAX_QUEUE;
    st->count--;
  }

  canfd_copy_packet(&st->q[st->tail], pkt);
  st->q_us[st->tail] = microsecond_timer_get();
  st->tail = (st->tail + 1U) % CANFD_BFWD_MAX_QUEUE;
  st->count++;
  st->started = true;
}

static bool canfd_bfwd_pop(CanfdBufferedFwd* st, CANPacket_t* pkt) {
  if ((st == NULL) || !st->enabled) {
    return false;
  }

  if (!st->started || (st->count == 0U)) {
    return false;
  }

  if (canfd_bfwd_guarded(st) && (canfd_bfwd_expired(st, st->q_us[st->head]) ||
      (hyundai_canfd_actuator_active(&st->q[st->head]) && !canfd_bfwd_packet_authorized(st, &st->q[st->head])))) {
    canfd_bfwd_reset(st);
    return false;
  }

  canfd_copy_packet(pkt, &st->q[st->head]);
  st->last_pkt_us = st->q_us[st->head];
  st->head = (st->head + 1U) % CANFD_BFWD_MAX_QUEUE;
  st->count--;

  // ������ ���� packet ����
  canfd_copy_packet(&st->last_pkt, pkt);
  st->has_last_pkt = true;
  st->reuse_left = CANFD_BFWD_REUSE_MAX;

  if (st->count == 0U) {
    st->started = false;
  }

  return true;
}
static bool canfd_bfwd_reuse_last(CanfdBufferedFwd* st, CANPacket_t* pkt) {
  if ((st == NULL) || !st->enabled) {
    return false;
  }

  if (!st->has_last_pkt || (st->reuse_left == 0U)) {
    return false;
  }

  if (canfd_bfwd_guarded(st) && (canfd_bfwd_expired(st, st->last_pkt_us) ||
      (hyundai_canfd_actuator_active(&st->last_pkt) && !canfd_bfwd_packet_authorized(st, &st->last_pkt)))) {
    canfd_bfwd_reset(st);
    return false;
  }

  canfd_copy_packet(pkt, &st->last_pkt);
  st->reuse_left--;

  return true;
}




static void hyundai_canfd_rx_hook(const CANPacket_t *to_push) {
  int bus = GET_BUS(to_push);
  int addr = GET_ADDR(to_push);

  int pt_bus = hyundai_canfd_hda2 ? 1 : 0;
  const int scc_bus = hyundai_camera_scc ? 2 : pt_bus;

  if (hyundai_camera_scc) pt_bus = 0;

  if (hyundai_canfd_lx3_guard && (addr == 0x10B) && (bus == 0)) {
    lx3_physical_buttons_rx(to_push);
  }

  if (bus == pt_bus) {
    // driver torque
    if (addr == 0xea) {
      int torque_driver_new = ((GET_BYTE(to_push, 11) & 0x1fU) << 8U) | GET_BYTE(to_push, 10);
      torque_driver_new -= 4095;
      update_sample(&torque_driver, torque_driver_new);
      if (hyundai_canfd_lx3_guard && (GET_LEN(to_push) == 24U) &&
          (hyundai_canfd_get_checksum(to_push) == hyundai_common_canfd_compute_checksum(to_push))) {
        // Host uses STEERING_ANGLE_2 with its DBC sign inverted: raw * +0.1 deg.
        lx3_measured_angle = to_signed(GET_BYTES(to_push, 16, 2), 16);
        lx3_mdps_fault = GET_BIT(to_push, 54U) || GET_BIT(to_push, 149U);
        lx3_mdps_us = microsecond_timer_get();
        lx3_mdps_seen = true;
      }
    }

    // cruise buttons
    const int button_addr = hyundai_canfd_alt_buttons ? 0x1aa : 0x1cf;
    if (addr == button_addr) {
      bool main_button = false;
      int cruise_button = 0;
      if (addr == 0x1cf) {
        cruise_button = GET_BYTE(to_push, 2) & 0x7U;
        main_button = GET_BIT(to_push, 19U);
      } else {
        cruise_button = (GET_BYTE(to_push, 4) >> 4) & 0x7U;
        main_button = GET_BIT(to_push, 34U);
      }
      if (!hyundai_canfd_lx3_guard) {
        hyundai_common_cruise_buttons_check(cruise_button, main_button);
      } else if (cruise_button == HYUNDAI_BTN_CANCEL) {
        controls_allowed = false;
      }
      // Legacy 0x1AA RES/SET never authorizes the guarded physical path.
    }

    // gas press, different for EV, hybrid, and ICE models
    if ((addr == 0x35) && hyundai_ev_gas_signal) {
      gas_pressed = GET_BYTE(to_push, 5) != 0U;
    } else if ((addr == 0x105) && hyundai_hybrid_gas_signal) {
      gas_pressed = GET_BIT(to_push, 103U) || (GET_BYTE(to_push, 13) != 0U) || GET_BIT(to_push, 112U);
    } else if ((addr == 0x100) && !hyundai_ev_gas_signal && !hyundai_hybrid_gas_signal) {
      gas_pressed = GET_BIT(to_push, 176U);
    } else {
    }

    // brake press
    if (addr == 0x175) {
      brake_pressed = GET_BIT(to_push, 81U);
    }

    // vehicle moving
    if (addr == 0xa0) {
      uint32_t fl = (GET_BYTES(to_push, 8, 2)) & 0x3FFFU;
      uint32_t fr = (GET_BYTES(to_push, 10, 2)) & 0x3FFFU;
      uint32_t rl = (GET_BYTES(to_push, 12, 2)) & 0x3FFFU;
      uint32_t rr = (GET_BYTES(to_push, 14, 2)) & 0x3FFFU;
      vehicle_moving = (fl > HYUNDAI_STANDSTILL_THRSLD) || (fr > HYUNDAI_STANDSTILL_THRSLD) ||
                       (rl > HYUNDAI_STANDSTILL_THRSLD) || (rr > HYUNDAI_STANDSTILL_THRSLD);

      // average of all 4 wheel speeds. Conversion: raw * 0.03125 / 3.6 = m/s
      UPDATE_VEHICLE_SPEED((fr + rr + rl + fl) / 4.0 * 0.03125 / 3.6);
    }
  }

  if (bus == scc_bus) {
    // cruise state
    if ((addr == 0x1a0) && !hyundai_longitudinal) {
      // 1=enabled, 2=driver override
      int cruise_status = ((GET_BYTE(to_push, 8) >> 4) & 0x7U);
      bool cruise_engaged = (cruise_status == 1) || (cruise_status == 2);
      hyundai_common_cruise_state_check(cruise_engaged);
    }
  }

  const int steer_addr = hyundai_canfd_hda2 ? hyundai_canfd_hda2_get_lkas_addr() : 0x12a;
  bool stock_ecu_detected = (addr == steer_addr) && (bus == 0);
  if (hyundai_longitudinal) {
    // on HDA2, ensure ADRV ECU is still knocked out
    // on others, ensure accel msg is blocked from camera
    const int stock_scc_bus = hyundai_canfd_hda2 ? 1 : 0;
    stock_ecu_detected = stock_ecu_detected || ((addr == 0x1a0) && (bus == stock_scc_bus));
  }
  generic_rx_checks(stock_ecu_detected);

  if (hyundai_canfd_lx3_guard) {
    lx3_permission_maintenance();
    if (!controls_allowed) canfd_bfwd_revoke_actuators();
  }

}

static bool hyundai_canfd_tx_hook(const CANPacket_t *to_send_const) {
  CANPacket_t* to_send = (CANPacket_t*)to_send_const;

  const TorqueSteeringLimits HYUNDAI_CANFD_STEERING_LIMITS = {
    .max_steer = 512,
    .max_rt_delta = 112,
    .max_rt_interval = 250000,
    .max_rate_up = 10,
    .max_rate_down = 10,
    .driver_torque_allowance = 250,
    .driver_torque_multiplier = 2,
    .type = TorqueDriverLimited,

    // the EPS faults when the steering angle is above a certain threshold for too long. to prevent this,
    // we allow setting torque actuation bit to 0 while maintaining the requested torque value for two consecutive frames
    .min_valid_request_frames = 89,
    .max_invalid_request_frames = 2,
    .min_valid_request_rt_interval = 810000,  // 810ms; a ~10% buffer on cutting every 90 frames
    .has_steer_req_tolerance = true,
  };

  bool tx = true;
  int addr = GET_ADDR(to_send);
  bool violation = false;

  if (hyundai_canfd_lx3_guard) {
    lx3_permission_maintenance();
    if (!controls_allowed || safety_rx_checks_invalid) canfd_bfwd_revoke_actuators();
    if (safety_rx_checks_invalid && hyundai_canfd_actuator_addr(addr)) return false;
    if (!hyundai_camera_scc || !hyundai_canfd_hda2 || !hyundai_hybrid_gas_signal || !hyundai_longitudinal) {
      return false;
    }
    if ((addr == 0xEA) || (addr == 0x175) || (addr == 0x2AF) || (addr == 0x1AA) || (addr == 0x1CF)) {
      return false;  // Do not synthesize driver/EPS feedback or enable buttons.
    }
    if (((addr == 0x362) || (addr == 0x2A4) || hyundai_canfd_lx3_display_addr(addr)) &&
        (!controls_allowed || (lx3_mode == 0) || safety_rx_checks_invalid || relay_malfunction)) {
      // Lane suppression is camera input interference, even with zero torque.
      // A pending/refused/off host may not inject it into the stock CAN path.
      // Delayed display packets must not restore green icons or ownership
      // after native permission has already been revoked.
      return false;
    }
    if (addr == 0xCB) {
      const int active = (GET_BYTE(to_send, 3) >> 4) & 0x3U;
      const int torque_limit = GET_BYTE(to_send, 6);
      const int desired_angle = to_signed(GET_BYTES(to_send, 4, 2) & 0x3FFFU, 14);
      // Match existing host encoding: 0.1 degree per raw unit, 175-degree
      // absolute limit, 250 max-torque ceiling. These are software bounds,
      // not measured EPS qualification. The rate/driver envelope below is
      // independently checked; OEM timing/response still needs qualification.
      violation |= (active == 3) || ((active == 2) && !controls_allowed);
      violation |= (active != 2) && (torque_limit != 0);
      violation |= (desired_angle > HYUNDAI_LX3_MAX_ANGLE) || (desired_angle < -HYUNDAI_LX3_MAX_ANGLE) ||
                   (torque_limit > HYUNDAI_LX3_MAX_TORQUE);
      violation |= lx3_angle_violation(desired_angle, active, torque_limit);
    }
    if ((addr == 0x12A) && GET_BIT(to_send, 52U) && !controls_allowed) {
      violation = true;
    }
  }

  // steering
  const int steer_addr = (hyundai_canfd_hda2 && !hyundai_longitudinal) ? hyundai_canfd_hda2_get_lkas_addr() : 0x12a;
  if (addr == steer_addr) {
    int desired_torque = (((GET_BYTE(to_send, 6) & 0xFU) << 7U) | (GET_BYTE(to_send, 5) >> 1U)) - 1024U;
    bool steer_req = GET_BIT(to_send, 52U);

    if (steer_torque_cmd_checks(desired_torque, steer_req, HYUNDAI_CANFD_STEERING_LIMITS)) {
      //tx = false;
    }
  }

#if 0
  // cruise buttons check
  if (addr == 0x1cf) {
    int button = GET_BYTE(to_send, 2) & 0x7U;
    bool is_cancel = (button == HYUNDAI_BTN_CANCEL);
    bool is_resume = (button == HYUNDAI_BTN_RESUME);
    bool is_set = (button == HYUNDAI_BTN_SET);

    bool allowed = (is_cancel && cruise_engaged_prev) || (is_resume && controls_allowed) || (is_set && controls_allowed);
    if (!allowed) {
      tx = false;
    }
  }
#endif

  // UDS: only tester present ("\x02\x3E\x80\x00\x00\x00\x00\x00") allowed on diagnostics address
  if ((addr == 0x730) && hyundai_canfd_hda2) {
    if ((GET_BYTES(to_send, 0, 4) != 0x00803E02U) || (GET_BYTES(to_send, 4, 4) != 0x0U)) {
      tx = false;
    }
  }

  // ACCEL: safety check
  if (addr == 0x1a0) {
    int desired_accel_raw = (((GET_BYTE(to_send, 17) & 0x7U) << 8) | GET_BYTE(to_send, 16)) - 1023U;
    int desired_accel_val = ((GET_BYTE(to_send, 18) << 4) | (GET_BYTE(to_send, 17) >> 4)) - 1023U;


    if (hyundai_canfd_lx3_guard) {
      const int cruise_status = (GET_BYTE(to_send, 8) >> 4) & 0x7U;
      const int stop_request = GET_BYTE(to_send, 23) & 0x3U;
      const bool inactive = (cruise_status == 0) && (desired_accel_raw == 0) && (desired_accel_val == 0) &&
                            (stop_request == 0);
      const bool active_mode = (cruise_status == 1) || (cruise_status == 2) || (cruise_status == 4);
      // The common helper in this fork self-authorizes on nonzero accel.
      // Keep this opt-in policy independent of that legacy side effect.
      violation |= !inactive && ((lx3_mode != 2) || (!get_longitudinal_allowed() && !lx3_neutral_gas_override(to_send)) || !active_mode);
      violation |= (desired_accel_raw > HYUNDAI_LONG_LIMITS.max_accel) || (desired_accel_raw < HYUNDAI_LONG_LIMITS.min_accel);
      violation |= (desired_accel_val > HYUNDAI_LONG_LIMITS.max_accel) || (desired_accel_val < HYUNDAI_LONG_LIMITS.min_accel);
    } else if (hyundai_longitudinal) {
      int cruise_status = ((GET_BYTE(to_send, 8) >> 4) & 0x7U);
      bool cruise_engaged = (cruise_status == 1) || (cruise_status == 2) || (cruise_status == 4);
      if (cruise_engaged) {
        if (!controls_allowed) print("automatic controls_allowed enabled....\n");
        controls_allowed = true;
      }
      violation |= longitudinal_accel_checks(desired_accel_raw, HYUNDAI_LONG_LIMITS);
      violation |= longitudinal_accel_checks(desired_accel_val, HYUNDAI_LONG_LIMITS);
      if (violation) {
        print("long violation"); putui((uint32_t)desired_accel_raw); print(","); putui((uint32_t)desired_accel_val); print("\n");
      }

    }
    else {
      // only used to cancel on here
      if ((desired_accel_raw != 0) || (desired_accel_val != 0)) {
        violation = true;
        print("no long violation\n");
      }
    }
  }

  if (hyundai_canfd_lx3_guard && (addr == 0xCB) && tx && !violation) {
    lx3_angle_active_prev = ((GET_BYTE(to_send, 3) >> 4) & 0x3U) == 2U;
    if (lx3_angle_active_prev) {
      lx3_angle_accepted = to_signed(GET_BYTES(to_send, 4, 2) & 0x3FFFU, 14);
      lx3_angle_accepted_us = microsecond_timer_get();
    }
  }
  if (violation) {
    tx = false;
  }
  else if (hyundai_canfd_buffered_fwd) {
    CanfdBufferedFwd* bfwd = canfd_bfwd_find(addr, GET_BUS(to_send));
    if (bfwd != NULL) {
      canfd_bfwd_push(bfwd, to_send);
      extern bool safety_tx_buffered_for_fwd;
      safety_tx_buffered_for_fwd = true;
      //tx = false;
      return true;
    }
  }

  canfd_record_tx_time(GET_BUS(to_send), addr, tx);

  return tx;
}

static int hyundai_canfd_fwd_hook(CANPacket_t* to_send) {
  const int bus_num = GET_BUS(to_send);
  const int addr = GET_ADDR(to_send);

  int bus_fwd = -1;
  uint32_t now = microsecond_timer_get();
  if (hyundai_canfd_lx3_guard) lx3_permission_maintenance();
  if (hyundai_canfd_lx3_guard && (!controls_allowed || safety_rx_checks_invalid || relay_malfunction)) {
    canfd_bfwd_revoke_actuators();
  }
  if (bus_num == 0) {
    bus_fwd = 2;
  }
  else if (bus_num == 2) {
    bus_fwd = 0;
  }
  else {
    return -1;
  }

  if (hyundai_canfd_buffered_fwd) {
    CanfdBufferedFwd* bfwd = canfd_bfwd_find(addr, bus_fwd);
    if (bfwd != NULL) {
      if (canfd_bfwd_guarded(bfwd)) {
        const int expected_len = (addr == 0xCB) ? 24 : ((addr == 0x12A) ? 16 : 32);
        // Malformed OEM input is never an opportunity to insert OP control.
        // Preserve its original bytes; the pending OP queue can only advance
        // on a valid stock frame within its existing acceptance deadline.
        if ((GET_LEN(to_send) != expected_len) ||
            (hyundai_canfd_get_checksum(to_send) != hyundai_common_canfd_compute_checksum(to_send))) {
          if (addr == 0xCB) lx3_angle_forwarded_active_prev = false;
          return bus_fwd;
        }
      }
      CANPacket_t buffered_pkt;
      bool use_buffered = canfd_bfwd_pop(bfwd, &buffered_pkt);

      // queue�� ������� ������ ������ 1~2ȸ ����
      if (!use_buffered) {
        use_buffered = canfd_bfwd_reuse_last(bfwd, &buffered_pkt);
        if (use_buffered) {
          print("reuse:"); putui((uint32_t)addr); print(", reuse_left:"); putui((uint32_t)bfwd->reuse_left); print("\n");
        }
      }

      if (use_buffered) {
        uint8_t counter = hyundai_canfd_get_counter(to_send);

        canfd_copy_packet(to_send, &buffered_pkt);
        canfd_apply_counter_and_update_checksum(to_send, counter);

        if (hyundai_canfd_lx3_guard && (addr == 0xCB)) {
          lx3_angle_forwarded_active_prev = hyundai_canfd_actuator_active(to_send);
          if (lx3_angle_forwarded_active_prev) {
            lx3_angle_forwarded = to_signed(GET_BYTES(to_send, 4, 2) & 0x3FFFU, 14);
            lx3_angle_forwarded_us = now;
          }
        }

        return bus_fwd;
      }
    }
  }

  if (bus_num == 0) {
    if (canfd_should_block_fwd(2, addr, now)) {
      return -1;
    }
    if (addr == 0x4B9) {
      return -1;
    }
    return 2;
  }
  if (canfd_should_block_fwd(0, addr, now)) {
    return -1;
  }
  if (hyundai_canfd_lx3_guard && (addr == 0xCB)) {
    // Original fallback interrupts the OP stream. A later active insertion
    // must start from the current measured angle, not an old OP goal.
    lx3_angle_forwarded_active_prev = false;
  }
  return 0;
}

static safety_config hyundai_canfd_init(uint16_t param) {

  for (int i = 0; canfd_tx_states[i].addr > 0; i++) {
    canfd_tx_states[i].timeout_us = (uint32_t)(1000000.0 / canfd_tx_states[i].hz) + 20000U;
    canfd_tx_states[i].last_tx_us = 0U;
    canfd_tx_states[i].tx_active = false;
  }

  for (int i = 0; canfd_bfwd[i].addr > 0; i++) {
    canfd_bfwd_reset(&canfd_bfwd[i]);
  }

  hyundai_common_init(param);
  hyundai_canfd_lx3_guard = GET_FLAG(param, HYUNDAI_PARAM_LX3_ENGAGEMENT_GUARD);
  lx3_mdps_seen = false;
  lx3_mdps_fault = false;
  lx3_mdps_us = 0U;
  lx3_measured_angle = 0;
  lx3_angle_active_prev = false;
  lx3_angle_accepted = 0;
  lx3_angle_accepted_us = 0U;
  lx3_angle_forwarded_active_prev = false;
  lx3_angle_forwarded = 0;
  lx3_angle_forwarded_us = 0U;
  lx3_mode = 0;
  lx3_requested_mode = 0;
  lx3_pending = false;
  lx3_next_generation();
  lx3_request_counter = 0U;
  lx3_request_us = 0U;
  lx3_button_seen = false;
  lx3_button_ready = false;
  lx3_button_counter = 0U;
  lx3_button_us = 0U;
  lx3_neutral_samples = 0U;
  lx3_main_held = false;
  lx3_main_us = 0U;
  lx3_main_release_counter = 0U;
  lx3_button_prev = 0;

  gen_crc_lookup_table_16(0x1021, hyundai_canfd_crc_lut);
  hyundai_canfd_alt_buttons = GET_FLAG(param, HYUNDAI_PARAM_CANFD_ALT_BUTTONS);
  hyundai_canfd_hda2_alt_steering = GET_FLAG(param, HYUNDAI_PARAM_CANFD_HDA2_ALT_STEERING);
  hyundai_canfd_buffered_fwd = hyundai_camera_scc;

  // no long for radar-SCC HDA1 yet
  //if (!hyundai_canfd_hda2 && !hyundai_camera_scc) {
  //    hyundai_longitudinal = false;
  //}
  safety_config ret;
  if (hyundai_longitudinal) {
    if (hyundai_canfd_hda2) {
        print("hyundai safety canfd_hda2 long-");
        if(hyundai_camera_scc) print("camera_scc \n");
        else print("no camera_scc \n");
        if (hyundai_canfd_alt_buttons) {          // carrot : for CANIVAL 4TH HDA2
            print("hyundai safety canfd_hda2 long_alt_buttons\n");
            if (hyundai_camera_scc) ret = BUILD_SAFETY_CFG(hyundai_canfd_hda2_long_alt_buttons_rx_checks_scc2, HYUNDAI_CANFD_HDA2_LONG_TX_MSGS);                
            else ret = BUILD_SAFETY_CFG(hyundai_canfd_hda2_long_alt_buttons_rx_checks, HYUNDAI_CANFD_HDA2_LONG_TX_MSGS);
        }
        else {
            if (hyundai_camera_scc) ret = BUILD_SAFETY_CFG(hyundai_canfd_hda2_long_rx_checks_scc2, HYUNDAI_CANFD_HDA2_LONG_TX_MSGS);
            else ret = BUILD_SAFETY_CFG(hyundai_canfd_hda2_long_rx_checks, HYUNDAI_CANFD_HDA2_LONG_TX_MSGS);
        }
    } else {
      if(hyundai_canfd_alt_buttons) print("hyundai safety canfd_hda1 long alt_buttons\n");
      else print("hyundai safety canfd_hda1 long general_buttons\n");

      ret = hyundai_canfd_alt_buttons ? BUILD_SAFETY_CFG(hyundai_canfd_long_alt_buttons_rx_checks, HYUNDAI_CANFD_HDA1_TX_MSGS) : \
                                        BUILD_SAFETY_CFG(hyundai_canfd_long_rx_checks, HYUNDAI_CANFD_HDA1_TX_MSGS);
    }
  } else {
    print("hyundai safety canfd_hda2 stock");
    if (hyundai_camera_scc) print("camera_scc \n");
    else print("no camera_scc \n");
    if (hyundai_canfd_hda2 && hyundai_camera_scc) {
      if (hyundai_canfd_alt_buttons) { // carrot : for CANIVAL 4TH HDA2
        ret = hyundai_canfd_hda2_alt_steering ? BUILD_SAFETY_CFG(hyundai_canfd_hda2_alt_buttons_rx_checks_scc2, HYUNDAI_CANFD_HDA2_LONG_TX_MSGS) : \
          BUILD_SAFETY_CFG(hyundai_canfd_hda2_alt_buttons_rx_checks_scc2, HYUNDAI_CANFD_HDA2_LONG_TX_MSGS);
      }
      else {
        ret = hyundai_canfd_hda2_alt_steering ? BUILD_SAFETY_CFG(hyundai_canfd_hda2_rx_checks_scc2, HYUNDAI_CANFD_HDA2_LONG_TX_MSGS) : \
          BUILD_SAFETY_CFG(hyundai_canfd_hda2_rx_checks_scc2, HYUNDAI_CANFD_HDA2_LONG_TX_MSGS);
      }
    }else if (hyundai_canfd_hda2) {
        if (hyundai_canfd_alt_buttons) { // carrot : for CANIVAL 4TH HDA2
            ret = hyundai_canfd_hda2_alt_steering ? BUILD_SAFETY_CFG(hyundai_canfd_hda2_alt_buttons_rx_checks, HYUNDAI_CANFD_HDA2_LONG_TX_MSGS) : \
                BUILD_SAFETY_CFG(hyundai_canfd_hda2_alt_buttons_rx_checks, HYUNDAI_CANFD_HDA2_LONG_TX_MSGS);
        }
        else {
            ret = hyundai_canfd_hda2_alt_steering ? BUILD_SAFETY_CFG(hyundai_canfd_hda2_rx_checks, HYUNDAI_CANFD_HDA2_LONG_TX_MSGS) : \
                BUILD_SAFETY_CFG(hyundai_canfd_hda2_rx_checks, HYUNDAI_CANFD_HDA2_LONG_TX_MSGS);
        }
    } else if (!hyundai_camera_scc) {
      static RxCheck hyundai_canfd_radar_scc_alt_buttons_rx_checks[] = {
        HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
        HYUNDAI_CANFD_ALT_BUTTONS_ADDR_CHECK(0)
        HYUNDAI_CANFD_SCC_ADDR_CHECK(0)
      };

      // Radar sends SCC messages on these cars instead of camera
      static RxCheck hyundai_canfd_radar_scc_rx_checks[] = {
        HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
        HYUNDAI_CANFD_BUTTONS_ADDR_CHECK(0)
        HYUNDAI_CANFD_SCC_ADDR_CHECK(0)
      };

      ret = hyundai_canfd_alt_buttons ? BUILD_SAFETY_CFG(hyundai_canfd_radar_scc_alt_buttons_rx_checks, HYUNDAI_CANFD_HDA1_TX_MSGS) : \
                                        BUILD_SAFETY_CFG(hyundai_canfd_radar_scc_rx_checks, HYUNDAI_CANFD_HDA1_TX_MSGS);
    } else {
      // *** Non-HDA2 checks ***
      static RxCheck hyundai_canfd_alt_buttons_rx_checks[] = {
        HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
        HYUNDAI_CANFD_ALT_BUTTONS_ADDR_CHECK(0)
        HYUNDAI_CANFD_SCC_ADDR_CHECK(2)
      };

      // Camera sends SCC messages on HDA1.
      // Both button messages exist on some platforms, so we ensure we track the correct one using flag
      static RxCheck hyundai_canfd_rx_checks[] = {
        HYUNDAI_CANFD_COMMON_RX_CHECKS(0)
        HYUNDAI_CANFD_BUTTONS_ADDR_CHECK(0)
        HYUNDAI_CANFD_SCC_ADDR_CHECK(2)
      };

      ret = hyundai_canfd_alt_buttons ? BUILD_SAFETY_CFG(hyundai_canfd_alt_buttons_rx_checks, HYUNDAI_CANFD_HDA1_TX_MSGS) : \
                                        BUILD_SAFETY_CFG(hyundai_canfd_rx_checks, HYUNDAI_CANFD_HDA1_TX_MSGS);
    }
  }

  return ret;
}

const safety_hooks hyundai_canfd_hooks = {
  .init = hyundai_canfd_init,
  .rx = hyundai_canfd_rx_hook,
  .tx = hyundai_canfd_tx_hook,
  .fwd = hyundai_canfd_fwd_hook,
  .get_counter = hyundai_canfd_get_counter,
  .get_checksum = hyundai_canfd_get_checksum,
  .compute_checksum = hyundai_common_canfd_compute_checksum,
};
