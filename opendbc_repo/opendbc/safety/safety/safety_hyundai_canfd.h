#pragma once
// [전처리] 컴파일 시 이 파일을 한 번만 포함하라는 중복 방지 명령입니다.
#include "safety_declarations.h"      // 기본 안전 선언(구조체, 도구들)을 가져옵니다.
#include "safety_hyundai_common.h"    // 현대차 공통 설정(CRC, 버튼 로직 등)을 가져옵니다.

// *** 조향 안전 제한값 설정 (Steering Limits) ***
// 오픈파일럿이 핸들을 얼마나 세게, 얼마나 빨리 돌릴 수 있는지 제한합니다.
const TorqueSteeringLimits HYUNDAI_CANFD_STEERING_LIMITS = {
  .max_steer = 512,             // [중요] 최대 조향 토크. 512면 꽤 강력하게 설정된 것입니다. (보통 270~384 사용)
  .max_rt_delta = 112,          // 실시간 토크 변화량 제한 (갑작스런 힘 변화 방지)
  .max_rt_interval = 250000,    // 위 변화량을 검사하는 시간 간격 (마이크로초)
  .max_rate_up = 2,             // 토크 올릴 때 속도 제한 (낮을수록 부드럽지만 반응이 느림)
  .max_rate_down = 3,           // 토크 내릴 때 속도 제한
  .driver_torque_allowance = 250, // 운전자가 핸들을 잡고 돌릴 때 허용하는 오차 범위
  .driver_torque_multiplier = 2,  // 운전자 개입 시 토크 감소 비율
  .type = TorqueDriverLimited,    // 운전자가 힘을 주면 모터 힘을 제한하는 방식

// EPS(조향모터) 과부하/오류 방지 설정 (조향각이 임계각을 계속 초과하면 에러남, 잠시 쉬어가는 로직)
  // 최소 89프레임 동안은 조향 요청을 유지하고, 그 후 2프레임 정도는 잠깐 쉬어준다(Cut)는 의미입니다.
  .min_valid_request_frames = 89,
  .max_invalid_request_frames = 2,
  .min_valid_request_rt_interval = 810000,  // 810ms; a ~10% buffer on cutting every 90 frames
  .has_steer_req_tolerance = true,          // 조향 요청 오차 허용 여부
};

// *** [TX 리스트] 메시지 전송 허용 목록 ***
// Panda가 차량으로 보낼 수 있는 메시지 ID와 Bus 번호를 정의합니다.

// HDA2 차량이 '순정 롱컨'일 때 보낼 메시지 목록
const CanMsg HYUNDAI_CANFD_HDA2_TX_MSGS[] = {
  {0x50, 0, 16},  // LKAS (기본 조향) - Bus 0
  {0x1CF, 1, 8},  // CRUISE_BUTTON (크루즈 버튼 제어) - Bus 1
  {0x2A4, 0, 24}, // CAM_0x2A4 (카메라 관련 메시지) - Bus 0
};

// HDA2 차량이 'Alt Steering' (대체 조향, 0x110) 옵션을 켰을 때 목록
const CanMsg HYUNDAI_CANFD_HDA2_ALT_STEERING_TX_MSGS[] = {
  {0x110, 0, 32}, // LKAS_ALT (대체 조향, 0x110) - Bus 0
  {0x1CF, 1, 8},  // CRUISE_BUTTON
  {0x362, 0, 32}, // CAM_0x362 (0x110과 짝꿍인 카메라 메시지)
  {0x1AA, 1, 16}, // CRUISE_ALT_BUTTONS (카니발/LX3 등 일부 차종용 대체 버튼)
};

// HDA2 차량이 '오픈파일럿 롱컨'을 쓸 때 목록
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
  {687, 2, 8}, // STEER_TOUCH_2AF

  {0x4BE, 2, 8}, // NEW_MSG_4BE (may be corner radar enabler x)
  {0x4B9, 2, 8}, // NEW_MSG_4B9 (may be corner radar enabler)
};

// HDA1 (구형) 차량용 목록
const CanMsg HYUNDAI_CANFD_HDA1_TX_MSGS[] = {
  {0x12A, 0, 16}, // LFA
  {0x1A0, 0, 32}, // CRUISE_INFO
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
  {687, 2, 8}, // STEER_TOUCH_2AF

};


// *** 수신 메시지 검사 (RX Checks) 정의 ***
// 차가 보내는 메시지가 정상적인지(주기, 카운터 등) 확인하는 규칙들입니다.

// 공통 검사: 가속페달, 브레이크, 휠 속도(0xa0), MDPS토크(0xea)
// 가속 페달은 EV(0x35), 하이브리드(0x105), 내연기관(0x100)에 따라 다릅니다.
#define HYUNDAI_CANFD_COMMON_RX_CHECKS(pt_bus)                                                                              \
  {.msg = {{0x35, (pt_bus), 32, .max_counter = 0xffU, .frequency = 100U},                   \
           {0x100, (pt_bus), 32, .max_counter = 0xffU, .frequency = 100U},                  \
           {0x105, (pt_bus), 32, .max_counter = 0xffU, .frequency = 100U}}},                \
  {.msg = {{0x175, (pt_bus), 24, .max_counter = 0xffU, .frequency = 50U}, { 0 }, { 0 }}},  \
  {.msg = {{0xa0, (pt_bus), 24, .max_counter = 0xffU, .frequency = 100U}, { 0 }, { 0 }}},   \
  {.msg = {{0xea, (pt_bus), 24, .max_counter = 0xffU, .frequency = 100U}, { 0 }, { 0 }}},   \

// 표준 크루즈 버튼 검사 (0x1cf)
#define HYUNDAI_CANFD_BUTTONS_ADDR_CHECK(pt_bus)                                                                            \
  {.msg = {{0x1cf, (pt_bus), 8, .ignore_checksum = true, .max_counter = 0xfU, .frequency = 50U}, { 0 }, { 0 }}}, \

// 대체 크루즈 버튼 검사 (0x1aa, 일부 차종)
#define HYUNDAI_CANFD_ALT_BUTTONS_ADDR_CHECK(pt_bus)                                                                            \
  {.msg = {{0x1aa, (pt_bus), 16, .ignore_checksum = true, .max_counter = 0xffU, .frequency = 50U}, { 0 }, { 0 }}},   \

// SCC_CONTROL (스마트 크루즈) 상태 검사 (0x1a0)
#define HYUNDAI_CANFD_SCC_ADDR_CHECK(scc_bus)                                                                                 \
  {.msg = {{0x1a0, (scc_bus), 32, .max_counter = 0xffU, .frequency = 50U}, { 0 }, { 0 }}}, \

// 파라미터 플래그 변수
//static bool hyundai_canfd_alt_buttons = false;
//static bool hyundai_canfd_hda2_alt_steering = false;

// *** 안전 설정 구성 (Safety Configs) ***
// 위에서 정의한 검사 규칙들을 조합하여 상황별(HDA1/2, 롱컨 유무 등) 세트 메뉴를 만듭니다.

// *** Non-HDA2 checks ***
// HDA1 차량은 Camera가 SCC 메시지를 보냅니다.
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

int canfd_tx_addr[32] = { 80, 81, 272, 282, 298, 352, 353, 354, 442, 485, 416, 437, 506, 474, 480, 490, 512, 676, 866, 837, 1402, 908, 1848, 1187, 1204, 203, 0, };
int canfd_tx_hz[32] = {  100,100, 100, 100, 100,  50,  20,  20,  20,  20,  50,  20,  10,   1,  20,  20,  20,  20,  10,   5,   10,   5,   10,    5,   10, 100, 0, };
uint32_t canfd_tx_timeout[32] = { 0, };
int canfd_tx_addr2[32] = { 0x4a3, 373, 506, 463, 426, 234, 687, 0 };
int canfd_tx_hz2[32] = {       5,  50,  10,  50,  50, 100, 10, 0 };
uint32_t canfd_tx_timeout2[32] = { 0, };
uint32_t canfd_tx_time[32] = { 0, };
uint32_t canfd_tx_time2[32] = { 0, };

int hyundai_canfd_hda2_get_lkas_addr(void) {
  return hyundai_canfd_hda2_alt_steering ? 0x110 : 0x50;
}

static uint8_t hyundai_canfd_get_counter(const CANPacket_t *to_push) {
  uint8_t ret = 0;
  if (GET_LEN(to_push) == 8U) {
    ret = GET_BYTE(to_push, 1) >> 4;
  } else {
    ret = GET_BYTE(to_push, 2);
  }
  return ret;
}

static uint32_t hyundai_canfd_get_checksum(const CANPacket_t *to_push) {
  uint32_t chksum = GET_BYTE(to_push, 0) | (GET_BYTE(to_push, 1) << 8);
  return chksum;
}

static void hyundai_canfd_rx_hook(const CANPacket_t *to_push) {
  int bus = GET_BUS(to_push);
  int addr = GET_ADDR(to_push);

  int pt_bus = hyundai_canfd_hda2 ? 1 : 0;
  const int scc_bus = hyundai_camera_scc ? 2 : pt_bus;

  if (hyundai_camera_scc) pt_bus = 0;

  if (bus == pt_bus) {
    // driver torque
    if (addr == 0xea) {
      int torque_driver_new = ((GET_BYTE(to_push, 11) & 0x1fU) << 8U) | GET_BYTE(to_push, 10);
      torque_driver_new -= 4095;
      update_sample(&torque_driver, torque_driver_new);
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
      hyundai_common_cruise_buttons_check(cruise_button, main_button);
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

}

// *** TX HOOK: 전송 메시지 검사 함수 ***
// 오픈파일럿이 보내려는 메시지가 안전한지 검사합니다. (문지기 역할)
static bool hyundai_canfd_tx_hook(const CANPacket_t *to_send) {
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

    bool violation = false;

    if (hyundai_longitudinal) {
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

    } else {
      // only used to cancel on here
      if ((desired_accel_raw != 0) || (desired_accel_val != 0)) {
        violation = true;
        print("no long violation\n");
      }
    }

    if (violation) {
      tx = false;
    }
  }

  for (int i = 0; canfd_tx_addr[i] > 0; i++) {
      if (addr == canfd_tx_addr[i]) canfd_tx_time[i] = (tx) ? microsecond_timer_get() : 0;
  }
  for (int i = 0; canfd_tx_addr2[i] > 0; i++) {
      if (addr == canfd_tx_addr2[i]) canfd_tx_time2[i] = (tx) ? microsecond_timer_get() : 0;
  }

  return tx;
}

int addr_list1[128] = { 0, };
int addr_list_count1 = 0;
int addr_list2[128] = { 0, };
int addr_list_count2 = 0;
#define OP_CAN_SEND_TIMEOUT 100000

// *** FWD HOOK: 메시지 전달/차단 함수 ***
// 차에서 오는 메시지를 다른 버스로 넘겨줄지, 차단할지 결정합니다.
static int hyundai_canfd_fwd_hook(int bus_num, int addr) {
  int bus_fwd = -1;
  uint32_t now = microsecond_timer_get();

  if (bus_num == 0) {
    bus_fwd = 2;
    for (int i = 0; canfd_tx_addr2[i] > 0; i++) {
        if (addr == canfd_tx_addr2[i] && (now - canfd_tx_time2[i]) < canfd_tx_timeout2[i]) {
            bus_fwd = -1;
            break;
        }
    }
    if(addr == 0x4b9) bus_fwd = -1; // maybe corner rara disabler.
  }
  if (bus_num == 1) {
      int i;
      for (i = 0; i < addr_list_count1 && i < 127; i++) {
          if (addr_list1[i] == addr) {
              break;
          }
      }
      if (i == addr_list_count1 && i!=127) {
          addr_list1[addr_list_count1] = addr;
          addr_list_count1++;
          print("!!!!! bus1_list=");
          for (int j = 0; j < addr_list_count1; j++) { putui((uint32_t)addr_list1[j]); print(","); }
          print("\n");
      }
  }
  if (bus_num == 2) {
      int i;
      for (i = 0; i < addr_list_count2 && i < 127; i++) {
          if (addr_list2[i] == addr) {
              break;
          }
      }
      if (i == addr_list_count2 && i != 127) {
          addr_list2[addr_list_count2] = addr;
          addr_list_count2++;
          print("@@@@ bus2_list=");
          for (int j = 0; j < addr_list_count2; j++) { putui((uint32_t)addr_list2[j]); print(","); }
          print("\n");
      }
#if 1
      bus_fwd = 0;
      for (int i = 0; canfd_tx_addr[i] > 0; i++) {
          if (addr == canfd_tx_addr[i] && (now - canfd_tx_time[i]) < canfd_tx_timeout[i]) {
              bus_fwd = -1;
              break;
          }
      }
      //if (addr == 353) bus_fwd = -1;
      //else if (addr == 354) bus_fwd = -1;
      //if (addr == 908) bus_fwd = -1;
      //else if (addr == 1402) bus_fwd = -1;
      //
      // 아래코드중 오토상향등코드 있음.. ㅋ
      //if (addr == 698) bus_fwd = -1;
      //if (addr == 1848) bus_fwd = -1;
      //if (addr == 1996) bus_fwd = -1;
#else
    // LKAS for HDA2, LFA for HDA1
    int hda2_lfa_block_addr = hyundai_canfd_hda2_alt_steering ? 0x362 : 0x2a4;
    bool is_lkas_msg = ((addr == hyundai_canfd_hda2_get_lkas_addr()) || (addr == hda2_lfa_block_addr)) && hyundai_canfd_hda2;
    bool is_lfa_msg = ((addr == 0x12a) && !hyundai_canfd_hda2);

    // HUD icons
    bool is_lfahda_msg = ((addr == 0x1e0) && !hyundai_canfd_hda2);

    // CRUISE_INFO for non-HDA2, we send our own longitudinal commands
    bool is_scc_msg = ((addr == 0x1a0) && hyundai_longitudinal && !hyundai_canfd_hda2);

    bool block_msg = is_lkas_msg || is_lfa_msg || is_lfahda_msg || is_scc_msg;
    if (!block_msg) {
      bus_fwd = 0;
    }
#endif
  }

  return bus_fwd;
}

// *** 초기화 함수 ***
// 파이썬 파라미터를 읽어 적절한 RX 체크 리스트와 TX 메시지 리스트를 선택합니다.
static safety_config hyundai_canfd_init(uint16_t param) {

  for (int i = 0; i < 32; i++) {
    if (canfd_tx_addr[i] > 0) canfd_tx_timeout[i] = 1. / canfd_tx_hz[i] * 1000000 + 20000;  // add 20ms for safety
    if (canfd_tx_addr2[i] > 0) canfd_tx_timeout2[i] = 1. / canfd_tx_hz2[i] * 1000000 + 20000;  // add 20ms for safety
  }

  hyundai_common_init(param);

  gen_crc_lookup_table_16(0x1021, hyundai_canfd_crc_lut);
  hyundai_canfd_alt_buttons = GET_FLAG(param, HYUNDAI_PARAM_CANFD_ALT_BUTTONS);
  hyundai_canfd_hda2_alt_steering = GET_FLAG(param, HYUNDAI_PARAM_CANFD_HDA2_ALT_STEERING);

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
