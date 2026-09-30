// Replay saved CAN into actual hooks. Never opens a CAN device or grants permission.
// Fixed little-endian record: uint32 us, uint8 kind, uint16 addr, uint8 bus,
// uint8 length, uint8 data[32]. RX=0; historical host-requested TX=1.
#define main lx3_fixture_main
#include "test_lx3_native.c"
#undef main

int main(int argc, char **argv) {
  if ((argc != 3) && (argc != 4)) return 2;
  FILE *input = fopen(argv[1], "rb");
  FILE *output = fopen(argv[2], "w");
  if ((input == NULL) || (output == NULL)) return 2;
  assert(set_safety_hooks(SAFETY_HYUNDAI_CANFD, lx3_param()) == 0);
  init_tests();
  set_alternative_experience((argc == 4 && strcmp(argv[3], "1") == 0) ? ALT_EXP_DISABLE_DISENGAGE_ON_GAS : 0);
  uint8_t record[41];
  uint32_t last_us = 0U, tick_us = 0U;
  unsigned int rx_invalid = 0U, tx_accepted = 0U, tx_rejected = 0U;
  unsigned int out_of_order = 0U, active_without_permission = 0U;
  unsigned int pending_rows = 0U, accepted_rows = 0U, active_requested = 0U;
  fprintf(output, "us,raw,mode,allowed,ready,rx_invalid,mdps_fault,brake,gas,mdps_age_us,phase,requested,generation,physical_counter,request_age_ms\n");
  while (fread(record, sizeof(record), 1U, input) == 1U) {
    uint32_t us = (uint32_t)record[0] | ((uint32_t)record[1] << 8U) | ((uint32_t)record[2] << 16U) | ((uint32_t)record[3] << 24U);
    if (us < last_us) { out_of_order++; continue; }
    last_us = us;
    set_timer(us);
    if ((tick_us != 0U) && (us - tick_us >= 1000000U)) {
      safety_tick_current_safety_config();
      tick_us = us;
    } else if (tick_us == 0U) tick_us = us;
    unsigned int addr = (unsigned int)record[5] | ((unsigned int)record[6] << 8U);
    unsigned int length = record[8], dlc = 0U;
    while ((dlc < 16U) && (dlc_to_len[dlc] != length)) dlc++;
    if (dlc == 16U) { rx_invalid++; continue; }
    CANPacket_t p = packet(addr, record[7], dlc);
    memcpy(p.data, &record[9], length);
    if (record[4] == 0U) {
      if (!safety_rx_hook(&p)) rx_invalid++;
      if ((addr == 0x10BU) && (p.bus == 0U)) {
        const lx3_permission_t permission = lx3_permission_snapshot();
        if (permission.phase == 1U) pending_rows++;
        if (permission.phase == 2U) accepted_rows++;
        fprintf(output, "%u,%u,%d,%u,%u,%u,%u,%u,%u,%u,%u,%u,%u,%u,%u\n", us, p.data[10] & 143U, lx3_mode,
                controls_allowed, lx3_button_ready, safety_rx_checks_invalid, lx3_mdps_fault,
                brake_pressed, gas_pressed, lx3_mdps_seen ? us-lx3_mdps_us : UINT32_MAX,
                permission.phase, permission.requested_mode, permission.generation,
                permission.physical_counter, permission.age_ms);
      }
    } else {
      bool allowed = controls_allowed;
      if ((addr == 0xCBU) && (p.bus == 0U) && ((p.data[3] & 0x30U) == 0x20U)) active_requested++;
      bool accepted = safety_tx_hook(&p);
      if (accepted) tx_accepted++; else tx_rejected++;
      if ((addr == 0xCBU) && (p.bus == 0U) && ((p.data[3] & 0x30U) == 0x20U) && accepted && !allowed) active_without_permission++;
    }
  }
  bool bad = ferror(input) || ferror(output);
  fclose(input); fclose(output);
  printf("ARCHIVE rx_rejected=%u tx_accepted=%u tx_rejected=%u reordered=%u active_without_permission=%u pending_rows=%u accepted_rows=%u active_requested=%u\n",
         rx_invalid, tx_accepted, tx_rejected, out_of_order, active_without_permission, pending_rows, accepted_rows, active_requested);
  // These historical logs contain no version1 host ACK. Even real physical
  // releases must not become accepted permission in this specific replay.
  return (bad || (active_without_permission != 0U) || (accepted_rows != 0U)) ? 1 : 0;
}
