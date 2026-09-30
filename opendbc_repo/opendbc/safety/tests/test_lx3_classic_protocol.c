// Compile the actual shared safety headers as a classic, non-CANFD board.
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
bool safety_tx_buffered_for_fwd = false;
void putui(uint32_t value) { (void)value; }
#include "libsafety/safety.c"
#include "../../../../panda/board/health.h"

int main(void) {
  CANPacket_t packet = {0};
  assert(sizeof(packet.data) == 8U);
  assert(HEALTH_PACKET_VERSION == 16 && sizeof(struct health_t) == 58U);
  assert(sizeof(lx3_permission_t) == 12U);
  assert(set_safety_hooks(SAFETY_NOOUTPUT, 0) == 0);
  assert(!safety_lx3_guarded());
  const lx3_permission_t state = safety_lx3_permission();
  assert(state.version == 0 && !lx3_permission_valid(&state, sizeof(state)));
  safety_host_heartbeat(1U, 65535U);
  assert(heartbeat_engaged && !controls_allowed);
  safety_host_heartbeat(lx3_heartbeat_value(true, 1, 1, 254), 1U);
  assert(!heartbeat_engaged && !controls_allowed);
  safety_host_heartbeat(0U, 0U);
  assert(!heartbeat_engaged && !controls_allowed);
  puts("PASS: actual non-CANFD safety compile, legacy heartbeat and health-v16 layout preserved");
  return 0;
}
