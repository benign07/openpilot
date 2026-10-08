"""Exercise real comms/rings and pre-TXBAR code with mock registers, not hardware.

The one source adaptation is uint32_t->uintptr_t for the mock RAM base on a
64-bit host. All packet admission, queues, stamps and final policy are unchanged.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def function(text, name):
  start = text.index('void ' + name + '(')
  left = text.index('{', start)
  depth = 1
  end = left + 1
  while depth:
    depth += (text[end] == '{') - (text[end] == '}')
    end += 1
  return text[start:end]


PREFIX = r'''
#define LX3_BOARD_TEST
#define main policy_fixture_main
#include "opendbc_repo/opendbc/safety/tests/test_lx3_authority_native.c"
#undef main
#include <stddef.h>
_Static_assert(offsetof(CANPacket_t, data) == CANPACKET_HEAD_SIZE, "test compiler must use production GNU bitfield layout");
static unsigned critical_depth;
#define ENTER_CRITICAL() (critical_depth++)
#define EXIT_CRITICAL() do { assert(critical_depth > 0U); critical_depth--; } while (0)
static const uint8_t PANDA_CAN_CNT = 3U, PANDA_BUS_CNT = 3U;
#include "panda/board/health.h"
static void mock_set_can_mode(uint8_t mode) { (void)mode; }
static struct { uint8_t status; } harness = {0};
static struct { bool has_canfd; void (*set_can_mode)(uint8_t); } mock_board = {true, mock_set_can_mode}, *current_board = &mock_board;
bool can_init(uint8_t n) { (void)n; return true; }
void refresh_can_tx_slots_available(void);
void can_tx_comms_resume_usb(void) {}
void can_tx_comms_resume_spi(void) {}
#define MAX_CAN_MSGS_PER_USB_BULK_TRANSFER 4U
#define MAX_CAN_MSGS_PER_SPI_BULK_TRANSFER 4U
#include "panda/board/drivers/can_common.h"
#include "panda/board/can_comms.h"
typedef struct { uint32_t IR, TXFQS, TXBAR; } FDCAN_GlobalTypeDef;
#include "panda/board/drivers/fdcan_declarations.h"
static FDCAN_GlobalTypeDef registers[3];
FDCAN_GlobalTypeDef *cans[3] = {&registers[0], &registers[1], &registers[2]};
static canfd_fifo ram[3];
#define FDCAN_START_ADDRESS ((uintptr_t)&ram[0])
#define FDCAN_OFFSET sizeof(canfd_fifo)
#define FDCAN_RX_FIFO_0_EL_CNT 0U
#define FDCAN_RX_FIFO_0_EL_SIZE sizeof(canfd_fifo)
#define FDCAN_TX_FIFO_EL_SIZE sizeof(canfd_fifo)
#define FDCAN_IR_TFE 1U
#define FDCAN_TXFQS_TFQF 2U
#define FDCAN_TXFQS_TFQPI_Pos 16U
'''

SUFFIX = r'''
static void wire(CANPacket_t p, uint16_t generation, unsigned chunk) {
  can_set_checksum(&p);
  lx3_tx_identity_t id = {epoch, generation, lx3_packet_axis(&p), true};
  uint8_t prefix[8], incarnation[8], bytes[128];
  lx3_tx_encode(&id, (uint8_t*)&p, CANPACKET_HEAD_SIZE + GET_LEN(&p), prefix, incarnation);
  CANPacket_t m = {0}; m.bus=7U; m.extended=1U; m.data_len_code=8U;
  m.addr=LX3_TX_PREFIX_ADDR; memcpy(m.data,prefix,8U); can_set_checksum(&m);
  memcpy(bytes,&m,14U);
  m.addr=LX3_TX_EPOCH_ADDR; memcpy(m.data,incarnation,8U); can_set_checksum(&m);
  memcpy(bytes+14U,&m,14U); memcpy(bytes+28U,&p,CANPACKET_HEAD_SIZE+GET_LEN(&p));
  unsigned size=28U+CANPACKET_HEAD_SIZE+GET_LEN(&p);
  for (unsigned pos=0; pos<size;) {
    unsigned n=MIN(chunk,size-pos); comms_can_write(bytes+pos,n); pos+=n;
  }
}
static void board_reset(void) {
  comms_can_reset();
  for (unsigned i=0U;i<3U;i++) { can_clear(can_queues[i]); registers[i]=(FDCAN_GlobalTypeDef){0}; }
  can_clear(&can_rx_q); memset(ram,0,sizeof(ram)); memset(can_health,0,sizeof(can_health));
  safety_tx_blocked=0U; reset();
}
static void stage_to_busy_hardware(void) {
  CANPacket_t source=cb(2U,90U,0); source.bus=2U;
  assert(safety_fwd_hook(&source)==0);
  registers[0].TXFQS=FDCAN_TXFQS_TFQF;
  can_send(&source,0U,true);
  assert(can_tx1_q.w_ptr!=can_tx1_q.r_ptr && registers[0].TXBAR==0U);
}
static void fragmented_one_shot(void) {
  for (unsigned chunk=1U;chunk<=64U;chunk++) {
    board_reset(); lfa(); cite(LX3_LAT);
    CANPacket_t p=cb(2U,25U,0);
    wire(p,lx3_auth.lateral_generation,chunk);
    assert(canfd_bfwd_find(0xCB,0)->count==1U);
    assert(can_rx_q.w_ptr==can_rx_q.r_ptr); // Private markers never echoed/sent.
    assert(can_tx1_q.w_ptr==can_tx1_q.r_ptr && registers[0].TXBAR==0U);
    can_set_checksum(&p);
    comms_can_write((uint8_t*)&p,CANPACKET_HEAD_SIZE+GET_LEN(&p));
    assert(safety_tx_blocked==1U && canfd_bfwd_find(0xCB,0)->count==1U);
    assert(critical_depth==0U);
  }
  puts("PASS real comms 64 fragmentation sizes; marker consumed once; no private CAN/echo leakage");
}
static void revoke_after_software_enqueue(void) {
  board_reset(); lfa(); cite(LX3_LAT);
  CANPacket_t p=cb(2U,25U,0); const uint16_t old=lx3_auth.lateral_generation;
  wire(p,old,7U); stage_to_busy_hardware();
  state(0U,0U,0U,false,lx3_auth.longitudinal_revision);
  CANPacket_t off=cb(1U,0U,0); wire(off,old,3U); stage_to_busy_hardware();
  registers[0].TXFQS=0U;
  process_can(0U);
  assert(registers[0].TXBAR==1U && can_health[0].total_tx_cnt==1U);
  assert(safety_tx_blocked==1U && can_tx1_q.w_ptr==can_tx1_q.r_ptr);
  assert(((ram[0].data_word[0]>>28U)&3U)==1U); // Inactive reaches TXBAR, active does not.
  CANPacket_t echo;
  assert(can_pop(&can_rx_q,&echo) && echo.returned && echo.data[6]==0U);
  assert(!can_pop(&can_rx_q,&echo));
  assert(critical_depth==0U);
  puts("PASS revoke after real software enqueue drops active and submits following inactive in same IRQ");
}
static void generation_and_mode_change(void) {
  board_reset(); lfa(); cite(LX3_LAT);
  CANPacket_t p=cb(2U,25U,0); uint16_t old=lx3_auth.lateral_generation;
  wire(p,old,9U); stage_to_busy_hardware();
  state(0U,0U,0U,false,lx3_auth.longitudinal_revision); lfa(); cite(LX3_LAT);
  assert(lx3_auth.lateral_generation!=old);
  registers[0].TXFQS=0U; process_can(0U);
  assert(registers[0].TXBAR==0U && safety_tx_blocked==1U);
  wire(p,lx3_auth.lateral_generation,11U); stage_to_busy_hardware();
  assert(set_safety_hooks(SAFETY_HYUNDAI_CANFD,190U)==0);
  registers[0].TXFQS=0U; process_can(0U);
  assert(registers[0].TXBAR==0U && safety_tx_blocked==2U);
  puts("PASS queued old generation and guarded-to-legacy mode change cannot reach TXBAR");
}
int main(void) { fragmented_one_shot(); revoke_after_software_enqueue(); generation_and_mode_change(); return 0; }
'''


def main():
  p = argparse.ArgumentParser(description=__doc__)
  p.add_argument('--compiler', nargs='+', default=['cc'])
  p.add_argument('--output', type=Path, required=True)
  p.add_argument('--sanitize', action='store_true')
  a = p.parse_args(); a.output.mkdir(parents=True, exist_ok=True)
  original = function((ROOT / 'panda/board/drivers/fdcan.h').read_text(encoding='utf-8'), 'process_can')
  assert original.count('uint32_t TxFIFOSA') == 1
  body = original.replace('uint32_t TxFIFOSA', 'uintptr_t TxFIFOSA')
  source = a.output / 'board-delivery.c'; source.write_text(PREFIX + body + SUFFIX, encoding='utf-8')
  exe = a.output / ('board-delivery.exe' if os.name == 'nt' else 'board-delivery')
  command = [*a.compiler, '-std=gnu11', '-Wall', '-Wextra', '-Werror', '-Wno-pointer-to-int-cast',
    '-I'+str(ROOT), '-I'+str(ROOT/'opendbc_repo/opendbc/safety/board'),
    '-I'+str(ROOT/'opendbc_repo/opendbc/safety'), str(source), '-lm', '-o', str(exe)]
  if a.sanitize: command += ['-fsanitize=undefined', '-fno-sanitize-recover=undefined', '-g']
  if os.name == 'nt': command += ['-mno-ms-bitfields']
  build = subprocess.run(command, capture_output=True)
  (a.output/'build.log').write_bytes(build.stdout+build.stderr)
  run = subprocess.run([str(exe.resolve())], capture_output=True) if build.returncode == 0 else None
  (a.output/'test.log').write_bytes(run.stdout+run.stderr if run else b'NOT RUN')
  evidence = {'scope':__doc__, 'build':build.returncode, 'test':run.returncode if run else None,
    'process_can_source_sha256':hashlib.sha256(original.encode()).hexdigest(),
    'fixture_sha256':hashlib.sha256(source.read_bytes()).hexdigest(), 'command':command}
  (a.output/'result.json').write_text(json.dumps(evidence,indent=2),encoding='utf-8')
  print((build.stdout+build.stderr if build.returncode else run.stdout+run.stderr).decode(errors='replace'))
  return build.returncode or run.returncode


if __name__ == '__main__':
  raise SystemExit(main())
