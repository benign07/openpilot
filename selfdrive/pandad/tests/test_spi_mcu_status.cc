// Actual Panda optional diagnostic client with inert control transfers.
#include <cassert>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include "selfdrive/pandad/panda.h"

double spi_mcu_fixture_time_ms = 6000;
static int response;
static unsigned reads, writes;
static uint64_t epoch;
PandaSpiHandle::PandaSpiHandle(std::string) { hw_serial="inert-fixture"; }
PandaSpiHandle::~PandaSpiHandle() = default;
int PandaSpiHandle::control_read(uint8_t req,uint16_t p1,uint16_t p2,unsigned char*data,uint16_t length,unsigned timeout) {
  if (req==0xc1) {assert(length==1);data[0]=0;return 1;}
  if (req==LX3_STATUS_REQUEST) {
    assert(length==sizeof(lx3_status_t));lx3_status_t s{};
    s.version=LX3_PROTOCOL_VERSION;s.profile=1;s.epoch=epoch;std::memcpy(data,&s,sizeof(s));return sizeof(s);
  }
  if (req==0xd2) {
    assert(length==sizeof(health_t));health_t h{};h.voltage_pkt=12000;std::memcpy(data,&h,sizeof(h));return sizeof(h);
  }
  assert(req==SPI_DIAGNOSTICS_REQUEST && p1==SPI_DIAGNOSTICS_VERSION && p2==0 && length==SPI_DIAGNOSTICS_SIZE && timeout==20);
  ++reads;
  if (response<=0) return response;
  spi_diagnostics_t s{};s.version=response==2?99:SPI_DIAGNOSTICS_VERSION;s.size=sizeof(s);
  s.header_checksum_bad=123;s.can_backpressure_nack=456;std::memcpy(data,&s,sizeof(s));return sizeof(s);
}
int PandaSpiHandle::control_write(uint8_t req,uint16_t p1,uint16_t p2,unsigned) {
  ++writes;
  if (req==LX3_EPOCH_HIGH_REQUEST) epoch=((uint64_t)p1<<48U)|((uint64_t)p2<<32U);
  else if (req==LX3_EPOCH_LOW_REQUEST) epoch|=((uint64_t)p1<<16U)|p2;
  else assert(req==0xc0 || req==0xdc);
  return 0;
}
int PandaSpiHandle::bulk_read(unsigned char,unsigned char*,int,unsigned) {assert(false);return -1;}
int PandaSpiHandle::bulk_write(unsigned char,unsigned char*,int,unsigned) {assert(false);return -1;}
std::vector<std::string> PandaSpiHandle::list() {return {};}
int main(int argc,char**argv) {
  assert(argc==2);response=std::atoi(argv[1]);Panda panda("inert-fixture");
  assert(panda.get_state()->voltage_pkt==12000 && reads==0); // Other cars: no additional polling.
  panda.set_safety_model(cereal::CarParams::SafetyModel::HYUNDAI_CANFD,2048);
  assert(panda.lx3_guarded());const unsigned before_writes=writes;
  spi_mcu_fixture_time_ms=7000;
  assert(panda.get_state()->voltage_pkt==12000 && reads==1);
  spi_mcu_fixture_time_ms=7001;
  assert(panda.get_state()->voltage_pkt==12000 && reads==1);
  spi_mcu_fixture_time_ms=12000;
  assert(panda.get_state()->voltage_pkt==12000);
  assert(reads==(response==0 || response==2?1U:2U));
  assert(panda.lx3_guarded() && writes==before_writes);
  const unsigned before_reconfigure_reads=reads;
  panda.set_safety_model(cereal::CarParams::SafetyModel::HYUNDAI_CANFD,2048);
  assert(panda.lx3_guarded());const unsigned after_reconfigure_writes=writes;
  spi_mcu_fixture_time_ms=17000;
  assert(panda.get_state()->voltage_pkt==12000 && reads==before_reconfigure_reads+1);
  assert(writes==after_reconfigure_writes);
  std::printf("PASS actual optional MCU stats response %d: 5s rate limit, health preserved, no authority writes, guarded reconfiguration retries\n",response);
}
