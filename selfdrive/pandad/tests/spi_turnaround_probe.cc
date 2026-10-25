// Reuse the identical adversarial driver fixture, with additional wire timing
// observations. All I/O and time are inert; this cannot qualify real SPI DMA.
#include <chrono>
#include <ctime>
#define main regression_main
#include "spi_host_probe.cc"
#undef main

int main(int argc,char **argv) {
  std::string test=argc>1 ? argv[1] : "";
  if (test!="intervals" && test!="burst" && test!="sleep") return regression_main(argc,argv);
  PandaSpiHandle panda("");
  calls=0;fake_ms=0;scenario=Scenario::GOOD;
  std::vector<double> ends;
  on_transfer=[&](){ends.push_back(fake_ms);};
  unsigned char bytes[16] = {};
  if(test=="burst") {
    // Original state path: six LX3 writes, state read, health, 3 CAN-health,
    // LX3-status, heartbeat. Same call count; arbitrary inert payloads.
    const double begin=fake_ms;
    for(unsigned i=0;i<6;i++)assert(panda.control_write(0xe0+i,0,0,1)==0);
    for(unsigned i=0;i<6;i++)assert(panda.control_read(0x42+i,0,0,bytes,sizeof(bytes),1)==16);
    assert(panda.control_write(0xf3,0,0,1)==0);
    assert(calls==65);
    printf("PASS simulated13-call control occupancy %.6fms with %.3fms/IO; not hardware tick or competing-thread latency\n",fake_ms-begin,IO_MS);
    return 0;
  }
  if(test=="sleep") {
#ifdef EXPECT_SPI_TURNAROUND
    unsigned interrupted=0;
    sleep_hook=[&](unsigned){return interrupted++==0;};
    assert(panda.control_read(0x42,0,0,bytes,sizeof(bytes),1)==16);
    assert(interrupted>=2); // Recheck after an interrupted sleep.
    sleep_hook=[&](unsigned){panda.connected=false;return true;};
    auto before=calls;
    assert(panda.control_read(0x42,0,0,bytes,sizeof(bytes),1)<0);
    assert(calls==before+2); // Header+HACK, no DATA after disconnect.
    panda.connected=true;
    sleep_hook=[&](unsigned us){std::this_thread::sleep_for(std::chrono::microseconds(us));fake_ms+=us/1000.0;return true;};
    auto wall_start=std::chrono::steady_clock::now();auto cpu_start=std::clock();
    for(unsigned i=0;i<100;i++)assert(panda.control_read(0x42,0,0,bytes,sizeof(bytes),1)==16);
    auto wall=std::chrono::duration<double>(std::chrono::steady_clock::now()-wall_start).count();
    auto cpu=double(std::clock()-cpu_start)/CLOCKS_PER_SEC;
    assert(wall>=.039 && cpu<wall*.7);
    printf("PASS ready wait EINTR/disconnect; actual sleep fixture CPU %.6fs/wall %.6fs, no hardware\n",cpu,wall);
#else
    printf("PASS observed profile has no added ready-wait policy; sleep test not applicable\n");
#endif
    sleep_hook={};on_transfer={};return 0;
  }
  auto read=[&](unsigned req) {
    size_t begin=ends.size();
    assert(panda.control_read(req,0,0,bytes,sizeof(bytes),1)==16);
    assert(ends.size()==begin+5);
#ifdef EXPECT_SPI_TURNAROUND
    // After HACK ends, DATA begins at least 400us later.
    assert(ends[begin+2]-IO_MS-ends[begin+1]>=.399999);
#endif
    return begin;
  };
  auto write=[&]() {
    size_t begin=ends.size();
    assert(panda.bulk_write(3,bytes,sizeof(bytes),1)==0);
    assert(ends.size()==begin+5);
    return begin;
  };
  read(0x42);double previous=ends.back();auto second=read(0x42);
#ifdef EXPECT_SPI_TURNAROUND
  // Deliberately omit the stock 1ms control-burst delay: the original state
  // tick has more control requests than stock. Cadence still needs a bench.
  assert(ends[second]-IO_MS-previous<.001);
#endif
  previous=ends.back();auto can=write();
#ifdef EXPECT_SPI_TURNAROUND
  assert(ends[can]-IO_MS-previous>=.399999);
#endif
  read(0xdc);previous=ends.back();can=write();
#ifdef EXPECT_SPI_TURNAROUND
  assert(ends[can]-IO_MS-previous>=.999999);
#endif
  on_transfer={};
  printf("PASS original-profile SPI actual driver interval trace calls=%u simulated_ms=%.6f; no hardware\n",calls,fake_ms);
  return 0;
}
