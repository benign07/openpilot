// Actual Panda constructor and get_hw_type; inert SPI handle, no device I/O.
#include <cassert>
#include <cstdlib>
#include <cstdio>
#include <stdexcept>
#include "selfdrive/pandad/panda.h"

static int response = 1;
static unsigned writes = 0;
PandaSpiHandle::PandaSpiHandle(std::string serial) { hw_serial = "inert-fixture"; }
PandaSpiHandle::~PandaSpiHandle() = default;
int PandaSpiHandle::control_read(uint8_t request, uint16_t, uint16_t, unsigned char *data, uint16_t length, unsigned) {
  assert(request == 0xc1 && length == 1);
  if (response == 1) data[0] = static_cast<uint8_t>(cereal::PandaState::PandaType::WHITE_PANDA);
  return response;
}
int PandaSpiHandle::control_write(uint8_t request, uint16_t, uint16_t, unsigned) {
  assert(request == 0xc0); ++writes; return 0;
}
int PandaSpiHandle::bulk_read(unsigned char, unsigned char *, int, unsigned) { assert(false); return -1; }
int PandaSpiHandle::bulk_write(unsigned char, unsigned char *, int, unsigned) { assert(false); return -1; }
std::vector<std::string> PandaSpiHandle::list() { return {}; }

int main(int argc, char **argv) {
  assert(argc == 2); response = std::atoi(argv[1]);
  bool failed = false;
  try {
    Panda panda("inert-fixture");
    assert(panda.hw_type == cereal::PandaState::PandaType::WHITE_PANDA);
  } catch (const std::runtime_error &) { failed = true; }
  assert(failed == (response != 1));
  assert(writes == (response == 1 ? 1U : 0U));
  std::printf("PASS actual Panda hardware-type read length %d: construction/failure before reset verified\n", response);
}
