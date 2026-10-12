#pragma once
// Unit fixture only: actual TICI logging queries device model sysfs. An ARM
// build runner has no comma model. Keep logging inert while testing SPI reads.
#define LOGD(...) ((void)0)
#define LOGW(...) ((void)0)
#define LOGE(...) ((void)0)
