#pragma once
#include <string>
#define O_RDWR 2
inline int open(const char*,int){return 10;}
inline int close(int){return 0;}
inline int usleep(unsigned microseconds){fake_ms += microseconds / 1000.0; return 0;}
namespace util {
inline bool file_exists(const std::string&){return true;}
inline std::string getenv(const char*,const char *value){return value;}
inline std::string hexdump(const unsigned char*,int){return "fixture";}
int safe_ioctl(int,unsigned long,void*,const char *message=nullptr);
}
