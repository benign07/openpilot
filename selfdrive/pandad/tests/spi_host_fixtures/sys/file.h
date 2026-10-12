#pragma once
#define LOCK_EX 1
#define LOCK_UN 2
inline int flock(int,int){return 0;}
