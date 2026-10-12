#pragma once
static unsigned char crc_checksum(const unsigned char *p, unsigned short n, unsigned char polynomial) {
 unsigned char c=0; for(unsigned short i=0;i<n;i++){c^=p[i];for(int j=0;j<8;j++)c=(c&0x80)?(unsigned char)((c<<1)^polynomial):(unsigned char)(c<<1);}return c;
}
