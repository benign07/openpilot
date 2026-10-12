
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#define ENABLE_SPI 1
#define MIN(a,b) ((a)<(b)?(a):(b))
static uint8_t uid[12]={0};
#define UID_BASE uid
#define USB_PID 0xDD
static uint8_t hw_type=7;
typedef struct {uint8_t request;uint16_t param1,param2,length;} ControlPacket_t;
static int sent_size;
void llspi_init(void){}
void llspi_mosi_dma(uint8_t*a,int n){(void)a;(void)n;}
void llspi_miso_dma(uint8_t*a,int n){(void)a;sent_size=n;}
static uint16_t comms_control_handler(ControlPacket_t*p,uint8_t*out){(void)p;(void)out;return 0;}
static uint16_t comms_can_read(uint8_t*out,uint16_t n){(void)out;(void)n;return 0;}
static void comms_endpoint2_write(uint8_t*p,uint16_t n){(void)p;(void)n;}
static void comms_can_write(uint8_t*p,uint16_t n){(void)p;(void)n;}
static void print(const char*p){(void)p;}
static void puth(unsigned v){(void)v;}
#include "spi.h"
static void valid_header(void){
 spi_state=SPI_STATE_HEADER;memset(spi_buf_rx,0,SPI_HEADER_SIZE);
 spi_buf_rx[0]=SPI_SYNC_BYTE;spi_buf_rx[1]=0xAB;
 spi_buf_rx[6]=SPI_CHECKSUM_START;for(int i=0;i<6;i++)spi_buf_rx[6]^=spi_buf_rx[i];
}
int main(void){
 spi_checksum_error_count=0;valid_header();spi_rx_done();
 assert(spi_checksum_error_count==0 && spi_buf_tx[0]==SPI_HACK);
 printf("VALID_HEADER counter=0 PASS\n");
 spi_state=SPI_STATE_HEADER;memcpy(spi_buf_rx,"VERSION",7);spi_rx_done();
 assert(spi_checksum_error_count==0 && sent_size==25 && memcmp(spi_buf_tx,"VERSION",7)==0);
 assert(spi_buf_tx[24]==crc_checksum(spi_buf_tx,24,0xD5));
 printf("VALID_VERSION_RESPONSE counter=0 PASS\n");
 valid_header();spi_buf_rx[6]^=1;spi_rx_done();
 assert(spi_checksum_error_count==1 && spi_buf_tx[0]==SPI_NACK);
 printf("BAD_HEADER counter=1 PASS\n");
 valid_header();spi_rx_done();spi_state=SPI_STATE_DATA_RX;spi_buf_rx[7]=SPI_CHECKSUM_START;spi_rx_done();
 assert(spi_checksum_error_count==1 && spi_buf_tx[0]==SPI_DACK);
 printf("VALID_DATA counter=1 PASS\n");
 valid_header();spi_rx_done();spi_state=SPI_STATE_DATA_RX;spi_buf_rx[7]=SPI_CHECKSUM_START^1;spi_rx_done();
 assert(spi_checksum_error_count==2 && spi_buf_tx[0]==SPI_NACK);
 printf("BAD_DATA_CHECKSUM counter=2 PASS\n");
 spi_checksum_error_count=UINT16_MAX;spi_state=SPI_STATE_HEADER;memcpy(spi_buf_rx,"VERSION",7);spi_rx_done();assert(spi_checksum_error_count==UINT16_MAX);
 printf("COUNTER_SATURATION PASS\n");
 assert(sizeof(spi_diagnostics_t)==SPI_DIAGNOSTICS_SIZE);
 assert(spi_diagnostics.version_requests==2 && spi_diagnostics.header_checksum_bad==1 && spi_diagnostics.data_checksum_bad==1);
 assert(spi_diagnostics.header_sync_bad==0 && spi_diagnostics.unexpected_rx_state==0);
 // Wrong sync with a valid XOR is its own cause, not a checksum increment.
 spi_checksum_error_count=0;valid_header();spi_buf_rx[0]^=1;spi_buf_rx[6]^=1;spi_rx_done();
 assert(spi_checksum_error_count==0 && spi_diagnostics.header_sync_bad==1 && spi_diagnostics.header_checksum_bad==1);
 // Recovery/ACK filler classifications overlap checksum/sync counts intentionally.
 const uint8_t fills[]={0x11,0x13,0x14};
 for(unsigned j=0;j<3;j++){spi_state=SPI_STATE_HEADER;memset(spi_buf_rx,fills[j],SPI_HEADER_SIZE);spi_rx_done();}
 assert(spi_diagnostics.header_fill_11==1 && spi_diagnostics.header_fill_13==1 && spi_diagnostics.header_fill_14==1);
 assert(spi_checksum_error_count==3 && spi_diagnostics.header_checksum_bad==4);
 // Valid CAN backpressure and an unsupported endpoint are not bad checksums.
 valid_header();spi_buf_rx[1]=3;spi_buf_rx[2]=1;spi_buf_rx[6]=SPI_CHECKSUM_START;
 for(unsigned i=0;i<6;i++)spi_buf_rx[6]^=spi_buf_rx[i];
 spi_rx_done();spi_state=SPI_STATE_DATA_RX;spi_buf_rx[7]=0;spi_buf_rx[8]=SPI_CHECKSUM_START;spi_can_tx_ready=false;spi_rx_done();
 assert(spi_diagnostics.can_backpressure_nack==1 && spi_checksum_error_count==3);
 spi_buf_rx[1]=0xFE;spi_state=SPI_STATE_DATA_RX;spi_rx_done();
 assert(spi_diagnostics.other_nack==1 && spi_checksum_error_count==3);
 spi_state=SPI_STATE_HEADER_ACK;spi_rx_done();
 assert(spi_diagnostics.unexpected_rx_state==1 && spi_checksum_error_count==4);
 assert(spi_diagnostics.last_unexpected_rx_state==SPI_STATE_HEADER_ACK);
 spi_state=SPI_STATE_DATA_RX;spi_tx_done(false);
 assert(spi_diagnostics.unexpected_tx_state==1 && spi_checksum_error_count==4);
 assert(spi_diagnostics.last_unexpected_tx_state==SPI_STATE_DATA_RX);
 spi_diagnostics.header_checksum_bad=UINT32_MAX;valid_header();spi_buf_rx[6]^=1;spi_rx_done();
 assert(spi_diagnostics.header_checksum_bad==UINT32_MAX);
 printf("CAUSE_COUNTERS checksum/sync/state/backpressure/other/VERSION/filler/saturation PASS\n");return 0;
}
