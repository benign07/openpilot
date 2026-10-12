// Actual production SPI state machine; inert DMA records unsafe requested lengths.
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#define ENABLE_SPI 1
static uint8_t uid[12];
#define UID_BASE uid
#define USB_PID 0xDD
static uint8_t hw_type=7;
typedef struct {uint8_t request;uint16_t param1,param2,length;} ControlPacket_t;
static int rx_size;
void llspi_init(void) {}
void llspi_mosi_dma(uint8_t*a,int n) {(void)a;rx_size=n;}
void llspi_miso_dma(uint8_t*a,int n) {(void)a;(void)n;}
static uint16_t comms_control_handler(ControlPacket_t*p,uint8_t*out) {(void)p;(void)out;return 0;}
static uint16_t comms_can_read(uint8_t*out,uint16_t n) {(void)out;return n;}
static void comms_endpoint2_write(uint8_t*p,uint16_t n) {(void)p;(void)n;}
static void comms_can_write(uint8_t*p,uint16_t n) {(void)p;(void)n;}
static void print(const char*p) {(void)p;}
static void puth(unsigned v) {(void)v;}
#include "spi.h"
static void header(uint16_t tx,uint16_t rx) {
 spi_state=SPI_STATE_HEADER;memset(spi_buf_rx,0,SPI_HEADER_SIZE);rx_size=0;
 spi_buf_rx[0]=SPI_SYNC_BYTE;spi_buf_rx[1]=0xAB;
 spi_buf_rx[2]=tx;spi_buf_rx[3]=tx>>8;spi_buf_rx[4]=rx;spi_buf_rx[5]=rx>>8;
 spi_buf_rx[6]=SPI_CHECKSUM_START;
 for(unsigned i=0;i<6;i++)spi_buf_rx[6]^=spi_buf_rx[i];
 spi_rx_done();
}
int main(int argc,char**argv) {
 bool tx=argc>1 && strcmp(argv[1],"tx")==0;
 header(tx?UINT16_MAX:0,tx?0:UINT16_MAX);
 // Old code ACKs this XOR-valid header, then arms an out-of-bounds RX DMA
 // or permits a response which overflows the TX buffer. Never perform it here.
 assert(spi_state==SPI_STATE_HEADER_NACK && spi_buf_tx[0]==SPI_NACK);
 spi_tx_done(false);assert(rx_size==SPI_HEADER_SIZE);
 header(SPI_BUF_SIZE-SPI_HEADER_SIZE-1,SPI_BUF_SIZE-4);
 assert(spi_state==SPI_STATE_HEADER_ACK && spi_buf_tx[0]==SPI_HACK);
 spi_tx_done(false);assert(rx_size==SPI_BUF_SIZE-SPI_HEADER_SIZE);
 printf("PASS SPI MCU %s bounds reject before DMA/response; maximum valid header preserved\n",tx?"tx":"rx");
 return 0;
}
