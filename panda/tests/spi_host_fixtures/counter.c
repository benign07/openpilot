
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
 printf("COUNTER_SATURATION PASS\n");return 0;
}
