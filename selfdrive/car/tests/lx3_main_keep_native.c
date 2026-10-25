// Offline production authority/physical decoder fixture. Healthy input and
// clocks are dependencies, never an actual board, transport or actuator test.
#include <string.h>
#include "lx3_authority.h"
static lx3_authority_t auth;
static lx3_buttons_t buttons;
static uint32_t now;
static uint8_t counter;
void fixture_reset(int always) {
  auth=(lx3_authority_t){.epoch=12345U,.config={.always_lateral=always!=0,.lfa_long_us=700000U}};
  buttons=(lx3_buttons_t){0};now=1000000U;counter=248U;
}
int fixture_frame(int key) {
  now+=40000U;counter+=2U;lx3_buttons_feed(&buttons,now,counter,(uint8_t)key);
  for (uint8_t i=0U;i<buttons.count;i++) lx3_authority_button(&auth,&buttons.events[i],now,buttons.ready);
  lx3_authority_maintain(&auth,now,buttons.ready);return buttons.count;
}
void fixture_event(int index,uint64_t *out) {
  const lx3_button_event_t *e=&buttons.events[index];out[0]=e->button;out[1]=e->pressed;out[2]=e->counter;
}
uint32_t fixture_now(void) {return now;}
void fixture_status(uint64_t *out) {
  out[0]=auth.epoch;out[1]=auth.allowed;out[2]=auth.lateral_armed|(auth.longitudinal_armed?2U:0U);
  out[3]=auth.lateral_generation;out[4]=auth.longitudinal_generation;
  out[5]=auth.pending_key;out[6]=auth.pending_generation;
  out[7]=auth.pending_generation?(now-auth.pending_us)/1000U:65535U;out[8]=auth.pending_axes;
  out[9]=auth.pending_axis_key[1];out[10]=auth.pending_axis_generation[1];
  out[11]=auth.pending_axis_generation[1]?(now-auth.pending_axis_us[1])/1000U:65535U;
  out[12]=auth.pending_axis_generation[1]?2U:0U;out[13]=auth.reason;
  out[14]=auth.longitudinal_revision;out[15]=buttons.ready;
}
int fixture_heartbeat(int intent,int generation,int key) {
  return lx3_authority_heartbeat(&auth,now,auth.epoch,(uint8_t)intent,(uint16_t)generation,(uint16_t)key,
                                 false,buttons.ready,false,false,auth.longitudinal_revision);
}
void fixture_fault(void) {lx3_authority_maintain(&auth,now,false);}
int fixture_identity(int generation) {return lx3_authority_identity(&auth,auth.epoch,LX3_LAT,(uint16_t)generation);}
