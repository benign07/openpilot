#pragma once
#include <stdbool.h>
#include <stdint.h>

// Physical 0x10B only. Call after length/CRC validation, on the receive path.
// Timers are unsigned MCU microseconds; wrap is intentional. There is no
// minimum ISR spacing: several legitimate FIFO frames can be drained together.
#define LX3_BUTTON_TIMEOUT_US 200000U
#define LX3_MAIN_RELEASE_US 300000U
#define LX3_BUTTON_WIRE_PERIOD_US 40000U
#define LX3_BUTTON_LFA 128U
#define LX3_BUTTON_MAIN 8U
#define LX3_BUTTON_CANCEL 4U

typedef struct {
  uint8_t button;
  bool pressed;
  uint8_t counter;
  uint32_t held_us;
} lx3_button_event_t;

typedef struct {
  bool seen;
  bool ready;
  uint8_t counter;
  uint8_t neutral;
  uint8_t held;
  uint8_t raw_key;
  uint32_t last_us;
  uint32_t press_us;
  uint32_t sample;
  uint32_t press_sample;
  uint32_t main_press_sample;
  uint32_t main_last_sample;
  bool main_held;
  bool main_neutral;
  uint32_t main_last_us;
  uint32_t main_press_us;
  uint8_t main_release_counter;
  uint8_t count;
  lx3_button_event_t events[3];
} lx3_buttons_t;

static inline void lx3_buttons_invalidate(lx3_buttons_t *s) {
  s->ready = false;
  s->neutral = 0U;
  s->held = 0U;
  s->main_held = false;
  s->main_neutral = false;
  s->count = 0U;
}

static inline bool lx3_buttons_healthy(const lx3_buttons_t *s, uint32_t now) {
  return s->seen && s->ready && ((now - s->last_us) <= LX3_BUTTON_TIMEOUT_US);
}

static inline void lx3_buttons_event(lx3_buttons_t *s, uint8_t button, bool pressed,
                                     uint8_t counter, uint32_t held_us) {
  if (s->count < 3U) {
    s->events[s->count++] = (lx3_button_event_t){button, pressed, counter, held_us};
  }
}

static inline void lx3_buttons_feed(lx3_buttons_t *s, uint32_t now, uint8_t counter, uint8_t raw) {
  s->count = 0U;
  const uint8_t key = (raw & 128U) != 0U ? LX3_BUTTON_LFA : (raw & 15U);
  const bool valid = ((key <= 4U) || (key == LX3_BUTTON_MAIN) || (key == LX3_BUTTON_LFA)) &&
                     !(((raw & 128U) != 0U) && ((raw & 15U) != 0U));
  const bool continuous = s->seen && ((now - s->last_us) <= LX3_BUTTON_TIMEOUT_US) &&
                           ((uint8_t)(counter - s->counter) == 2U);
  if (!continuous || !valid) lx3_buttons_invalidate(s);
  s->seen = true;
  s->last_us = now;
  s->counter = counter;
  s->sample++;
  if (!valid) return;
  if (key == LX3_BUTTON_CANCEL) lx3_buttons_event(s, key, true, counter, 0U);
  if (!s->ready) {
    s->neutral = key == 0U ? s->neutral + 1U : 0U;
    s->ready = s->neutral >= 3U;
    s->raw_key = key;
    return;
  }
  if ((key != 0U) && (s->raw_key != 0U) && (key != s->raw_key)) {
    lx3_buttons_invalidate(s);
    if (key == LX3_BUTTON_CANCEL) lx3_buttons_event(s, key, true, counter, 0U);
    s->raw_key = key;
    return;
  }
  s->raw_key = key;

  if (key == LX3_BUTTON_MAIN) {
    if (!s->main_held) {
      s->main_held = true;
      s->main_press_us = now;
      s->main_press_sample = s->sample;
      lx3_buttons_event(s, key, true, counter, 0U);
    }
    s->main_last_us = now;
    s->main_last_sample = s->sample;
    s->main_neutral = false;
  } else if (s->main_held) {
    if (!s->main_neutral) {
      s->main_release_counter = counter;
      s->main_neutral = true;
    }
    if ((s->sample - s->main_last_sample) >= 8U) {
      lx3_buttons_event(s, LX3_BUTTON_MAIN, false, s->main_release_counter,
                        (s->main_last_sample - s->main_press_sample) * LX3_BUTTON_WIRE_PERIOD_US);
      s->main_held = false;
    }
  }

  const uint8_t other = key == LX3_BUTTON_MAIN ? 0U : key;
  if (other != s->held) {
    if ((s->held != 0U) && (other != 0U)) {
      lx3_buttons_invalidate(s);
      return;
    }
    if ((s->held != 0U) && (other == 0U)) {
      lx3_buttons_event(s, s->held, false, counter, (s->sample - s->press_sample) * LX3_BUTTON_WIRE_PERIOD_US);
    }
    if ((other != 0U) && (s->held == 0U)) {
      if (other != LX3_BUTTON_CANCEL) lx3_buttons_event(s, other, true, counter, 0U);
      s->press_us = now;
      s->press_sample = s->sample;
    }
    s->held = other;
  }
}
