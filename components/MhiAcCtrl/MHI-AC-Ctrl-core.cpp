// MHI-AC-Ctrl-core
// Implements the core functions (read & write SPI)
// Corrected for: SPI Mode 3 (CPOL=1, CPHA=1), LSB-first, 20-byte framing (SB0..SB2, DB0..DB14, CBH/CBL)
// Key fixes vs your version:
//  - Removes non-spec "doubleframe/DB14 toggling"
//  - Uses micros()-based edge timeouts (not ms per bit)
//  - No yield/delay inside bit edge loops (prevents missed edges)
//  - Detects frame start by "SCK idle-high gap" then syncs on first falling edge
//  - Uses fast ESP-IDF GPIO level/set calls (much faster than digitalRead/digitalWrite)

#include "MHI-AC-Ctrl-core.h"
#include "esphome/core/log.h"

#include "driver/gpio.h"
#include "esp_rom_sys.h"  // esp_rom_delay_us
#include "soc/gpio_struct.h"
#include "soc/gpio_reg.h"


namespace esphome {
namespace mhi {

static const char *TAG_CORE = "mhi.core";



// ----------------------------
// GPIO fast helpers
// ----------------------------
static inline int pin_read(int pin) {
  if (pin < 32) return (GPIO.in >> pin) & 1;
  return (GPIO.in1.data >> (pin - 32)) & 1;
}
static inline void pin_write(int pin, int level) {
  if (pin < 32) {
    if (level) GPIO.out_w1ts = (1UL << pin);
    else       GPIO.out_w1tc = (1UL << pin);
  } else {
    if (level) GPIO.out1_w1ts.data = (1UL << (pin - 32));
    else       GPIO.out1_w1tc.data = (1UL << (pin - 32));
  }
}

// ----------------------------
// Debug
// ----------------------------
static constexpr bool MHI_DEBUG = true;   // set false to silence
static constexpr uint32_t DBG_RATE_MS = 2000; // print at most every 2s

static inline bool dbg_ok() {
  static uint32_t last = 0;
  uint32_t now = millis();
  if (now - last >= DBG_RATE_MS) { last = now; return true; }
  return false;
}
#define DBG_LOGI(...) do { if (MHI_DEBUG && dbg_ok()) ESP_LOGI(TAG_CORE, __VA_ARGS__); } while(0)
#define DBG_LOGW(...) do { if (MHI_DEBUG && dbg_ok()) ESP_LOGW(TAG_CORE, __VA_ARGS__); } while(0)

static inline void dbg_xfer_fail(const char *msg, int sck_pin, int mosi_pin,
                                int byte_cnt, int bit_cnt) {
  static uint32_t last = 0;
  uint32_t now = millis();
  if (now - last < 250) return; // at most 4/sec
  last = now;
  ESP_LOGW(TAG_CORE, "xfer_fail: %s at byte=%d bit=%d SCK=%d MOSI=%d",
           msg, byte_cnt, bit_cnt, pin_read(sck_pin), pin_read(mosi_pin));
}

// ----------------------------
// Timing (tuned for ~31us/bit, but tolerant)
// ----------------------------
// Minimum "idle-high" time to consider a new frame boundary.
// Spec examples show long gaps (tens of ms) between frames. 
static constexpr uint32_t FRAME_GAP_US  = 20; // quiet time indicating frame boundary (tune 800..5000)

// Edge timeouts:
// Allow generous margins but still "microsecond scale" so you don't drift into next bytes/frames.
static constexpr uint32_t EDGE_TIMEOUT_US = 2000;   // max time waiting for a single edge
static constexpr uint32_t SYNC_TIMEOUT_US = 80000;     // initial sync up to 80ms
static constexpr uint32_t FRAME_TIMEOUT_US = 50000; // total time allowed to receive a frame (~10ms active + pauses)

// Optional: small spin delay while waiting for edges (keeps CPU from 100% busy-waiting)
static constexpr uint32_t SPIN_DELAY_US = 0;

// ----------------------------
// Checksums
// ----------------------------
uint16_t calc_checksum(byte *frame) {
  // sum(SB0..SB2) + sum(DB0..DB14) == bytes [0..17]
  uint16_t checksum = 0;
  for (int i = 0; i < CBH; i++)
    checksum += frame[i];
  return checksum;
}

uint16_t calc_checksumFrame33(byte *frame) {
  uint16_t checksum = 0;
  for (int i = 0; i < CBL2; i++)
    checksum += frame[i];
  return checksum;
}

// ----------------------------
// Internal helper: wait for pin level with timeout (micros)
// ----------------------------
static inline bool wait_level(int pin, int desired_level, uint32_t timeout_us) {
  const uint32_t t0 = micros();
  while ((uint32_t) (micros() - t0) < timeout_us) {
    if (pin_read(pin) == desired_level)
      return true;
    if (SPIN_DELAY_US)
      esp_rom_delay_us(SPIN_DELAY_US);
  }
  return false;
}



// Wait for an edge:
// For Mode 3, clock idles high; each bit begins with falling edge then rising edge.
static inline bool wait_falling_edge(int sck_pin, uint32_t timeout_us) {
  // Expect current high, wait for low.
  return wait_level(sck_pin, 0, timeout_us);
}
// Expect current low, wait for high.
return wait_level(sck_pin, 1, timeout_us);
}


// ----------------------------
// Capture MOSI bytes helper (unused in final code, but kept for reference)
// ----------------------------
static bool capture_mosi_bytes(
    int sck_pin, int mosi_pin,
    bool lsb_first, bool sample_on_rising,
    uint8_t *out, int nbytes, uint32_t max_us) {

  const uint32_t t0 = micros();
  for (int i = 0; i < nbytes; i++) {
    uint8_t b = 0;
    for (int bit = 0; bit < 8; bit++) {
      // wait falling
      if (!wait_falling_edge(sck_pin, EDGE_TIMEOUT_US)) return false;
      int mosi_fall = pin_read(mosi_pin);

      // wait rising
      if (!wait_rising_edge(sck_pin, EDGE_TIMEOUT_US)) return false;
      int mosi_rise = pin_read(mosi_pin);

      int sample = sample_on_rising ? mosi_rise : mosi_fall;

      if (lsb_first) {
        if (sample) b |= (1u << bit);
      } else {
        if (sample) b |= (1u << (7 - bit));
      }

      if ((uint32_t)(micros() - t0) > max_us) return false;
    }
    out[i] = b;
  }
  return true;
}

static void analyze_headers(const uint8_t *buf, int n) {
  // Find most frequent 2-byte and 3-byte sequences in buf
  uint32_t best2_cnt = 0, best3_cnt = 0;
  int best2_i = 0, best3_i = 0;

  for (int i = 0; i < n - 1; i++) {
    uint32_t cnt2 = 0;
    for (int j = 0; j < n - 1; j++) {
      if (buf[j] == buf[i] && buf[j+1] == buf[i+1]) cnt2++;
    }
    if (cnt2 > best2_cnt) { best2_cnt = cnt2; best2_i = i; }
  }

  for (int i = 0; i < n - 2; i++) {
    uint32_t cnt3 = 0;
    for (int j = 0; j < n - 2; j++) {
      if (buf[j] == buf[i] && buf[j+1] == buf[i+1] && buf[j+2] == buf[i+2]) cnt3++;
    }
    if (cnt3 > best3_cnt) { best3_cnt = cnt3; best3_i = i; }
  }

  ESP_LOGI(TAG_CORE, "Header guess 2B: %02X %02X  count=%u",
           buf[best2_i], buf[best2_i+1], (unsigned)best2_cnt);
  ESP_LOGI(TAG_CORE, "Header guess 3B: %02X %02X %02X  count=%u",
           buf[best3_i], buf[best3_i+1], buf[best3_i+2], (unsigned)best3_cnt);
}

// ----------------------------
// end of testing block
// ----------------------------


static inline bool wait_rising_edge(int sck_pin, uint32_t timeout_us) {
// Detect a new frame boundary:
// Require SCK to stay high continuously for FRAME_GAP_US .
// New helper: wait for "no edges" gap on SCK, then first falling edge
static bool wait_frame_start(int sck_pin, int mosi_pin, uint32_t max_wait_us) {
  const uint32_t t0 = micros();

  // sliding window of last 3 bytes seen on MOSI
  uint8_t w0 = 0, w1 = 0, w2 = 0;

  // helper: read one MOSI byte (Mode3, LSB-first) while ignoring MISO
  auto read_mosi_byte = [&](uint8_t &out) -> bool {
    uint8_t b = 0;
    uint8_t mask = 1;

    for (int bit = 0; bit < 8; bit++) {
      // wait falling (enter low phase)
      if (!wait_falling_edge(sck_pin, EDGE_TIMEOUT_US)) return false;
      // wait rising (sample MOSI on rising for CPHA=1 with CPOL=1)
      if (!wait_rising_edge(sck_pin, EDGE_TIMEOUT_US)) return false;

      if (pin_read(mosi_pin)) b |= mask;
      mask <<= 1;
    }
    out = b;
    return true;
  };

  while ((uint32_t)(micros() - t0) < max_wait_us) {
    uint8_t b;
    if (!read_mosi_byte(b)) {
      // If clock paused, keep looping until max_wait_us.
      continue;
    }

    // slide window
    w0 = w1; w1 = w2; w2 = b;

    // Check signature: 6C/6D 80 04
    if ((w0 == 0x6C || w0 == 0x6D) && w1 == 0x80 && w2 == 0x04) {
      if (MHI_DEBUG && dbg_ok()) {
        ESP_LOGI(TAG_CORE, "frame_start(sig): aligned on %02X %02X %02X", w0, w1, w2);
      }
      return true; // next byte read will be DB0
    }
  }

  if (MHI_DEBUG && dbg_ok()) {
    ESP_LOGW(TAG_CORE, "frame_start(sig): TIMEOUT waiting for signature");
  }
  return false;
}



// ----------------------------
// Bit-banged SPI transfer: Mode 3, LSB-first, edge-driven
// ----------------------------
static bool spi_transfer_frame_mode3_lsb(
    int sck_pin, int mosi_pin, int miso_pin,
    const uint8_t *tx, uint8_t *rx,
    uint8_t frame_len_bytes) {

  const uint32_t t_frame0 = micros();

  // Sync to first falling edge
  // if (!wait_falling_edge(sck_pin, EDGE_TIMEOUT_US)) {
  //   if (MHI_DEBUG && dbg_ok()) {
  //     ESP_LOGW(TAG_CORE, "xfer: timeout waiting initial FALL. SCK=%d MOSI=%d",
  //              pin_read(sck_pin), pin_read(mosi_pin));
  //   }
  //   return false;
  // }


  for (uint8_t byte_cnt = 0; byte_cnt < frame_len_bytes; byte_cnt++) {
    uint8_t in_byte = 0;
    uint8_t mask = 1;

    for (uint8_t bit_cnt = 0; bit_cnt < 8; bit_cnt++) {

      // Drive MISO while SCK low
      pin_write(miso_pin, (tx[byte_cnt] & mask) ? 1 : 0);

      // Wait for rising edge, then sample MOSI
      // if (!wait_rising_edge(sck_pin, EDGE_TIMEOUT_US)) {
      //   if (MHI_DEBUG && dbg_ok()) {
      //     ESP_LOGW(TAG_CORE, "xfer: timeout RISE at byte=%u bit=%u SCK=%d MOSI=%d",
      //              byte_cnt, bit_cnt, pin_read(sck_pin), pin_read(mosi_pin));
      //   }
      //   return false;
      // }
      if (!wait_rising_edge(sck_pin, EDGE_TIMEOUT_US)) { dbg_xfer_fail("RISE timeout", sck_pin, mosi_pin, byte_cnt, bit_cnt); return false; }
      if (pin_read(mosi_pin)) in_byte |= mask;

      // Wait for falling edge to start next bit
      // if (!wait_falling_edge(sck_pin, EDGE_TIMEOUT_US)) {
      //   if (MHI_DEBUG && dbg_ok()) {
      //     ESP_LOGW(TAG_CORE, "xfer: timeout FALL at byte=%u bit=%u SCK=%d MOSI=%d",
      //              byte_cnt, bit_cnt, pin_read(sck_pin), pin_read(mosi_pin));
      //   }
      //   return false;
      // }
      if (!wait_falling_edge(sck_pin, EDGE_TIMEOUT_US)) { dbg_xfer_fail("FALL timeout", sck_pin, mosi_pin, byte_cnt, bit_cnt); return false; }

      mask <<= 1;

      // total frame timeout
      // if ((uint32_t)(micros() - t_frame0) > FRAME_TIMEOUT_US) {
      //   if (MHI_DEBUG && dbg_ok()) {
      //     ESP_LOGW(TAG_CORE, "xfer: frame timeout at byte=%u bit=%u elapsed_us=%u",
      //              byte_cnt, bit_cnt, (unsigned)(micros() - t_frame0));
      //   }
      //   return false;
      // }
      if ((uint32_t)(micros() - t_frame0) > FRAME_TIMEOUT_US) { dbg_xfer_fail("FRAME timeout", sck_pin, mosi_pin, byte_cnt, bit_cnt); return false; }
    }
    
    rx[byte_cnt] = in_byte;
  }

  pin_write(miso_pin, 0);
  return true;
}


// ------------------------------------------------------------------
// Existing code (state)
// ------------------------------------------------------------------
void MHI_AC_Ctrl_Core::reset_old_values() {
  status_power_old = 0xff;
  status_mode_old = 0xff;
  status_fan_old = 0xff;
  status_vanes_old = 0xff;
  status_troom_old = 0xfe;
  status_tsetpoint_old = 0x00;
  status_errorcode_old = 0xff;
  status_vanesLR_old = 0xff;
  status_3Dauto_old = 0xff;

  op_kwh_old = 0xffff;
  op_mode_old = 0xff;
  op_settemp_old = 0xff;
  op_return_air_old = 0xff;
  op_iu_fanspeed_old = 0xff;
  op_thi_r1_old = 0x00;
  op_thi_r2_old = 0x00;
  op_thi_r3_old = 0x00;
  op_total_iu_run_old = 0;
  op_outdoor_old = 0xff;
  op_tho_r1_old = 0x00;
  op_total_comp_run_old = 0;
  op_ct_old = 0xff;
  op_tdsh_old = 0xff;
  op_protection_no_old = 0xff;
  op_ou_fanspeed_old = 0xff;
  op_defrost_old = 0x00;
  op_comp_old = 0xffff;
  op_td_old = 0x00;
  op_ou_eev1_old = 0xffff;
}

void MHI_AC_Ctrl_Core::init() {
  ESP_LOGI(TAG_CORE, "MHI AC Ctrl Core init");

  // Mode 3 bus: AC is master -> we must not drive SCK/MOSI, only read them.
  gpio_set_direction((gpio_num_t) SCK_PIN, GPIO_MODE_INPUT);
  gpio_set_pull_mode((gpio_num_t) SCK_PIN, GPIO_PULLUP_ONLY);

  gpio_set_direction((gpio_num_t) MOSI_PIN, GPIO_MODE_INPUT);
  gpio_set_pull_mode((gpio_num_t) MOSI_PIN, GPIO_PULLUP_ONLY);

  // Our reply line to AC
  gpio_set_direction((gpio_num_t) MISO_PIN, GPIO_MODE_OUTPUT);
  pin_write(MISO_PIN, 0);

  ESP_LOGI(TAG_CORE, "Initialized: SCK=%d (in) MOSI=%d (in) MISO=%d (out)", SCK_PIN, MOSI_PIN, MISO_PIN);

  reset_old_values();
}

void MHI_AC_Ctrl_Core::set_power(boolean power) { new_Power = 0b10 | power; }
void MHI_AC_Ctrl_Core::set_mode(ACMode mode) { new_Mode = 0b00100000 | mode; }
void MHI_AC_Ctrl_Core::set_tsetpoint(uint tsetpoint) { new_Tsetpoint = 0b10000000 | tsetpoint; }
void MHI_AC_Ctrl_Core::set_fan(uint fan) { new_Fan = 0b00001000 | fan; }
void MHI_AC_Ctrl_Core::set_3Dauto(AC3Dauto Dauto) { new_3Dauto = 0b00001010 | Dauto; }

void MHI_AC_Ctrl_Core::set_vanes(uint vanes) {
  if (vanes == vanes_swing) {
    new_Vanes0 = 0b11000000; // enable swing
  } else {
    new_Vanes0 = 0b10000000; // disable swing
    new_Vanes1 = 0b10000000 | ((vanes - 1) << 4);
  }
}

void MHI_AC_Ctrl_Core::set_vanesLR(uint vanesLR) {
  if (vanesLR == vanesLR_swing) {
    new_VanesLR0 = 0b00001011;
  } else {
    new_VanesLR0 = 0b00001010;
    new_VanesLR1 = 0b00010000 | (vanesLR - 1);
  }
}

void MHI_AC_Ctrl_Core::request_ErrOpData() { request_erropData = true; }
void MHI_AC_Ctrl_Core::set_troom(byte troom) { new_Troom = troom; }
float MHI_AC_Ctrl_Core::get_troom_offset() { return Troom_offset; }
void MHI_AC_Ctrl_Core::set_troom_offset(float offset) { Troom_offset = offset; }

void MHI_AC_Ctrl_Core::set_frame_size(byte framesize) {
  if (framesize == 20 || framesize == 33) frameSize = framesize;
}

// ------------------------------------------------------------------
// Corrected loop()
// ------------------------------------------------------------------
int MHI_AC_Ctrl_Core::loop(uint max_time_ms) {
  const byte opdataCnt = sizeof(opdata) / sizeof(byte) / 2;
  static byte opdataNo = 0;
  static byte erropdataCnt = 0;

  static uint32_t call_counter = 0;
  static unsigned long lastTroomInternalMillis = 0;

  call_counter++;

  // One-time dump for analysis
  static bool did_dump = false;
  if (!did_dump) {
    did_dump = true;

    static uint8_t cap[400];

    // Start with your current assumption (LSB + sample rising).
    bool ok = capture_mosi_bytes(SCK_PIN, MOSI_PIN,
                                true /*lsb_first*/, false /*sample_on_falling*/,
                                cap, (int)sizeof(cap), 300000 /*300ms*/);

    if (!ok) {
      ESP_LOGW(TAG_CORE, "capture_mosi_bytes failed");
      return err_msg_timeout_SCK_high;
    }

    // Print first 64 bytes
    char line[3*64 + 1];
    line[0] = 0;
    for (int i = 0; i < 64; i++) {
      char tmp[4];
      snprintf(tmp, sizeof(tmp), "%02X ", cap[i]);
      strncat(line, tmp, sizeof(line)-strlen(line)-1);
    }
    ESP_LOGI(TAG_CORE, "MOSI first 64 bytes: %s", line);

    analyze_headers(cap, (int)sizeof(cap));
    return 0;
  }
  // ---------------------------- 



  // 20-byte MOSI frame buffer (supports up to 33 if you use WF-RAC)
  static byte MOSI_frame[33];

  // Default outgoing MISO frame template (33 bytes allocated, but frameSize selects how much is sent)
  // IMPORTANT: For 20-byte frames, bytes [18..19] are checksum (CBH/CBL).
  static byte MISO_frame[] = {
    // SB0  SB1  SB2
    0xA9, 0x00, 0x07,
    // DB0..DB14 (15 bytes)
    0x00, 0x00, 0x00, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x00,
    // CBH, CBL (filled each frame)
    0x00, 0x00,
    // DB15.. etc for 33-byte variant (kept from your code)
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x22
  };

  // If 33-byte mode, SB0 differs
  if (frameSize == 33)
    MISO_frame[0] = 0xAA;

  // ----------------------------
  // Wait for next frame boundary (SCK idle-high gap)
  // Do NOT wait in ms per bit; wait for the frame gap.
  // ----------------------------
  const uint32_t max_wait_us = (uint32_t) max_time_ms * 1000UL;
  if (!wait_frame_start(SCK_PIN, MOSI_PIN, max_wait_us)) {
    return err_msg_timeout_SCK_high;
  }


  // ----------------------------
  // Build next MISO frame according to spec:
  // - No undocumented per-frame toggles
  // - Apply set-bits as you already do (DB0[1], DB0[5], DB2[7], DB1[3], DB0[7]/DB1[7], etc.)
  // - Opdata/error-opdata request logic retained, but not tied to "doubleframe"
  // ----------------------------

  // OpData request pacing:
  // Spec examples show ~20–25 frames/s; do opdata scheduling on frame count.
  // Preserve your 20s-ish cycle behavior, but make it frame-count based.
  // A "cycle" is ~20 seconds. If ~25Hz, 20s ~ 500 frames. Keep NoFramesPerOpDataCycle if you have it defined.
  static uint16_t frame_counter = 0;
  frame_counter++;

  // Request new opdata periodically; do it for 2 frames as your code did.
  // If NoFramesPerOpDataCycle is defined in your headers, use it; otherwise default.
#ifndef NoFramesPerOpDataCycle
  static constexpr uint16_t NoFramesPerOpDataCycle = 500; // ~20s @25Hz
#endif

  const uint16_t interval = (opdataCnt > 0) ? (NoFramesPerOpDataCycle / opdataCnt) : NoFramesPerOpDataCycle;
  const bool opdata_window = (interval > 0) ? ((frame_counter % interval) < 2) : false;

  // Reset opdata request fields by default
  MISO_frame[DB6] = 0x80;
  MISO_frame[DB9] = 0xff;

  if (opdata_window) {
    if (erropdataCnt == 0) {
      // Request next opdata
      MISO_frame[DB6] = pgm_read_word(opdata + opdataNo);
      MISO_frame[DB9] = pgm_read_word(opdata + opdataNo) >> 8;
      opdataNo = (opdataNo + 1) % opdataCnt;
    }
  }

  // Error opdata request/continuation (unchanged behavior)
  if (erropdataCnt > 0) {
    MISO_frame[DB6] = 0x80;
    MISO_frame[DB9] = 0xff;
    erropdataCnt--;
  }

  // Apply control bits (your existing pattern)
  MISO_frame[DB0] = 0x00;
  MISO_frame[DB1] = 0x00;
  MISO_frame[DB2] = 0x00;

  // Power
  MISO_frame[DB0] = new_Power;
  new_Power = 0;

  // Mode
  MISO_frame[DB0] |= new_Mode;
  new_Mode = 0;

  // Setpoint
  MISO_frame[DB2] = new_Tsetpoint;
  new_Tsetpoint = 0;

  // Fan
  MISO_frame[DB1] = new_Fan;
  new_Fan = 0;

  // Vanes U/D
  MISO_frame[DB0] |= new_Vanes0;
  MISO_frame[DB1] |= new_Vanes1;
  new_Vanes0 = 0;
  new_Vanes1 = 0;

  // Error opdata request command
  if (request_erropData) {
    MISO_frame[DB6] = 0x80;
    MISO_frame[DB9] = 0x45;
    request_erropData = false;
  }

  // Room temperature injection (DB3)
  MISO_frame[DB3] = new_Troom;

  // Compute 20-byte checksum (bytes 0..17) and store at CBH/CBL
  uint16_t checksum = calc_checksum(MISO_frame);
  MISO_frame[CBH] = highByte(checksum);
  MISO_frame[CBL] = lowByte(checksum);

  // 33-byte extension (unchanged structure, but note: this is a different frame family)
  if (frameSize == 33) {
    MISO_frame[DB16] = 0;
    MISO_frame[DB16] |= new_VanesLR1;

    MISO_frame[DB17] = 0;
    MISO_frame[DB17] |= new_VanesLR0;
    MISO_frame[DB17] |= new_3Dauto;

    new_3Dauto = 0;
    new_VanesLR0 = 0;
    new_VanesLR1 = 0;

    checksum = calc_checksumFrame33(MISO_frame);
    MISO_frame[CBL2] = lowByte(checksum);
  }

  // ----------------------------
  // Transfer frame: Mode 3, LSB-first
  // ----------------------------
  bool ok = spi_transfer_frame_mode3_lsb(SCK_PIN, MOSI_PIN, MISO_PIN, MISO_frame, MOSI_frame, frameSize);
  if (!ok) {
    // Allow WiFi/RTOS after a failed attempt (we are between frames now)
    delay(0);
    return err_msg_timeout_SCK_high; // or define a distinct err for "xfer failed"
  }

  // After transfer, we are between frames; safe to yield
  delay(0);

  // ----------------------------
  // Validate signature + checksum
  // ----------------------------
  // MOSI signature bytes: SB0 is 0x6c or 0x6d; SB1=0x80; SB2=0x04
  const uint8_t sb0 = MOSI_frame[SB0];
  const uint8_t sb1 = MOSI_frame[SB1];
  const uint8_t sb2 = MOSI_frame[SB2];

  if (!((sb0 == 0x6c) || (sb0 == 0x6d)) || (sb1 != 0x80) || (sb2 != 0x04)) {
    ESP_LOGW(TAG_CORE, "Invalid signature: SB0=0x%02X SB1=0x%02X SB2=0x%02X", sb0, sb1, sb2);
    return err_msg_invalid_signature;
  }

  checksum = calc_checksum(MOSI_frame);
  const uint16_t rx_ck = ((uint16_t) MOSI_frame[CBH] << 8) | MOSI_frame[CBL];
  if (rx_ck != checksum) {
    ESP_LOGW(TAG_CORE, "Invalid checksum: got=0x%04X calc=0x%04X", rx_ck, checksum);
    return err_msg_invalid_checksum;
  }

  if (frameSize == 33) {
    checksum = calc_checksumFrame33(MOSI_frame);
    const uint8_t expected = lowByte(checksum);
    const uint8_t received = MOSI_frame[CBL2];
    if (received != expected) {
      ESP_LOGW(TAG_CORE, "Invalid Frame33 checksum: got=0x%02X exp=0x%02X", received, expected);
      return err_msg_invalid_checksum;
    }
  }

  // ----------------------------
  // Determine if anything changed (your existing logic)
  // ----------------------------
  static byte MOSI_prev[33] = {0};
  bool new_datapacket_received = false;
  for (uint8_t i = 0; i < frameSize; i++) {
    if (MOSI_prev[i] != MOSI_frame[i]) {
      new_datapacket_received = true;
      MOSI_prev[i] = MOSI_frame[i];
    }
  }

  if (!new_datapacket_received) {
    return (int) call_counter;
  }

  // ----------------------------
  // Your existing decode logic follows (kept as-is except no dependency on "doubleframe")
  // ----------------------------

  // 33-byte extras
  if (frameSize == 33) {
    byte vanesLRtmp = (MOSI_frame[DB16] & 0x07) + ((MOSI_frame[DB17] & 0x01) << 4);
    if (vanesLRtmp != status_vanesLR_old) {
      if ((vanesLRtmp & 0x10) != 0)
        m_cbiStatus->cbiStatusFunction(status_vanesLR, vanesLR_swing);
      else
        m_cbiStatus->cbiStatusFunction(status_vanesLR, (vanesLRtmp & 0x07) + 1);
      status_vanesLR_old = vanesLRtmp;
    }

    if ((MOSI_frame[DB17] & 0x04) != status_3Dauto_old) {
      status_3Dauto_old = MOSI_frame[DB17] & 0x04;
      m_cbiStatus->cbiStatusFunction(status_3Dauto, status_3Dauto_old);
    }
  }

  // Mode
  if ((MOSI_frame[DB0] & 0x1c) != status_mode_old) {
    status_mode_old = MOSI_frame[DB0] & 0x1c;
    m_cbiStatus->cbiStatusFunction(status_mode, status_mode_old);
  }

  // Power
  if ((MOSI_frame[DB0] & 0x01) != status_power_old) {
    status_power_old = MOSI_frame[DB0] & 0x01;
    m_cbiStatus->cbiStatusFunction(status_power, status_power_old);
  }

  // Fan
  uint fantmp = MOSI_frame[DB1] & 0x07;
  if (fantmp != status_fan_old) {
    status_fan_old = fantmp;
    m_cbiStatus->cbiStatusFunction(status_fan, status_fan_old);
  }

  // Vanes
  uint vanestmp = (MOSI_frame[DB0] & 0xc0) + ((MOSI_frame[DB1] & 0xB0) >> 4);
  if (vanestmp != status_vanes_old) {
    if ((vanestmp & 0x40) != 0)
      m_cbiStatus->cbiStatusFunction(status_vanes, vanes_swing);
    else
      m_cbiStatus->cbiStatusFunction(status_vanes, (vanestmp & 0x03) + 1);
    status_vanes_old = vanestmp;
  }

  // Room temp publish throttling (unchanged)
  if (MOSI_frame[DB3] != status_troom_old) {
    if (MISO_frame[DB3] != 0xff) {
      status_troom_old = MOSI_frame[DB3];
      m_cbiStatus->cbiStatusFunction(status_troom, status_troom_old);
      lastTroomInternalMillis = 0;
    } else {
      if ((unsigned long) (millis() - lastTroomInternalMillis) > minTimeInternalTroom) {
        lastTroomInternalMillis = millis();
        status_troom_old = MOSI_frame[DB3];
        m_cbiStatus->cbiStatusFunction(status_troom, status_troom_old);
      }
    }
  }

  // Setpoint
  if (MOSI_frame[DB2] != status_tsetpoint_old) {
    status_tsetpoint_old = MOSI_frame[DB2];
    m_cbiStatus->cbiStatusFunction(status_tsetpoint, status_tsetpoint_old);
  }

  // Error code
  if (MOSI_frame[DB4] != status_errorcode_old) {
    status_errorcode_old = MOSI_frame[DB4];
    m_cbiStatus->cbiStatusFunction(status_errorcode, status_errorcode_old);
  }

  // Operating / error operating data decode (kept exactly as you had it)
  bool MOSI_type_opdata = (MOSI_frame[DB10] & 0x30) == 0x10;

  switch (MOSI_frame[DB9]) {
    case 0x94:
      if ((MOSI_frame[DB6] & 0x80) != 0) {
        if (MOSI_type_opdata) {
          if (((MOSI_frame[DB12] << 8) + (MOSI_frame[DB11])) != op_kwh_old) {
            op_kwh_old = (MOSI_frame[DB12] << 8) + (MOSI_frame[DB11]);
            m_cbiStatus->cbiStatusFunction(opdata_kwh, op_kwh_old);
          }
        }
      }
      break;

    case 0x02:
      if ((MOSI_frame[DB6] & 0x80) != 0) {
        if (MOSI_type_opdata) {
          if ((MOSI_frame[DB10] != op_mode_old)) {
            op_mode_old = MOSI_frame[DB10];
            m_cbiStatus->cbiStatusFunction(opdata_mode, (op_mode_old & 0x0f) << 2);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_mode, (MOSI_frame[DB10] & 0x0f) << 2);
        }
      }
      break;

    case 0x05:
      if ((MOSI_frame[DB6] & 0x80) != 0) {
        if (MOSI_frame[DB10] == 0x13) {
          if (MOSI_frame[DB11] != op_settemp_old) {
            op_settemp_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_tsetpoint, op_settemp_old);
          }
        } else if (MOSI_frame[DB10] == 0x33) {
          m_cbiStatus->cbiStatusFunction(erropdata_tsetpoint, MOSI_frame[DB11]);
        }
      }
      break;

    case 0x81:
      if ((MOSI_frame[DB6] & 0x80) != 0) {
        if ((MOSI_frame[DB10] & 0x30) == 0x20) {
          if (MOSI_frame[DB11] != op_thi_r1_old) {
            op_thi_r1_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_thi_r1, op_thi_r1_old);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_thi_r1, MOSI_frame[DB11]);
        }
      } else {
        if (MOSI_type_opdata) {
          if (MOSI_frame[DB11] != op_thi_r2_old) {
            op_thi_r2_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_thi_r2, op_thi_r2_old);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_thi_r2, MOSI_frame[DB11]);
        }
      }
      break;

    case 0x87:
      if ((MOSI_frame[DB6] & 0x80) != 0) {
        if (MOSI_type_opdata) {
          if (MOSI_frame[DB11] != op_thi_r3_old) {
            op_thi_r3_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_thi_r3, op_thi_r3_old);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_thi_r3, MOSI_frame[DB11]);
        }
      }
      break;

    case 0x80:
      if ((MOSI_frame[DB6] & 0x80) != 0) {
        if ((MOSI_frame[DB10] & 0x30) == 0x20) {
          if (MOSI_frame[DB11] != op_return_air_old) {
            op_return_air_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_return_air, op_return_air_old);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_return_air, MOSI_frame[DB11]);
        }
      } else {
        if (MOSI_type_opdata) {
          if (MOSI_frame[DB11] != op_outdoor_old) {
            op_outdoor_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_outdoor, op_outdoor_old);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_outdoor, MOSI_frame[DB11]);
        }
      }
      break;

    case 0x1f:
      if ((MOSI_frame[DB6] & 0x80) != 0) {
        if (MOSI_type_opdata) {
          if (MOSI_frame[DB10] != op_iu_fanspeed_old) {
            op_iu_fanspeed_old = MOSI_frame[DB10];
            m_cbiStatus->cbiStatusFunction(opdata_iu_fanspeed, op_iu_fanspeed_old & 0x0f);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_iu_fanspeed, MOSI_frame[DB10] & 0x0f);
        }
      } else {
        if (MOSI_type_opdata) {
          if (MOSI_frame[DB10] != op_ou_fanspeed_old) {
            op_ou_fanspeed_old = MOSI_frame[DB10];
            m_cbiStatus->cbiStatusFunction(opdata_ou_fanspeed, op_ou_fanspeed_old & 0x0f);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_ou_fanspeed, MOSI_frame[DB10] & 0x0f);
        }
      }
      break;

    case 0x1e:
      if ((MOSI_frame[DB6] & 0x80) != 0) {
        if (MOSI_type_opdata) {
          if (MOSI_frame[DB11] != op_total_iu_run_old) {
            op_total_iu_run_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_total_iu_run, op_total_iu_run_old);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_total_iu_run, MOSI_frame[DB11]);
        }
      } else {
        if (MOSI_frame[DB10] == 0x11) {
          if (MOSI_frame[DB11] != op_total_comp_run_old) {
            op_total_comp_run_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_total_comp_run, op_total_comp_run_old);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_total_comp_run, MOSI_frame[DB11]);
        }
      }
      break;

    case 0x82:
      if ((MOSI_frame[DB6] & 0x80) == 0) {
        if (MOSI_type_opdata) {
          if (MOSI_frame[DB11] != op_tho_r1_old) {
            op_tho_r1_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_tho_r1, op_tho_r1_old);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_tho_r1, MOSI_frame[DB11]);
        }
      }
      break;

    case 0x11:
      if ((MOSI_frame[DB6] & 0x80) == 0) {
        if (MOSI_type_opdata) {
          if ((MOSI_frame[DB10] << 8 | MOSI_frame[DB11]) != op_comp_old) {
            op_comp_old = MOSI_frame[DB10] << 8 | MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_comp, op_comp_old & 0x0fff);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_comp, (MOSI_frame[DB10] << 8 | MOSI_frame[DB11]) & 0x0fff);
        }
      }
      break;

    case 0x85:
      if ((MOSI_frame[DB6] & 0x80) == 0) {
        if (MOSI_type_opdata) {
          if (MOSI_frame[DB11] != op_td_old) {
            op_td_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_td, op_td_old);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_td, MOSI_frame[DB11]);
        }
      }
      break;

    case 0x90:
      if ((MOSI_frame[DB6] & 0x80) == 0) {
        if (MOSI_type_opdata) {
          if (MOSI_frame[DB11] != op_ct_old) {
            op_ct_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_ct, op_ct_old);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_ct, MOSI_frame[DB11]);
        }
      }
      break;

    case 0xb1:
      if ((MOSI_frame[DB6] & 0x80) == 0) {
        if (MOSI_type_opdata) {
          if (MOSI_frame[DB11] != op_tdsh_old) {
            op_tdsh_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_tdsh, op_tdsh_old / 2);
          }
        }
      }
      break;

    case 0x7c:
      if ((MOSI_frame[DB6] & 0x80) == 0) {
        if (MOSI_type_opdata) {
          if (MOSI_frame[DB11] != op_protection_no_old) {
            op_protection_no_old = MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_protection_no, op_protection_no_old);
          }
        }
      }
      break;

    case 0x0c:
      if ((MOSI_frame[DB6] & 0x80) == 0) {
        if (MOSI_type_opdata) {
          if (MOSI_frame[DB10] != op_defrost_old) {
            op_defrost_old = MOSI_frame[DB10];
            m_cbiStatus->cbiStatusFunction(opdata_defrost, op_defrost_old & 0b1);
          }
        }
      }
      break;

    case 0x13:
      if ((MOSI_frame[DB6] & 0x80) == 0) {
        if (MOSI_type_opdata) {
          if ((MOSI_frame[DB12] << 8 | MOSI_frame[DB11]) != op_ou_eev1_old) {
            op_ou_eev1_old = MOSI_frame[DB12] << 8 | MOSI_frame[DB11];
            m_cbiStatus->cbiStatusFunction(opdata_ou_eev1, op_ou_eev1_old);
          }
        } else {
          m_cbiStatus->cbiStatusFunction(erropdata_ou_eev1, MOSI_frame[DB12] << 8 | MOSI_frame[DB11]);
        }
      }
      break;

    case 0x45:
      if ((MOSI_frame[DB6] & 0x80) != 0) {
        if (MOSI_frame[DB10] == 0x11) {
          m_cbiStatus->cbiStatusFunction(erropdata_errorcode, MOSI_frame[DB11]);
        } else if (MOSI_frame[DB10] == 0x12) {
          erropdataCnt = MOSI_frame[DB11] + 4;
        }
      }
      break;

    default:
      // Keep your behavior
      m_cbiStatus->cbiStatusFunction(opdata_unknown, MOSI_frame[DB10] << 8 | MOSI_frame[DB9]);
      Serial.printf("Unknown operating data, MOSI_frame[DB9]=%i MOSI_frame[D10]=%i\n",
                    MOSI_frame[DB9], MOSI_frame[DB10]);
      break;

    case 0x00:
    case 0xff:
      break;
  }

  return (int) call_counter;
}

}  // namespace mhi
}  // namespace esphome
