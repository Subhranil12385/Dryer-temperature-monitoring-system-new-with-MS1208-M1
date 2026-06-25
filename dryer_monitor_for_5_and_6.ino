/*
 * ================================================================
 *  CPIL Chennai — Dryer 5 & 6 Temperature Monitor
 *  Hardware : Waveshare ESP32-S3-ETH (W5500 SPI)
 *  Version  : 2.4  (Adafruit_NeoPixel support)
 * ================================================================
 *
 *  Features:
 *   • Ethernet via W5500 — replaces WiFi
 *   • Modbus RTU RS-485, Slave IDs 5 & 6 (8 channels each)
 *   • I2C 16×2 LCD — shows MAC address after IP address on boot
 *   • 4×4 Keypad — threshold set/fetch via Google Sheets
 *   • 2-channel dryer light-bulb relay (one per dryer) — BLINKS on alert
 *   • 1-channel alarm/horn relay — CONSTANT ON on alert
 *   • RGB LED (GPIO21, rgbLedWrite) — RED = no ETH, GREEN = ETH OK
 *   • LCD self-heal: L1 garbage detect, L2 periodic reinit, L3 I2C ping+bitbang
 *   • Auto reboot on ETH loss for > 5 min
 *   • Upload to Google Sheets every 60 s
 *   • Threshold fetch from Google Sheets every 20 s
 *   • NeoPixel rings (WS2812 on GPIO34) — shows channel status and config animations
 *
 *  GPIO ALLOCATION:
 *   W5500 ETH  : GPIO9(RST) GPIO10(INT) GPIO11(MOSI) GPIO12(MISO) GPIO13(CLK) GPIO14(CS)
 *   SD Card    : GPIO4(CS) GPIO5(MISO) GPIO6(MOSI) GPIO7(CLK)
 *   USB D+/D-  : GPIO19, GPIO20
 *   UART0      : GPIO43(TX), GPIO44(RX)
 *   RGB LED    : GPIO21
 *   LCD I2C    : GPIO2(SDA), GPIO3(SCL)
 *   RS-485     : GPIO16(DE), GPIO17(RX), GPIO18(TX)
 *   Keypad     : GPIO41,45,46,47(rows)  GPIO48,42,40,39(cols)
 *   RELAY HORN : GPIO15
 *   RELAY D5   : GPIO1
 *   RELAY D6   : GPIO38
 *   NEOPIXEL   : GPIO34
 *
 * ================================================================
 */

#include <ETH.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>
#include <ArduinoJson.h>
#include <Wire.h>
#include <LiquidCrystal_I2C.h>
#include <Keypad.h>
#include <SPI.h>
#include <esp_task_wdt.h>
#include <Adafruit_NeoPixel.h>
#include <Preferences.h>

// ── Google Sheets URL ─────────────────────────────────────────
const char* SHEET_URL = "https://script.google.com/macros/s/AKfycbxwz4Unk7eJDNHqwGUn8PwqDNoWrINFC84Djo3FL28pUU_8olAtNhqKfPFE6Epf-wZI/exec";

// ── W5500 SPI pins ────────────────────────────────────────────
#define ETH_MOSI_PIN  11
#define ETH_MISO_PIN  12
#define ETH_SCK_PIN   13
#define ETH_CS_PIN    14
#define ETH_RST_PIN    9
#define ETH_INT_PIN   10

// ── RGB LED ───────────────────────────────────────────────────
#define RGB_PIN     21
#define RGB_BRIGHT  80

// ── NeoPixel rings ────────────────────────────────────────────
#define NEO_PIN         34
#define NEO_RINGS       2
#define NEO_PER_RING    8
#define NEO_TOTAL       (NEO_RINGS * NEO_PER_RING)   // 16
#define NEO_CH_LEDS     6      // active channel LEDs per ring (channels 1-6)
#define MAX_BRIGHTNESS  128    // 50% of 255
#define NEO_REFRESH_MS  30

// ── Channel → LED index mapping (per ring) ───────────────────
// 8 LEDs per ring, numbered clockwise from data-in pad (near wire entry).
// Physical layout (clockwise from top, wires at top-right):
//   LED0=top, LED1=top-right, LED2=right, LED3=bottom-right(OFF),
//   LED4=bottom(OFF), LED5=bottom-left, LED6=left, LED7=top-left
// Channels 1-6 start at left (LED5) going clockwise, skipping bottom 2:
//   Ch1=LED5, Ch2=LED6, Ch3=LED7, Ch4=LED0, Ch5=LED1, Ch6=LED2
// LEDs 3 and 4 (bottom) are always off (spacers).
static const uint8_t CH_TO_LED[NEO_CH_LEDS] = {5, 6, 7, 0, 1, 2};
static const uint8_t NEO_SPACERS[2]         = {3, 4};  // bottom LEDs always off

static Adafruit_NeoPixel neoStrip(NEO_TOTAL, NEO_PIN, NEO_GRB + NEO_KHZ800);

// ── I2C LCD ───────────────────────────────────────────────────
#define LCD_I2C_ADDR  0x27
#define LCD_SDA_PIN    2
#define LCD_SCL_PIN    3
LiquidCrystal_I2C lcd(LCD_I2C_ADDR, 16, 2);

// ── 4×4 Keypad ───────────────────────────────────────────────
const byte KP_ROWS = 4;
const byte KP_COLS = 4;
char keys[KP_ROWS][KP_COLS] = {
  {'1','2','3','A'},
  {'4','5','6','B'},
  {'7','8','9','C'},
  {'*','0','#','D'}
};
byte rowPins[KP_ROWS] = {39, 40, 42, 48};
byte colPins[KP_COLS] = {47, 46, 45, 41};
Keypad keypad = Keypad(makeKeymap(keys), rowPins, colPins, KP_ROWS, KP_COLS);

// ── RS-485 ────────────────────────────────────────────────────
#define RS485_RX_PIN  17
#define RS485_TX_PIN  18
#define RS485_DE_PIN  16
HardwareSerial rs485Serial(1);

// ── Relay Pins ────────────────────────────────────────────────
#define RELAY_HORN        15
int RELAY_DRYER[2] = {1, 38};

// ── Dryer / Modbus Config ─────────────────────────────────────
#define NUM_DRYERS    2
#define FIRST_SLAVE   5
#define NUM_CHANNELS  8
#define TOTAL_CH      (NUM_DRYERS * NUM_CHANNELS)
#define RS485_BAUD    9600
#define START_REG     0x006E

// ── Timing ────────────────────────────────────────────────────
#define READ_INTERVAL            5000UL
#define UPLOAD_INTERVAL         60000UL
#define THRESHOLD_FETCH_INT     20000UL
#define THRESHOLD_FETCH_RETRIES     2
#define LCD_REFRESH_MS            250UL
#define LCD_ALERT_SWAP_MS        1000UL
#define ETH_FAIL_REBOOT_MS   (5UL * 60UL * 1000UL)

// ── LCD Self-Heal ─────────────────────────────────────────────
#define LCD_PING_INTERVAL_MS    30000UL
#define LCD_REINIT_INTERVAL_MS  5000UL

// ── Blink periods ─────────────────────────────────────────────
#define BLINK_FAST_MS    500UL
#define BLINK_SLOW_MS   2000UL

// =============================================================
//  SYSTEM READY FLAG
// =============================================================
volatile bool systemReady = false;

// =============================================================
//  MUTEXES
// =============================================================
SemaphoreHandle_t lcdBufMutex;
SemaphoreHandle_t lcdHwMutex;
SemaphoreHandle_t threshMutex;
SemaphoreHandle_t alertMutex;

// =============================================================
//  LCD DISPLAY BUFFER
// =============================================================
char lcdBuf[2][17];
char          statusMsgLine0[17] = "";
char          statusMsgLine1[17] = "";
unsigned long statusExpiry  = 0;

volatile int zeroPressCount = 0;

// =============================================================
//  LCD SELF-HEAL STATE
// =============================================================
static unsigned long lcdLastPingCheck    = 0;
static unsigned long lcdLastForcedReinit = 0;
static bool          lcdHealthy          = true;
static volatile bool lcdNeedsHeal        = false;

// =============================================================
//  SHARED DATA
// =============================================================
float temperatures[NUM_DRYERS][NUM_CHANNELS];
bool  channelValid[NUM_DRYERS][NUM_CHANNELS];
float thresholds[NUM_DRYERS][NUM_CHANNELS];
float lowerThresholds[NUM_DRYERS][NUM_CHANNELS];
bool  hasValidReading[NUM_DRYERS];
volatile bool dryerAlertHigh[NUM_DRYERS];
volatile bool dryerAlertLow[NUM_DRYERS];

struct AlertDisplay {
  int   dryer;
  int   chamber;
  float sv;
  float pv;
  bool  isHigh;
};
AlertDisplay lcdAlertList[TOTAL_CH * 2];
volatile int lcdAlertCount = 0;
volatile bool keypadActive = false;

// Home screen channel selection (keys 1–6)
volatile int homeDisplayChannel = 1;

// Info display: B = IP 5 s, D = MAC 5 s
enum InfoMode { INFO_NONE, INFO_IP, INFO_MAC };
volatile InfoMode     infoMode   = INFO_NONE;
volatile unsigned long infoExpiry = 0;

struct ThreshSaveRequest {
  volatile int   dryer;
  volatile int   chamber;
  volatile float value;
  volatile bool  isLower;
  volatile bool  pending;
};
volatile ThreshSaveRequest pendingSave     = {0, 0, 0.0f, false, false};
volatile bool              pendingFetch    = false;
volatile bool              saveResultReady = false;
volatile bool              saveResultOk    = false;

bool ethConnected  = false;
int  httpFailCount = 0;
unsigned long ethLostAt  = 0;
bool          ethWasLost = false;

unsigned long lastReadTime       = 0;
unsigned long lastUploadTime     = 0;
unsigned long lastThresholdFetch = 0;
unsigned long lastStatusFetch    = 0;
volatile int statusTimeoutSec = 120;

enum UIState {
  UI_HOME, UI_THRESH_DRYER, UI_THRESH_CHAMBER,
  UI_THRESH_TYPE, UI_THRESH_VALUE, UI_WAITING_SAVE
};
UIState uiState         = UI_HOME;
int     selectedDryer   = 0;
int     selectedChamber = 0;
bool    selectedIsLower = false;
String  inputValue      = "";

TaskHandle_t keypadTaskHandle = NULL;
TaskHandle_t relayTaskHandle  = NULL;
TaskHandle_t lcdTaskHandle    = NULL;

SPIClass ethSPI(HSPI);

// =============================================================
//  RING ASSIGNMENT HELPER
//  Dryer 5 (d=0, FIRST_SLAVE=5) → ring 1
//  Dryer 6 (d=1)                → ring 0
// =============================================================
static inline int dryerToRing(int dryerSlaveId) {
  return (dryerSlaveId == FIRST_SLAVE) ? 1 : 0;
}
static inline int dryerIdxToRing(int dIdx) {
  return (dIdx == 0) ? 1 : 0;
}

// =============================================================
//  RGB TASK  (Core 0)
// =============================================================
void rgbTask(void* param) {
  for (;;) {
    rgbLedWrite(RGB_PIN, ethConnected ? 0 : RGB_BRIGHT,
                         ethConnected ? RGB_BRIGHT : 0, 0);
    vTaskDelay(pdMS_TO_TICKS(500));
  }
}

// =============================================================
//  NEOPIXEL HELPERS
// =============================================================
static inline int neoIdx(int ring, int led) {
  return ring * NEO_PER_RING + led;
}
static inline uint32_t neoColor(uint8_t r, uint8_t g, uint8_t b) {
  return neoStrip.Color(r, g, b);
}
static const uint32_t NEO_BLACK   = 0;
static const uint32_t NEO_RED50   = (uint32_t)MAX_BRIGHTNESS << 16;
static const uint32_t NEO_GREEN50 = (uint32_t)MAX_BRIGHTNESS << 8;
static const uint32_t NEO_BLUE50  = (uint32_t)MAX_BRIGHTNESS;

static uint32_t tempToColor(float ratio) {
  if (ratio <= 0.0f) return NEO_BLACK;
  float r = ratio > 1.0f ? 1.0f : ratio;
  uint8_t red, green, blue = 0;
  if (r < 0.33f) {
    float t = r / 0.33f;
    red = (uint8_t)(t * 255); green = 255;
  } else if (r < 0.66f) {
    float t = (r - 0.33f) / 0.33f;
    red = 255; green = (uint8_t)((1.0f - t * 0.6f) * 255);
  } else {
    float t = (r - 0.66f) / 0.34f;
    red = 255; green = (uint8_t)((1.0f - t) * 100);
  }
  return neoColor(
    (uint8_t)(red   * MAX_BRIGHTNESS / 255),
    (uint8_t)(green * MAX_BRIGHTNESS / 255),
    0
  );
}
static bool blinkOn(unsigned long now, uint16_t periodMs) {
  return (now % (unsigned long)periodMs) < (unsigned long)(periodMs / 2);
}

// =============================================================
//  NEOPIXEL HELPERS — rainbow & color interpolation
// =============================================================

// HSV→RGB: hue 0-255, sat/val 0-255 → packed color
static uint32_t hsvToColor(uint8_t hue, uint8_t sat, uint8_t val) {
  // Scale val to MAX_BRIGHTNESS
  val = (uint8_t)((uint16_t)val * MAX_BRIGHTNESS / 255);
  return neoStrip.ColorHSV((uint16_t)hue * 256, sat, val);
}

// Interpolate two RGB colors by t (0.0 - 1.0)
static uint32_t lerpColor(uint32_t a, uint32_t b, float t) {
  uint8_t ar = (a >> 16) & 0xFF, ag = (a >> 8) & 0xFF, ab = a & 0xFF;
  uint8_t br = (b >> 16) & 0xFF, bg = (b >> 8) & 0xFF, bb = b & 0xFF;
  return neoColor(
    (uint8_t)(ar + t * (br - ar)),
    (uint8_t)(ag + t * (bg - ag)),
    (uint8_t)(ab + t * (bb - ab))
  );
}

// Set all 8 LEDs on a ring to a rainbow gradient offset by phaseHue (0-255)
static void ringRainbow(int ring, uint8_t phaseHue, uint8_t brightness) {
  for (int l = 0; l < NEO_PER_RING; l++) {
    uint8_t hue = phaseHue + (uint8_t)(l * 256 / NEO_PER_RING);
    uint32_t c = neoStrip.gamma32(neoStrip.ColorHSV((uint16_t)hue * 256, 255,
                                   (uint16_t)brightness * MAX_BRIGHTNESS / 255));
    neoStrip.setPixelColor(neoIdx(ring, l), c);
  }
}

// Blink all 8 LEDs on a ring N times (blocking)
static void ringBlinkAll(int ring, uint32_t color, int times, uint16_t halfMs) {
  for (int i = 0; i < times; i++) {
    for (int l = 0; l < NEO_PER_RING; l++)
      neoStrip.setPixelColor(neoIdx(ring, l), color);
    neoStrip.show(); vTaskDelay(pdMS_TO_TICKS(halfMs));
    for (int l = 0; l < NEO_PER_RING; l++)
      neoStrip.setPixelColor(neoIdx(ring, l), NEO_BLACK);
    neoStrip.show(); vTaskDelay(pdMS_TO_TICKS(halfMs));
  }
}

// Blink all 8 LEDs on a ring N times with BLUE (blocking, for dryer-select confirm)
static void ringBlinkBlue3(int ring) {
  for (int i = 0; i < 3; i++) {
    for (int l = 0; l < NEO_PER_RING; l++)
      neoStrip.setPixelColor(neoIdx(ring, l), NEO_BLUE50);
    neoStrip.show(); vTaskDelay(pdMS_TO_TICKS(80));
    for (int l = 0; l < NEO_PER_RING; l++)
      neoStrip.setPixelColor(neoIdx(ring, l), NEO_BLACK);
    neoStrip.show(); vTaskDelay(pdMS_TO_TICKS(80));
  }
}

// Clear spacer LEDs (always-off physical positions)
static void clearSpacers(int ring) {
  for (int s = 0; s < 2; s++)
    neoStrip.setPixelColor(neoIdx(ring, NEO_SPACERS[s]), NEO_BLACK);
}

// Clear all neo pixels
static void neoClear() {
  for (int i = 0; i < NEO_TOTAL; i++) neoStrip.setPixelColor(i, NEO_BLACK);
}

// =============================================================
//  NEOPIXEL TASK  (Core 0, priority 1)
// =============================================================
void neoTask(void* param) {
  neoStrip.begin();
  neoStrip.setBrightness(MAX_BRIGHTNESS);
  neoStrip.clear();
  neoStrip.show();
  Serial.println("[Task] NeoPixel task started on Core 0, GPIO 34");

  UIState       prevUiState    = UI_HOME;
  int           prevDryer      = 0;
  int           prevChamber    = 0;
  bool          saveBlinkDone  = false;
  unsigned long wavePhase      = millis();
  unsigned long breathPhase    = millis();
  // Wave direction per ring: ring 0 goes CW (+1), ring 1 goes CCW (-1)
  // When chamber screen is active, two opposing waves give visual interest
  int           waveDir[NEO_RINGS] = {1, -1};

  for (;;) {
    unsigned long now = millis();

    UIState curState   = uiState;
    bool    ethLost    = ethWasLost && !ethConnected;
    int     curDryer   = selectedDryer;
    int     curChamber = selectedChamber;
    bool    saveDone   = saveResultReady;
    bool    inConfig   = (keypadActive || curState != UI_HOME);

    // ─── SYSTEM NOT READY: circle blue continuously ───────────
    if (!systemReady) {
      int pos = (int)((now / 100UL) % (unsigned long)NEO_PER_RING);
      for (int r = 0; r < NEO_RINGS; r++) {
        for (int l = 0; l < NEO_PER_RING; l++) {
          int dist = abs(l - pos);
          if (dist > NEO_PER_RING / 2) dist = NEO_PER_RING - dist;
          uint8_t v = (dist == 0) ? MAX_BRIGHTNESS
                    : (dist == 1) ? (uint8_t)(MAX_BRIGHTNESS / 2)
                    : (dist == 2) ? (uint8_t)(MAX_BRIGHTNESS / 6) : 0;
          neoStrip.setPixelColor(neoIdx(r, l), neoColor(0, 0, v));
        }
      }
      neoStrip.show();
      vTaskDelay(pdMS_TO_TICKS(NEO_REFRESH_MS));
      continue;
    }

    // ─── MODE 1: CONFIG ───────────────────────────────────────
    if (inConfig) {
      bool stateChanged = (curState != prevUiState);
      bool dryerChanged = (curDryer != prevDryer);

      // ── Transition effects on state entry ───────────────────
      if (stateChanged || dryerChanged) {
        int ring  = (curDryer >= FIRST_SLAVE) ? dryerToRing(curDryer) : 0;
        int other = 1 - ring;

        // Entering UI_THRESH_DRYER: already handled below (rainbow fade)
        // Entering UI_THRESH_CHAMBER from DRYER: blink blue 3× fast on selected ring
        if (curState == UI_THRESH_CHAMBER &&
            (prevUiState == UI_THRESH_DRYER || dryerChanged)) {
          for (int l = 0; l < NEO_PER_RING; l++)
            neoStrip.setPixelColor(neoIdx(other, l), NEO_BLACK);
          neoStrip.show();
          ringBlinkBlue3(ring);
          wavePhase  = millis();
          breathPhase = millis();
        }

        // Entering UI_THRESH_TYPE from CHAMBER: blink the selected-channel LED blue 3×
        if (curState == UI_THRESH_TYPE && prevUiState == UI_THRESH_CHAMBER) {
          int led = (curChamber >= 1 && curChamber <= NEO_CH_LEDS)
                    ? CH_TO_LED[curChamber - 1] : 0;
          for (int i = 0; i < 3; i++) {
            neoClear();
            neoStrip.setPixelColor(neoIdx(ring, led), NEO_BLUE50);
            neoStrip.show(); vTaskDelay(pdMS_TO_TICKS(80));
            neoClear();
            neoStrip.show(); vTaskDelay(pdMS_TO_TICKS(80));
          }
          breathPhase = millis();
        }

        if (curState == UI_WAITING_SAVE) saveBlinkDone = false;
        prevUiState = curState;
        prevDryer   = curDryer;
        prevChamber = curChamber;
        breathPhase = millis();
      }

      int ring  = (curDryer >= FIRST_SLAVE) ? dryerToRing(curDryer) : 0;
      int other = 1 - ring;

      // ── UI_THRESH_DRYER: rainbow fade circle on BOTH rings ──
      // Pressing C triggers this. Both rings sweep a slow rotating rainbow.
      if (curState == UI_THRESH_DRYER) {
        uint8_t phaseHue = (uint8_t)((now / 20UL) & 0xFF);
        // Breathe brightness in sync
        float   bt  = (float)((now % 2000UL)) / 2000.0f;
        float   brt = (bt < 0.5f) ? bt * 2.0f : 2.0f - bt * 2.0f;
        uint8_t bv  = 80 + (uint8_t)(brt * 175);  // 80..255 range
        for (int r = 0; r < NEO_RINGS; r++)
          ringRainbow(r, phaseHue + (uint8_t)(r * 128), bv);
      }

      // ── UI_THRESH_CHAMBER: opposing rainbow waves on selected ring ──
      // Two waves going in opposite directions (CW and CCW), rainbow colored.
      else if (curState == UI_THRESH_CHAMBER) {
        // Clear other ring
        for (int l = 0; l < NEO_PER_RING; l++)
          neoStrip.setPixelColor(neoIdx(other, l), NEO_BLACK);

        unsigned long elapsed = now - wavePhase;
        // Wave A: CW position
        int posA = (int)((elapsed / 80UL) % (unsigned long)NEO_PER_RING);
        // Wave B: CCW position (opposite direction)
        int posB = (NEO_PER_RING - 1 - posA) % NEO_PER_RING;
        uint8_t baseHue = (uint8_t)((now / 15UL) & 0xFF);

        for (int l = 0; l < NEO_PER_RING; l++) {
          // Distance from wave A and wave B
          int dA = abs(l - posA); if (dA > NEO_PER_RING/2) dA = NEO_PER_RING - dA;
          int dB = abs(l - posB); if (dB > NEO_PER_RING/2) dB = NEO_PER_RING - dB;
          int d  = min(dA, dB);
          uint8_t v = (d == 0) ? MAX_BRIGHTNESS
                    : (d == 1) ? (uint8_t)(MAX_BRIGHTNESS * 2 / 3)
                    : (d == 2) ? (uint8_t)(MAX_BRIGHTNESS / 4) : 0;
          uint8_t hue = baseHue + (uint8_t)(l * 256 / NEO_PER_RING);
          uint32_t c  = neoStrip.gamma32(neoStrip.ColorHSV((uint16_t)hue * 256, 255, v));
          neoStrip.setPixelColor(neoIdx(ring, l), c);
        }
      }

      // ── UI_THRESH_TYPE: rainbow fade-glow on selected chamber LED ──
      // After chamber selected, before A/B chosen: that LED glows through rainbow.
      else if (curState == UI_THRESH_TYPE) {
        neoClear();
        int led = (curChamber >= 1 && curChamber <= NEO_CH_LEDS)
                  ? CH_TO_LED[curChamber - 1] : 0;
        float t   = (float)((now - breathPhase) % 1800UL) / 1800.0f;
        float brt = (t < 0.5f) ? t * 2.0f : 2.0f - t * 2.0f;
        uint8_t v   = (uint8_t)(brt * 255);
        uint8_t hue = (uint8_t)((now / 12UL) & 0xFF);  // slow rainbow cycle
        uint32_t c  = neoStrip.gamma32(neoStrip.ColorHSV((uint16_t)hue * 256, 255,
                                       (uint16_t)v * MAX_BRIGHTNESS / 255));
        neoStrip.setPixelColor(neoIdx(ring, led), c);
      }

      // ── UI_THRESH_VALUE: color-coded fade per threshold type ──
      // Upper threshold (A): fade RED → YELLOW (hot warning colors)
      // Lower threshold (B): fade YELLOW → GREEN (safe zone colors)
      else if (curState == UI_THRESH_VALUE) {
        int led = (curChamber >= 1 && curChamber <= NEO_CH_LEDS)
                  ? CH_TO_LED[curChamber - 1] : 0;
        float t   = (float)((now - breathPhase) % 1500UL) / 1500.0f;
        float brt = (t < 0.5f) ? t * 2.0f : 2.0f - t * 2.0f;
        uint8_t v = (uint8_t)(brt * MAX_BRIGHTNESS);
        neoClear();
        uint32_t fadeColor;
        if (!selectedIsLower) {
          // Upper threshold: interpolate RED → YELLOW using breath
          // Full bright = red; half breath = yellow
          float phase = (float)((now / 30UL) % 100UL) / 100.0f;
          uint32_t red    = neoColor(v, 0, 0);
          uint32_t yellow = neoColor(v, v / 2, 0);
          fadeColor = lerpColor(red, yellow, phase);
        } else {
          // Lower threshold: interpolate YELLOW → GREEN using breath
          float phase = (float)((now / 30UL) % 100UL) / 100.0f;
          uint32_t yellow = neoColor(v, v / 2, 0);
          uint32_t green  = neoColor(0, v, 0);
          fadeColor = lerpColor(yellow, green, phase);
        }
        neoStrip.setPixelColor(neoIdx(ring, led), fadeColor);
      }

      // ── UI_WAITING_SAVE: rotating GREEN chaser all 8 LEDs ──
      else if (curState == UI_WAITING_SAVE) {
        if (saveDone && !saveBlinkDone) {
          saveBlinkDone = true;
          ringBlinkAll(ring, NEO_GREEN50, 3, 150);
        } else if (!saveBlinkDone) {
          unsigned long elapsed = (now - breathPhase) % (unsigned long)(NEO_PER_RING * 80);
          int pos = (int)(elapsed / 80UL);
          neoClear();
          for (int l = 0; l < NEO_PER_RING; l++) {
            int dist = abs(l - pos);
            if (dist > NEO_PER_RING / 2) dist = NEO_PER_RING - dist;
            uint8_t v = (dist == 0) ? MAX_BRIGHTNESS
                      : (dist == 1) ? (uint8_t)(MAX_BRIGHTNESS / 2)
                      : (dist == 2) ? (uint8_t)(MAX_BRIGHTNESS / 6) : 0;
            neoStrip.setPixelColor(neoIdx(ring, l), neoColor(0, v, 0));
          }
        }
      }

      neoStrip.show();
      vTaskDelay(pdMS_TO_TICKS(NEO_REFRESH_MS));
      continue;
    }

    prevUiState   = UI_HOME;
    saveBlinkDone = false;

    // ─── MODE 2: ETH LOST — blue chaser ALL 8 LEDs on both rings ──
    if (ethLost) {
      int pos = (int)((now / 100UL) % (unsigned long)NEO_PER_RING);
      for (int r = 0; r < NEO_RINGS; r++) {
        for (int l = 0; l < NEO_PER_RING; l++) {
          int dist = abs(l - pos);
          if (dist > NEO_PER_RING / 2) dist = NEO_PER_RING - dist;
          uint8_t v = (dist == 0) ? MAX_BRIGHTNESS
                    : (dist == 1) ? (uint8_t)(MAX_BRIGHTNESS / 2)
                    : (dist == 2) ? (uint8_t)(MAX_BRIGHTNESS / 6) : 0;
          neoStrip.setPixelColor(neoIdx(r, l), neoColor(0, 0, v));
        }
      }
      neoStrip.show();
      vTaskDelay(pdMS_TO_TICKS(NEO_REFRESH_MS));
      continue;
    }

    // ─── MODE 3: NORMAL temperature display ──────────────────
    // Ring assignment: Dryer 5 (dIdx=0) → ring 1, Dryer 6 (dIdx=1) → ring 0
    for (int d = 0; d < NUM_DRYERS; d++) {
      int ring = dryerIdxToRing(d);
      // Always clear spacer (bottom) LEDs
      clearSpacers(ring);

      if (!hasValidReading[d]) {
        // No reading: all 8 LEDs red to indicate comm failure
        for (int l = 0; l < NEO_PER_RING; l++)
          neoStrip.setPixelColor(neoIdx(ring, l), NEO_RED50);
        continue;
      }
      // Update the 6 channel LEDs using CH_TO_LED mapping
      for (int c = 0; c < NEO_CH_LEDS; c++) {
        int led = CH_TO_LED[c];
        if (!channelValid[d][c] || temperatures[d][c] <= 0.0f) {
          neoStrip.setPixelColor(neoIdx(ring, led), NEO_BLACK);
          continue;
        }
        float temp   = temperatures[d][c];
        float thresh = 200.0f;
        float lowThresh = 10.0f;
        if (xSemaphoreTake(threshMutex, pdMS_TO_TICKS(5)) == pdTRUE) {
          if (thresholds[d][c] > 0.0f) thresh = thresholds[d][c];
          if (lowerThresholds[d][c] > 0.0f) lowThresh = lowerThresholds[d][c];
          xSemaphoreGive(threshMutex);
        }
        if (temp > thresh) {
          neoStrip.setPixelColor(neoIdx(ring, led),
                                 blinkOn(now, 500) ? NEO_RED50 : NEO_BLACK);
        } else if (temp < lowThresh) {
          neoStrip.setPixelColor(neoIdx(ring, led),
                                 blinkOn(now, 500) ? NEO_GREEN50 : NEO_BLACK);
        } else {
          float ratio = temp / thresh;
          neoStrip.setPixelColor(neoIdx(ring, led), tempToColor(ratio));
        }
      }
    }
    neoStrip.show();
    vTaskDelay(pdMS_TO_TICKS(NEO_REFRESH_MS));
  }
}

// =============================================================
//  LCD BUFFER HELPERS
// =============================================================
static void padTo16(const char* src, char* dst) {
  int i = 0;
  while (i < 16 && src[i]) { dst[i] = src[i]; i++; }
  while (i < 16)            { dst[i++] = ' '; }
  dst[16] = '\0';
}
void setLcdRow(int row, const char* msg) {
  if (xSemaphoreTake(lcdBufMutex, pdMS_TO_TICKS(30)) == pdTRUE) {
    padTo16(msg, lcdBuf[row]);
    xSemaphoreGive(lcdBufMutex);
  }
}
void setLcdRow(int row, const String& msg) { setLcdRow(row, msg.c_str()); }
void setLcdBuf(const char* r0, const char* r1) {
  if (xSemaphoreTake(lcdBufMutex, pdMS_TO_TICKS(30)) == pdTRUE) {
    padTo16(r0, lcdBuf[0]);
    padTo16(r1, lcdBuf[1]);
    xSemaphoreGive(lcdBufMutex);
  }
}
void setLcdBuf(const String& r0, const String& r1) {
  setLcdBuf(r0.c_str(), r1.c_str());
}
void setStatusMsg(const char* msg, unsigned long holdMs) {
  if (xSemaphoreTake(lcdBufMutex, pdMS_TO_TICKS(30)) == pdTRUE) {
    statusMsgLine0[0] = '\0';
    padTo16(msg, statusMsgLine1);
    statusExpiry = millis() + holdMs;
    xSemaphoreGive(lcdBufMutex);
  }
  Serial.printf("[Status] %s (%lu ms)\n", msg, holdMs);
}
void setStatusMsg(const String& msg, unsigned long holdMs) {
  setStatusMsg(msg.c_str(), holdMs);
}
void setDualStatusMsg(const char* r0, const char* r1, unsigned long holdMs) {
  if (xSemaphoreTake(lcdBufMutex, pdMS_TO_TICKS(30)) == pdTRUE) {
    padTo16(r0, statusMsgLine0);
    padTo16(r1, statusMsgLine1);
    statusExpiry = millis() + holdMs;
    xSemaphoreGive(lcdBufMutex);
  }
  Serial.printf("[DualStatus] %s | %s (%lu ms)\n", r0, r1, holdMs);
}

// =============================================================
//  LCD HARDWARE WRITE
// =============================================================
static bool bufferHasGarbage(const char* r0, const char* r1) {
  for (int i = 0; i < 16; i++) {
    if ((uint8_t)r0[i] < 0x20 || (uint8_t)r0[i] > 0x7E) return true;
    if ((uint8_t)r1[i] < 0x20 || (uint8_t)r1[i] > 0x7E) return true;
  }
  return false;
}
void lcdHardwareFlush() {
  char r0[17], r1[17];
  if (xSemaphoreTake(lcdBufMutex, pdMS_TO_TICKS(30)) == pdTRUE) {
    memcpy(r0, lcdBuf[0], 17);
    memcpy(r1, lcdBuf[1], 17);
    xSemaphoreGive(lcdBufMutex);
  } else return;
  if (bufferHasGarbage(r0, r1)) {
    Serial.println("[LCD-L1] Garbage in lcdBuf — triggering heal");
    lcdNeedsHeal = true;
    return;
  }
  if (xSemaphoreTake(lcdHwMutex, pdMS_TO_TICKS(30)) == pdTRUE) {
    lcd.setCursor(0, 0); lcd.print(r0);
    lcd.setCursor(0, 1); lcd.print(r1);
    xSemaphoreGive(lcdHwMutex);
  }
}
void lcdHardwareDirect(const char* r0, const char* r1) {
  char b0[17], b1[17];
  padTo16(r0, b0);
  padTo16(r1, b1);
  if (lcdBufMutex != NULL &&
      xSemaphoreTake(lcdBufMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    memcpy(lcdBuf[0], b0, 17);
    memcpy(lcdBuf[1], b1, 17);
    xSemaphoreGive(lcdBufMutex);
  }
  if (lcdHwMutex != NULL &&
      xSemaphoreTake(lcdHwMutex, pdMS_TO_TICKS(500)) == pdTRUE) {
    lcd.setCursor(0, 0); lcd.print(b0);
    lcd.setCursor(0, 1); lcd.print(b1);
    xSemaphoreGive(lcdHwMutex);
  } else if (lcdHwMutex == NULL) {
    lcd.setCursor(0, 0); lcd.print(b0);
    lcd.setCursor(0, 1); lcd.print(b1);
  }
  Serial.printf("[LCD] \"%s\" | \"%s\"\n", b0, b1);
}

// =============================================================
//  LCD SELF-HEAL
// =============================================================
static void i2cBusReset() {
  Serial.println("[LCD-Heal] I2C bit-bang reset...");
  Wire.end();
  pinMode(LCD_SDA_PIN, INPUT_PULLUP);
  pinMode(LCD_SCL_PIN, OUTPUT);
  for (int i = 0; i < 9; i++) {
    digitalWrite(LCD_SCL_PIN, LOW);  delayMicroseconds(5);
    digitalWrite(LCD_SCL_PIN, HIGH); delayMicroseconds(5);
    if (digitalRead(LCD_SDA_PIN) == HIGH) {
      Serial.printf("[LCD-Heal] SDA released after %d pulses\n", i + 1);
      break;
    }
  }
  pinMode(LCD_SDA_PIN, OUTPUT);
  digitalWrite(LCD_SDA_PIN, LOW);  delayMicroseconds(5);
  digitalWrite(LCD_SCL_PIN, HIGH); delayMicroseconds(5);
  digitalWrite(LCD_SDA_PIN, HIGH); delayMicroseconds(5);
  pinMode(LCD_SDA_PIN, INPUT_PULLUP);
  pinMode(LCD_SCL_PIN, INPUT_PULLUP);
  Wire.begin(LCD_SDA_PIN, LCD_SCL_PIN);
  delayMicroseconds(100);
  Serial.println("[LCD-Heal] I2C restored");
}
static void lcdReinitAndRedraw(const char* reason) {
  Serial.printf("[LCD-Heal] Reinit: %s\n", reason);
  lcd.init();
  lcd.backlight();
  delay(50);
  char r0[17], r1[17];
  bool gotBuf = false;
  if (lcdBufMutex != NULL &&
      xSemaphoreTake(lcdBufMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    memcpy(r0, lcdBuf[0], 17);
    memcpy(r1, lcdBuf[1], 17);
    gotBuf = true;
    if (bufferHasGarbage(r0, r1)) {
      memset(lcdBuf[0], ' ', 16); lcdBuf[0][16] = '\0';
      memset(lcdBuf[1], ' ', 16); lcdBuf[1][16] = '\0';
      memset(r0, ' ', 16); r0[16] = '\0';
      memset(r1, ' ', 16); r1[16] = '\0';
    }
    xSemaphoreGive(lcdBufMutex);
  }
  if (!gotBuf) {
    memset(r0, ' ', 16); r0[16] = '\0';
    memset(r1, ' ', 16); r1[16] = '\0';
  }
  lcd.setCursor(0, 0); lcd.print(r0);
  lcd.setCursor(0, 1); lcd.print(r1);
  lcdNeedsHeal = false;
}
static void lcdSelfHeal() {
  i2cBusReset();
  lcdReinitAndRedraw("I2C NACK / lockup");
}

// =============================================================
//  BUILD LCD CONTENT  (Core 0, every 250 ms)
// =============================================================
static int           alertIdx    = 0;
static bool          alertToggle = false;
static unsigned long alertSwapAt = 0;
void buildLCDContent() {
  if (!systemReady) return;
  if (keypadActive)  return;
  unsigned long now = millis();
  bool alertHiSnap[NUM_DRYERS], alertLoSnap[NUM_DRYERS];
  if (xSemaphoreTake(alertMutex, pdMS_TO_TICKS(5)) == pdTRUE) {
    for (int d = 0; d < NUM_DRYERS; d++) {
      alertHiSnap[d] = dryerAlertHigh[d];
      alertLoSnap[d] = dryerAlertLow[d];
    }
    xSemaphoreGive(alertMutex);
  } else return;
  if (infoMode != INFO_NONE) {
    if (now >= (unsigned long)infoExpiry) {
      infoMode = INFO_NONE;
    } else {
      if (infoMode == INFO_IP) {
        String ip = ethConnected ? ETH.localIP().toString() : "No ETH link";
        setLcdBuf("IP Address:     ", ip);
      } else {
        // Marquee scrolling of MAC address
        String mac = ETH.macAddress();
        String scrollText = "MAC: " + mac + "   ";
        int elapsedMs = 0;
        if (infoExpiry > now) {
          elapsedMs = 5000 - (infoExpiry - now);
        } else {
          elapsedMs = 5000;
        }
        int step = (elapsedMs / 300) % scrollText.length();
        String displayStr = scrollText.substring(step) + scrollText.substring(0, step);
        setLcdBuf("MAC Address:    ", displayStr.substring(0, 16));
      }
      return;
    }
  }
  bool statusActive = false;
  char sBuf0[17] = "";
  char sBuf1[17] = "";
  if (xSemaphoreTake(lcdBufMutex, pdMS_TO_TICKS(20)) == pdTRUE) {
    if (now < statusExpiry) {
      memcpy(sBuf0, statusMsgLine0, 17);
      memcpy(sBuf1, statusMsgLine1, 17);
      statusActive = true;
    }
    xSemaphoreGive(lcdBufMutex);
  }
  if (statusActive) {
    if (sBuf0[0] == '\0') {
      int ch = homeDisplayChannel;
      int ci = ch - 1;
      char r0[17];
      if (channelValid[0][ci])
        snprintf(r0, sizeof(r0), "Dryer5,C%d:%3d%s",
                 ch, (int)temperatures[0][ci],
                 (alertHiSnap[0] || alertLoSnap[0]) ? "!" : " ");
      else
        snprintf(r0, sizeof(r0), "Dryer5,C%d: --- ", ch);
      setLcdBuf(r0, sBuf1);
    } else {
      setLcdBuf(sBuf0, sBuf1);
    }
    return;
  }
  AlertDisplay snapList[TOTAL_CH * 2];
  int snapCnt = 0;
  if (xSemaphoreTake(alertMutex, pdMS_TO_TICKS(5)) == pdTRUE) {
    snapCnt = lcdAlertCount;
    if (snapCnt > 0)
      memcpy(snapList, lcdAlertList, snapCnt * sizeof(AlertDisplay));
    xSemaphoreGive(alertMutex);
  } else return;
  if (snapCnt > 0) {
    if (alertIdx >= snapCnt) alertIdx = 0;
    if (now >= alertSwapAt) {
      alertSwapAt = now + LCD_ALERT_SWAP_MS;
      alertToggle = !alertToggle;
      if (!alertToggle && snapCnt > 0)
        alertIdx = (alertIdx + 1) % snapCnt;
    }
    if (alertIdx >= snapCnt) alertIdx = 0;
    AlertDisplay& a = snapList[alertIdx];
    char r0[17], r1[17];
    snprintf(r0, sizeof(r0), "D%d Ch%d %s",
             a.dryer, a.chamber, a.isHigh ? "HIGH " : "LOW  ");
    if (!alertToggle)
      snprintf(r1, sizeof(r1), "PV=%-4d SV=%-4d", (int)a.pv, (int)a.sv);
    else
      snprintf(r1, sizeof(r1), a.isHigh ? "Above threshold" : "Below threshold");
    setLcdBuf(r0, r1);
    return;
  }
  int ch = homeDisplayChannel;
  int ci = ch - 1;
  char r0[17], r1[17];
  if (channelValid[0][ci])
    snprintf(r0, sizeof(r0), "Dryer5,C%d:%3d%s",
             ch, (int)temperatures[0][ci],
             (alertHiSnap[0] || alertLoSnap[0]) ? "!" : " ");
  else
    snprintf(r0, sizeof(r0), "Dryer5,C%d: --- ", ch);
  if (channelValid[1][ci])
    snprintf(r1, sizeof(r1), "Dryer6,C%d:%3d%s",
             ch, (int)temperatures[1][ci],
             (alertHiSnap[1] || alertLoSnap[1]) ? "!" : " ");
  else
    snprintf(r1, sizeof(r1), "Dryer6,C%d: --- ", ch);
  setLcdBuf(r0, r1);
}

// =============================================================
//  LCD TASK  (Core 0, priority 1)
// =============================================================
void lcdTask(void* param) {
  Serial.println("[Task] LCD task started on Core 0");
  lcdLastPingCheck    = millis();
  lcdLastForcedReinit = millis();
  for (;;) {
    unsigned long now = millis();
    if (lcdNeedsHeal) {
      if (xSemaphoreTake(lcdHwMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        i2cBusReset();
        lcdReinitAndRedraw("garbage in buffer");
        xSemaphoreGive(lcdHwMutex);
      }
      lcdLastPingCheck    = now;
      lcdLastForcedReinit = now;
    }
    if (!keypadActive &&
        (now - lcdLastForcedReinit >= LCD_REINIT_INTERVAL_MS)) {
      lcdLastForcedReinit = now;
      if (xSemaphoreTake(lcdHwMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        lcdReinitAndRedraw("periodic reinit");
        xSemaphoreGive(lcdHwMutex);
      }
      lcdLastPingCheck = now;
    }
    if (now - lcdLastPingCheck >= LCD_PING_INTERVAL_MS) {
      lcdLastPingCheck = now;
      if (xSemaphoreTake(lcdHwMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        Wire.beginTransmission(LCD_I2C_ADDR);
        bool ok = (Wire.endTransmission() == 0);
        if (ok) {
          if (!lcdHealthy) lcdReinitAndRedraw("wire reconnected");
          else Serial.println("[LCD-L3] Ping OK");
          lcdHealthy = true;
        } else {
          lcdHealthy = false;
          Serial.println("[LCD-L3] Ping NACK — full self-heal");
          lcdSelfHeal();
        }
        xSemaphoreGive(lcdHwMutex);
      }
    }
    buildLCDContent();
    lcdHardwareFlush();
    vTaskDelay(pdMS_TO_TICKS(LCD_REFRESH_MS));
  }
}

// =============================================================
//  CRC16 / FLOAT DECODE
// =============================================================
uint16_t crc16(uint8_t* buf, uint8_t len) {
  uint16_t crc = 0xFFFF;
  for (uint8_t i = 0; i < len; i++) {
    crc ^= buf[i];
    for (uint8_t j = 0; j < 8; j++)
      crc = (crc & 1) ? (crc >> 1) ^ 0xA001 : (crc >> 1);
  }
  return crc;
}
float bytesToFloat(uint8_t b0, uint8_t b1, uint8_t b2, uint8_t b3) {
  uint32_t raw = ((uint32_t)b0 << 24) | ((uint32_t)b1 << 16) |
                 ((uint32_t)b2 <<  8) |  (uint32_t)b3;
  float f; memcpy(&f, &raw, 4); return f;
}

// =============================================================
//  MODBUS READ
// =============================================================
bool readMS1208(uint8_t slaveId, float* results, bool* valid) {
  while (rs485Serial.available()) rs485Serial.read();
  uint8_t req[8];
  req[0] = slaveId; req[1] = 0x03;
  req[2] = (START_REG >> 8) & 0xFF; req[3] = START_REG & 0xFF;
  req[4] = 0x00; req[5] = 16;
  uint16_t c = crc16(req, 6);
  req[6] = c & 0xFF; req[7] = (c >> 8) & 0xFF;
  digitalWrite(RS485_DE_PIN, HIGH);
  vTaskDelay(pdMS_TO_TICKS(1));
  rs485Serial.write(req, 8);
  rs485Serial.flush();
  delay(2);
  digitalWrite(RS485_DE_PIN, LOW);
  const uint8_t RLEN = 37;
  uint8_t resp[RLEN]; uint8_t idx = 0;
  unsigned long t = millis();
  while (millis() - t < 1500 && idx < RLEN) {
    if (rs485Serial.available()) resp[idx++] = rs485Serial.read();
    vTaskDelay(1);
  }
  if (idx < RLEN) {
    Serial.printf("[Modbus] Slave %d: timeout\n", slaveId);
    return false;
  }
  uint16_t rCRC = ((uint16_t)resp[RLEN-1] << 8) | resp[RLEN-2];
  if (rCRC != crc16(resp, RLEN-2)) {
    Serial.printf("[Modbus] Slave %d: CRC err\n", slaveId);
    return false;
  }
  for (uint8_t i = 0; i < NUM_CHANNELS; i++) {
    uint8_t base = 3 + i * 4;
    float val = bytesToFloat(resp[base+2], resp[base+3], resp[base], resp[base+1]);
    if (isnan(val) || isinf(val) || val > 2000.0f || val < -300.0f)
      { results[i] = 0.0f; valid[i] = false; }
    else
      { results[i] = val;  valid[i] = true;  }
  }
  return true;
}
void readAllDryers() {
  if (keypadActive) return;
  Serial.println("[Read] Reading dryers 5 & 6...");
  for (int d = 0; d < NUM_DRYERS; d++) {
    if (keypadActive) return;
    if (!systemReady) {
      char buf[17];
      snprintf(buf, sizeof(buf), "Reading Dryer %d..", d + FIRST_SLAVE);
      lcdHardwareDirect("Reading Sensors", buf);
    }
    bool ok = readMS1208(d + FIRST_SLAVE, temperatures[d], channelValid[d]);
    if (ok) {
      hasValidReading[d] = true;
      Serial.printf("[Read] Dryer %d OK\n", d + FIRST_SLAVE);
    } else {
      Serial.printf("[Read] Dryer %d FAILED\n", d + FIRST_SLAVE);
      for (int c = 0; c < NUM_CHANNELS; c++) {
        temperatures[d][c] = 0; channelValid[d][c] = false;
      }
    }
    delay(20);
  }
}

// =============================================================
//  RELAY BLINK TASK  (Core 0, priority 1)
// =============================================================
void relayBlinkTask(void* param) {
  bool          relayOn[NUM_DRYERS]        = {};
  unsigned long lastToggle[NUM_DRYERS]     = {};
  int           prevAlertState[NUM_DRYERS] = {};
  static bool   prevHorn                   = false;
  for (;;) {
    unsigned long now = millis();
    bool hi[NUM_DRYERS], lo[NUM_DRYERS];
    if (xSemaphoreTake(alertMutex, pdMS_TO_TICKS(5)) == pdTRUE) {
      for (int d = 0; d < NUM_DRYERS; d++) {
        hi[d] = dryerAlertHigh[d];
        lo[d] = dryerAlertLow[d];
      }
      xSemaphoreGive(alertMutex);
    } else {
      vTaskDelay(pdMS_TO_TICKS(20));
      continue;
    }
    bool anyAlert = false;
    for (int d = 0; d < NUM_DRYERS; d++)
      if (hi[d] || lo[d]) { anyAlert = true; break; }
    if (anyAlert != prevHorn) {
      digitalWrite(RELAY_HORN, anyAlert ? HIGH : LOW);
      prevHorn = anyAlert;
      vTaskDelay(pdMS_TO_TICKS(50));
    }
    bool anyTransition = false;
    for (int d = 0; d < NUM_DRYERS; d++) {
      int alertState = 0;
      if (hi[d])      alertState = 2;
      else if (lo[d]) alertState = 1;
      bool newOn = relayOn[d];
      if (alertState == 0) {
        newOn = false;
        lastToggle[d] = 0;
        prevAlertState[d] = 0;
      } else {
        if (alertState != prevAlertState[d]) {
          prevAlertState[d] = alertState;
          lastToggle[d] = now;
          newOn = true;
        } else {
          unsigned long iv = (alertState == 2) ? BLINK_FAST_MS : BLINK_SLOW_MS;
          if (now - lastToggle[d] >= iv) {
            lastToggle[d] = now;
            newOn = !relayOn[d];
          }
        }
      }
      if (newOn != relayOn[d]) {
        relayOn[d] = newOn;
        anyTransition = true;
        digitalWrite(RELAY_DRYER[d], relayOn[d] ? LOW : HIGH);
      }
    }
    if (anyTransition) vTaskDelay(pdMS_TO_TICKS(50));
    vTaskDelay(pdMS_TO_TICKS(20));
  }
}

// =============================================================
//  BUILD ALERT LIST  (Core 1)
// =============================================================
void buildAlertList() {
  AlertDisplay tmp[TOTAL_CH * 2];
  int cnt = 0;
  for (int d = 0; d < NUM_DRYERS; d++) {
    for (int c = 0; c < NUM_CHANNELS; c++) {
      if (!channelValid[d][c] || temperatures[d][c] == 0.0f) continue;
      float t = temperatures[d][c];
      if (thresholds[d][c]      > 0.0f && t > thresholds[d][c])
        tmp[cnt++] = {d + FIRST_SLAVE, c + 1, thresholds[d][c], t, true};
      if (lowerThresholds[d][c] > 0.0f && t < lowerThresholds[d][c])
        tmp[cnt++] = {d + FIRST_SLAVE, c + 1, lowerThresholds[d][c], t, false};
    }
  }
  if (xSemaphoreTake(alertMutex, pdMS_TO_TICKS(20)) == pdTRUE) {
    memcpy(lcdAlertList, tmp, cnt * sizeof(AlertDisplay));
    lcdAlertCount = cnt;
    xSemaphoreGive(alertMutex);
  }
}
void updateRelays() {
  for (int d = 0; d < NUM_DRYERS; d++) {
    bool hi = false, lo = false;
    for (int c = 0; c < NUM_CHANNELS; c++) {
      if (!channelValid[d][c] || temperatures[d][c] == 0.0f) continue;
      float t = temperatures[d][c];
      if (thresholds[d][c]      > 0.0f && t > thresholds[d][c])      hi = true;
      if (lowerThresholds[d][c] > 0.0f && t < lowerThresholds[d][c]) lo = true;
    }
    if (xSemaphoreTake(alertMutex, pdMS_TO_TICKS(10)) == pdTRUE) {
      dryerAlertHigh[d] = hi;
      dryerAlertLow[d]  = lo;
      xSemaphoreGive(alertMutex);
    }
  }
  buildAlertList();
}

// =============================================================
//  HTTP GET
// =============================================================
String httpGet(const String& url) {
  if (!ethConnected) return "";
  WiFiClientSecure* client = new WiFiClientSecure();
  if (!client) {
    Serial.println("[HTTP] OOM: WiFiClientSecure alloc failed");
    return "";
  }
  client->setInsecure();
  HTTPClient http;
  http.begin(*client, url);
  http.setFollowRedirects(HTTPC_FORCE_FOLLOW_REDIRECTS);
  http.setTimeout(4000);
  int code = http.GET();
  String body = "";
  if (code == HTTP_CODE_OK) {
    body = http.getString();
    httpFailCount = 0;
  } else {
    Serial.printf("[HTTP] Error: %d (fail#%d)\n", code, ++httpFailCount);
  }
  http.end();
  client->stop();
  delete client;
  return body;
}

// =============================================================
//  UPLOAD  (Core 1)
// =============================================================
bool uploadDryer(int dIdx) {
  if (keypadActive || !ethConnected || !hasValidReading[dIdx]) return false;
  String url = String(SHEET_URL) + "?action=write&dryer=" + String(dIdx + FIRST_SLAVE);
  for (int c = 0; c < NUM_CHANNELS; c++) {
    float v = (channelValid[dIdx][c] && !isnan(temperatures[dIdx][c]))
              ? temperatures[dIdx][c] : 0.0f;
    url += "&t" + String(c + 1) + "=" + String(v, 2);
  }
  String resp = httpGet(url);
  if (keypadActive) return false;
  if (resp.length() > 0) {
    setStatusMsg("D" + String(dIdx + FIRST_SLAVE) + " Sent OK", 2000UL);
    return true;
  }
  setStatusMsg("D" + String(dIdx + FIRST_SLAVE) + " FAILED!", 2000UL);
  return false;
}

// =============================================================
//  THRESHOLD FETCH  (Core 1)
// =============================================================
void fetchThresholds() {
  if (keypadActive || !ethConnected) return;
  Serial.printf("[Thresh] Fetching (max %d attempts)...\n", THRESHOLD_FETCH_RETRIES);
  for (int attempt = 1; attempt <= THRESHOLD_FETCH_RETRIES; attempt++) {
    char buf[17];
    snprintf(buf, sizeof(buf), "Try %d/%d ...", attempt, THRESHOLD_FETCH_RETRIES);
    if (!systemReady) lcdHardwareDirect("Fetching thresh.", buf);
    else              setStatusMsg(buf, 12000UL);
    String payload = httpGet(String(SHEET_URL) + "?action=getThresholds");
    if (keypadActive) return;
    if (payload.length() == 0) {
      char fb[17];
      snprintf(fb, sizeof(fb), "Fail %d/%d...", attempt, THRESHOLD_FETCH_RETRIES);
      setStatusMsg(fb, 1500UL);
      delay(500);
      continue;
    }
    JsonDocument doc;
    if (deserializeJson(doc, payload) || !doc["thresholds"].is<JsonObject>()) {
      char pb[17];
      snprintf(pb, sizeof(pb), "ParseErr %d/%d", attempt, THRESHOLD_FETCH_RETRIES);
      setStatusMsg(pb, 1500UL);
      delay(500);
      continue;
    }
    if (xSemaphoreTake(threshMutex, pdMS_TO_TICKS(500)) == pdTRUE) {
      JsonObject thresh = doc["thresholds"].as<JsonObject>();
      for (int d = FIRST_SLAVE; d < FIRST_SLAVE + NUM_DRYERS; d++) {
        String dk = "D" + String(d);
        if (!thresh[dk].is<JsonObject>()) continue;
        JsonObject dObj = thresh[dk].as<JsonObject>();
        for (int c = 1; c <= NUM_CHANNELS; c++) {
          String ck = "C" + String(c);
          if (!dObj[ck].isNull())
            thresholds[d - FIRST_SLAVE][c - 1] = dObj[ck].as<float>();
        }
      }
      if (doc["lowerThresholds"].is<JsonObject>()) {
        JsonObject lt = doc["lowerThresholds"].as<JsonObject>();
        for (int d = FIRST_SLAVE; d < FIRST_SLAVE + NUM_DRYERS; d++) {
          String dk = "D" + String(d);
          if (!lt[dk].is<JsonObject>()) continue;
          JsonObject dObj = lt[dk].as<JsonObject>();
          for (int c = 1; c <= NUM_CHANNELS; c++) {
            String ck = "C" + String(c);
            if (!dObj[ck].isNull())
              lowerThresholds[d - FIRST_SLAVE][c - 1] = dObj[ck].as<float>();
          }
        }
      }
      xSemaphoreGive(threshMutex);
    }
    updateRelays();
    Serial.printf("[Thresh] Updated on attempt %d\n", attempt);
    setStatusMsg("Thresh updated!", 2000UL);
    return;
  }
  Serial.println("[Thresh] All attempts failed — will retry next cycle.");
  setStatusMsg("Thresh fetch fail", 3000UL);
}

// =============================================================
//  STATUS TIMEOUT FETCH
// =============================================================
void fetchStatusTimeout() {
  if (keypadActive || !ethConnected) return;
  String payload = httpGet(String(SHEET_URL) + "?action=getStatusTimeout");
  if (payload.length() == 0) return;
  JsonDocument doc;
  if (!deserializeJson(doc, payload) && !doc["timeoutSeconds"].isNull()) {
    int val = doc["timeoutSeconds"].as<int>();
    if (val >= 10 && val <= 3600) statusTimeoutSec = val;
  }
}

// =============================================================
//  THRESHOLD SAVE  (Core 1)
// =============================================================
bool saveThresholdToSheet(int dryer, int chamber, float value, bool isLower) {
  if (!ethConnected) {
    setStatusMsg("No ETH-cant save", 2000UL);
    return false;
  }
  const char* action = isLower ? "setLowerThreshold" : "setThreshold";
  String url = String(SHEET_URL)
             + "?action=" + action
             + "&dryer="   + String(dryer)
             + "&chamber=" + String(chamber)
             + "&value="   + String(value, 1);
  String tag = isLower ? "[Lo]" : "[Hi]";
  setLcdBuf("Saving D" + String(dryer) + "C" + String(chamber) + tag,
            "Value=" + String((int)value) + "  wait..");
  String resp = httpGet(url);
  if (resp.length() > 0) {
    if (xSemaphoreTake(threshMutex, pdMS_TO_TICKS(500)) == pdTRUE) {
      int di = dryer - FIRST_SLAVE;
      if (isLower) lowerThresholds[di][chamber - 1] = value;
      else         thresholds[di][chamber - 1]      = value;
      xSemaphoreGive(threshMutex);
    }
    updateRelays();
    setStatusMsg("D" + String(dryer) + "C" + String(chamber) + tag
                 + "=" + String((int)value) + " Saved!", 2000UL);
    return true;
  }
  setStatusMsg("Save FAILED! Retry", 2000UL);
  return false;
}

// =============================================================
//  KEYPAD HANDLER  (Core 0)
// =============================================================
void processKey(char key) {
  Serial.printf("[Keypad] Key '%c' State:%d\n", key, (int)uiState);
  if (uiState == UI_WAITING_SAVE) return;

  if (uiState == UI_HOME && key == '0') {
    zeroPressCount++;
  } else {
    zeroPressCount = 0;
  }

  switch (uiState) {
    case UI_HOME:
      if (key >= '1' && key <= '6') {
        homeDisplayChannel = key - '0';
        char buf[17];
        snprintf(buf, sizeof(buf), "Channel %c shown", key);
        setStatusMsg(buf, 800UL);
      } else if (key == 'B') {
        infoMode   = INFO_IP;
        infoExpiry = millis() + 5000UL;
        Serial.println("[Keypad] Show IP for 5 s");
      } else if (key == 'D') {
        infoMode   = INFO_MAC;
        infoExpiry = millis() + 5000UL;
        Serial.println("[Keypad] Show MAC for 5 s");
      } else if (key == '0') {
        if (zeroPressCount >= 5) {
          zeroPressCount = 0;
          infoMode = INFO_NONE; // Cancel marquee
          
          // Swap pins
          int temp = RELAY_DRYER[0];
          RELAY_DRYER[0] = RELAY_DRYER[1];
          RELAY_DRYER[1] = temp;
          
          // Save configuration
          Preferences prefs;
          prefs.begin("dryer_cfg", false);
          prefs.putInt("relay5", RELAY_DRYER[0]);
          prefs.putInt("relay6", RELAY_DRYER[1]);
          prefs.end();
          
          Serial.printf("[Relays] Swapped pins: D5=%d, D6=%d\n", RELAY_DRYER[0], RELAY_DRYER[1]);
          
          char buf1[17];
          snprintf(buf1, sizeof(buf1), "D5:%d  D6:%d", RELAY_DRYER[0], RELAY_DRYER[1]);
          setDualStatusMsg("Pins Swapped!", buf1, 4000UL);
        }
      } else if (key == 'C') {
        keypadActive = true;
        uiState = UI_THRESH_DRYER;
        setLcdBuf("Select Dryer:", "5/6  *=Cancel");
      } else if (key == 'A') {
        keypadActive = true;
        pendingFetch = true;
        setLcdBuf("Fetch queued...", "Please wait...");
      }
      break;
    case UI_THRESH_DRYER:
      if (key == '5' || key == '6') {
        selectedDryer = key - '0';
        uiState = UI_THRESH_CHAMBER;
        setLcdBuf("Dryer " + String(selectedDryer) + " sel.", "Chamber 1-8 *=Bk");
      } else if (key == '*') {
        uiState = UI_HOME; keypadActive = false;
        setLcdBuf("Cancelled", "Back to Home");
      }
      break;
    case UI_THRESH_CHAMBER:
      if (key == '*') {
        uiState = UI_THRESH_DRYER;
        setLcdBuf("Select Dryer:", "5/6  *=Cancel");
      } else if (key >= '1' && key <= '8') {
        selectedChamber = key - '0'; inputValue = "";
        uiState = UI_THRESH_TYPE;
        char r0[17], r1[17];
        int di = selectedDryer - FIRST_SLAVE;
        snprintf(r0, sizeof(r0), "Up:%-4d  Lo:%-4d",
                 (int)thresholds[di][selectedChamber - 1],
                 (int)lowerThresholds[di][selectedChamber - 1]);
        snprintf(r1, sizeof(r1), "A=Upper  B=Lower");
        setLcdBuf(r0, r1);
      }
      break;
    case UI_THRESH_TYPE:
      if (key == '*') {
        uiState = UI_THRESH_CHAMBER;
        setLcdBuf("Dryer " + String(selectedDryer) + ":", "Chamber 1-8 *=Bk");
      } else if (key == 'A' || key == 'B') {
        selectedIsLower = (key == 'B'); inputValue = ""; uiState = UI_THRESH_VALUE;
        int di = selectedDryer - FIRST_SLAVE;
        float cur = selectedIsLower
                    ? lowerThresholds[di][selectedChamber - 1]
                    : thresholds[di][selectedChamber - 1];
        String tag = selectedIsLower ? "[Lo]" : "[Hi]";
        setLcdBuf("D" + String(selectedDryer) + "C" + String(selectedChamber)
                  + tag + ":" + String((int)cur), "New:_   #=Save");
      }
      break;
    case UI_THRESH_VALUE:
      if (key == '*') {
        if (inputValue.length() > 0) {
          inputValue = inputValue.substring(0, inputValue.length() - 1);
          setLcdRow(1, "New:" + inputValue + "_   #=Save");
        } else {
          uiState = UI_THRESH_TYPE;
          int di = selectedDryer - FIRST_SLAVE;
          char r0[17];
          snprintf(r0, sizeof(r0), "Up:%-4d  Lo:%-4d",
                   (int)thresholds[di][selectedChamber - 1],
                   (int)lowerThresholds[di][selectedChamber - 1]);
          setLcdBuf(r0, "A=Upper  B=Lower");
        }
      } else if (key == '#') {
        if (inputValue.length() == 0) {
          setLcdRow(1, "Enter value 1st!"); delay(800);
          setLcdRow(1, "New:_   #=Save");
        } else {
          float val = inputValue.toFloat();
          if (val < -50.0f || val > 1500.0f) {
            setLcdRow(1, "Out of range!   "); delay(1000);
            inputValue = "";
            setLcdRow(1, "New:_   #=Save");
          } else {
            pendingSave.dryer   = selectedDryer;
            pendingSave.chamber = selectedChamber;
            pendingSave.value   = val;
            pendingSave.isLower = selectedIsLower;
            pendingSave.pending = true;
            saveResultReady     = false;
            uiState             = UI_WAITING_SAVE;
            setLcdBuf("Saving threshold", "Please wait...");
          }
        }
      } else if (key >= '0' && key <= '9') {
        if (inputValue.length() < 5) {
          inputValue += key;
          setLcdRow(1, "New:" + inputValue + "_   #=Save");
        }
      }
      break;
    case UI_WAITING_SAVE:
      // blocking, keypad ignored
      break;
  }
}

// =============================================================
//  KEYPAD TASK  (Core 0)
// =============================================================
void keypadTask(void* param) {
  Serial.println("[Task] Keypad task started on Core 0");
  for (;;) {
    if (uiState == UI_WAITING_SAVE && saveResultReady) {
      saveResultReady = false; inputValue = "";
      uiState = UI_HOME; keypadActive = false;
    }
    char key = keypad.getKey();
    if (key) processKey(key);
    vTaskDelay(pdMS_TO_TICKS(20));
  }
}

// =============================================================
//  ETH EVENT HANDLER
// =============================================================
void onEth(arduino_event_id_t ev, arduino_event_info_t info) {
  switch (ev) {
    case ARDUINO_EVENT_ETH_START:
      Serial.println("[ETH] Started");
      ETH.setHostname("cpil-dryer56");
      break;
    case ARDUINO_EVENT_ETH_CONNECTED:
      Serial.println("[ETH] Cable connected");
      break;
    case ARDUINO_EVENT_ETH_GOT_IP:
      ethConnected = true;
      ethWasLost   = false;
      Serial.println("[ETH] IP:  " + ETH.localIP().toString());
      Serial.println("[ETH] MAC: " + ETH.macAddress());
      setStatusMsg("ETH: " + ETH.localIP().toString(), 4000UL);
      break;
    case ARDUINO_EVENT_ETH_DISCONNECTED:
    case ARDUINO_EVENT_ETH_LOST_IP:
      if (ethConnected) {
        ethConnected = false;
        ethWasLost   = true;
        ethLostAt    = millis();
        Serial.println("[ETH] Disconnected — 5 min watchdog started");
        setStatusMsg("ETH: Disconnected", 5000UL);
      }
      break;
    default: break;
  }
}

// =============================================================
//  LOOP TASK STACK SIZE
// =============================================================
SET_LOOP_TASK_STACK_SIZE(24576);

// =============================================================
//  MAIN LOOP TASK  — forward declaration
// =============================================================
void mainLoopTask(void* param);

// =============================================================
//  SETUP
// =============================================================
void setup() {
  // Silence MAX485 first
  pinMode(RS485_DE_PIN, OUTPUT);
  digitalWrite(RS485_DE_PIN, LOW);
  Serial.begin(115200);
#if ARDUINO_USB_CDC_ON_BOOT
  Serial.setTxTimeoutMs(0);
#endif
  for (int i = 0; i < 300 && !Serial; i++) delay(10);
  Serial.println("\n====================================================");
  Serial.println("  CPIL Chennai — Dryer 5 & 6 Monitor v2.4  (ETH)");
  Serial.println("  v2.4: Adafruit_NeoPixel status display on Core 0");
  Serial.println("  v2.3: mainLoopTask 24KB explicit stack (bypass loopTask)");
  Serial.println("  v2.2: heap WiFiClientSecure, SET_LOOP_TASK_STACK_SIZE");
  Serial.println("  v2.1: WDT delete, FORCE redirect, 500ms retry/mutex");
  Serial.println("====================================================");
  disableLoopWDT();
  Serial.println("[WDT] Loop task WDT disabled via disableLoopWDT()");
  
  // Mutexes
  lcdBufMutex = xSemaphoreCreateMutex();
  lcdHwMutex  = xSemaphoreCreateMutex();
  threshMutex = xSemaphoreCreateMutex();
  alertMutex  = xSemaphoreCreateMutex();
  configASSERT(lcdBufMutex);
  configASSERT(lcdHwMutex);
  configASSERT(threshMutex);
  configASSERT(alertMutex);
  
  memset(lcdBuf, ' ', sizeof(lcdBuf));
  lcdBuf[0][16] = lcdBuf[1][16] = '\0';
  Wire.begin(LCD_SDA_PIN, LCD_SCL_PIN);
  lcd.init();
  lcd.backlight();
  lcdHardwareDirect("CPIL DRYER TEMP ", "MONITORING SYS  ");
  Serial.println("[Setup] LCD init OK");
  delay(1000);
  lcd.clear();
  
  // Load relay pins from Preferences
  Preferences prefs;
  prefs.begin("dryer_cfg", false);
  int p5 = prefs.getInt("relay5", 1);
  int p6 = prefs.getInt("relay6", 38);
  prefs.end();
  // Validate pins are one of the allowed combinations: {1, 38} or {38, 1}
  if ((p5 == 1 && p6 == 38) || (p5 == 38 && p6 == 1)) {
    RELAY_DRYER[0] = p5;
    RELAY_DRYER[1] = p6;
  } else {
    RELAY_DRYER[0] = 1;
    RELAY_DRYER[1] = 38;
  }
  Serial.printf("[Setup] Configured RELAY_DRYER: D5=%d, D6=%d\n", RELAY_DRYER[0], RELAY_DRYER[1]);

  // Relays init
  pinMode(RELAY_HORN, OUTPUT); digitalWrite(RELAY_HORN, LOW);
  for (int i = 0; i < NUM_DRYERS; i++) {
    pinMode(RELAY_DRYER[i], OUTPUT);
    digitalWrite(RELAY_DRYER[i], HIGH);
  }
  Serial.println("[Setup] Relays init OK");
  
  // RS-485 init
  rs485Serial.end();
  digitalWrite(RS485_DE_PIN, HIGH);
  delayMicroseconds(100);
  rs485Serial.begin(RS485_BAUD, SERIAL_8N1, RS485_RX_PIN, RS485_TX_PIN);
  delayMicroseconds(200);
  digitalWrite(RS485_DE_PIN, LOW);
  delay(5);
  while (rs485Serial.available()) rs485Serial.read();
  Serial.println("[Setup] RS-485 init OK");
  
  for (int d = 0; d < NUM_DRYERS; d++) {
    hasValidReading[d] = false;
    dryerAlertHigh[d]  = false;
    dryerAlertLow[d]   = false;
    for (int c = 0; c < NUM_CHANNELS; c++) {
      thresholds[d][c]      = 200.0f;
      lowerThresholds[d][c] = 10.0f;
      temperatures[d][c]    = 0.0f;
      channelValid[d][c]    = false;
    }
  }
  lcdAlertCount      = 0;
  homeDisplayChannel = 1;
  infoMode           = INFO_NONE;
  
  // Create tasks
  xTaskCreatePinnedToCore(rgbTask,        "rgbTask",    2048,  NULL, 1, NULL,              0);
  xTaskCreatePinnedToCore(keypadTask,     "KeypadTask", 12288, NULL, 2, &keypadTaskHandle, 0);
  xTaskCreatePinnedToCore(relayBlinkTask, "RelayBlink", 3072,  NULL, 1, &relayTaskHandle,  0);
  xTaskCreatePinnedToCore(lcdTask,        "LCDTask",    9216,  NULL, 1, &lcdTaskHandle,    0);
  xTaskCreatePinnedToCore(neoTask,        "NeoTask",    4096,  NULL, 1, NULL,              0);
  Serial.println("[Setup] Core 0 tasks launched");
  
  Network.onEvent(onEth);
  ethSPI.begin(ETH_SCK_PIN, ETH_MISO_PIN, ETH_MOSI_PIN, ETH_CS_PIN);
  ETH.begin(ETH_PHY_W5500, 1, ETH_CS_PIN, ETH_INT_PIN, ETH_RST_PIN, ethSPI);
  lcdHardwareDirect("ETH Connecting..", "Please wait...  ");
  Serial.print("[ETH] Waiting for IP");
  uint32_t t0 = millis();
  while (!ethConnected && millis() - t0 < 20000) {
    Serial.print('.'); delay(500);
  }
  Serial.println();
  if (ethConnected) {
    String ipStr  = ETH.localIP().toString();
    String macStr = ETH.macAddress();
    Serial.println("[ETH] IP:  " + ipStr);
    Serial.println("[ETH] MAC: " + macStr);
    String scrollText = "MAC: " + macStr;
    int maxOffset = max(0, (int)scrollText.length() - 16);
    int offset = 0, dir = 1;
    unsigned long bootEnd = millis() + 6000;
    while (millis() < bootEnd) {
      lcdHardwareDirect(ipStr.c_str(),
                        scrollText.substring(offset, offset + 16).c_str());
      delay(400);
      if (maxOffset > 0) {
        offset += dir;
        if (offset >= maxOffset) { offset = maxOffset; dir = -1; }
        else if (offset <= 0)    { offset = 0;         dir =  1; }
      }
    }
  } else {
    lcdHardwareDirect("ETH: NO LINK    ", "Will retry...   ");
    Serial.println("[ETH] No IP. Watchdog will reboot after 5 min.");
    delay(2000);
    ethWasLost = true;
    ethLostAt  = millis();
  }
  readAllDryers();
  if (ethConnected) fetchThresholds();
  updateRelays();
  lastReadTime       = millis();
  lastUploadTime     = millis();
  lastThresholdFetch = millis();
  lastStatusFetch    = millis();
  Serial.printf("[Setup] Done. ETH: %s\n", ethConnected ? "OK" : "NO");
  lcdHardwareDirect("System READY    ",
                    ethConnected ? "ETH: OK         " : "ETH: NO (5m Rbt)");
  delay(2000);
  systemReady = true;
  Serial.println("[Setup] systemReady = true.");
  
  xTaskCreatePinnedToCore(
    mainLoopTask,   // task function
    "MainLoop",     // name
    24576,          // 24 KB stack
    NULL,           // parameter
    1,              // priority
    NULL,           // handle
    1               // Core 1
  );
  Serial.println("[Setup] MainLoop task (24 KB, Core 1) launched. loop() stub suspended.");
}

// =============================================================
//  LOOP STUB
// =============================================================
void loop() {
  vTaskSuspend(NULL);   // Suspend loopTask forever — mainLoopTask does the work
}

// =============================================================
//  MAIN LOOP TASK  (Core 1, 24 KB stack — created in setup())
// =============================================================
void mainLoopTask(void* param) {
  esp_task_wdt_delete(NULL);
  Serial.println("[MainLoop] Task WDT removed for MainLoop task.");
  for (;;) {
    unsigned long now = millis();
    if (ethWasLost && !ethConnected) {
      unsigned long elapsed = now - ethLostAt;
      if (elapsed >= ETH_FAIL_REBOOT_MS) {
        Serial.println("[ETH] No link for 5 min — rebooting!");
        lcdHardwareDirect("ETH lost 5 min! ", "Rebooting now...");
        delay(2000);
        ESP.restart();
      }
      static unsigned long lastCdLog = 0;
      if (now - lastCdLog >= 30000) {
        lastCdLog = now;
        char buf[17];
        snprintf(buf, sizeof(buf), "ETH gone %3ds..", (int)(elapsed / 1000));
        setStatusMsg(buf, 5000UL);
        Serial.printf("[ETH] Still no link. %lu s until reboot.\n",
                      (ETH_FAIL_REBOOT_MS - elapsed) / 1000);
      }
    }
    if (ethConnected) ethWasLost = false;
    if (pendingSave.pending) {
      pendingSave.pending = false;
      bool ok = saveThresholdToSheet(pendingSave.dryer, pendingSave.chamber,
                                     pendingSave.value, pendingSave.isLower);
      saveResultOk = ok; saveResultReady = true; keypadActive = false;
      delay(20); continue;
    }
    if (pendingFetch) {
      pendingFetch = false;
      keypadActive = false;
      uiState      = UI_HOME;
      fetchThresholds();
      delay(20); continue;
    }
    if (keypadActive) { delay(20); continue; }
    if (now - lastReadTime >= READ_INTERVAL) {
      lastReadTime = now;
      readAllDryers();
      updateRelays();
      delay(10);
    }
    if (ethConnected && now - lastUploadTime >= UPLOAD_INTERVAL) {
      lastUploadTime = now;
      Serial.println("[Upload] Starting upload cycle...");
      for (int d = 0; d < NUM_DRYERS; d++) {
        if (keypadActive) break;
        uploadDryer(d);
        delay(300);
      }
      Serial.printf("[Heap] Free: %u bytes\n", ESP.getFreeHeap());
    }
    if (ethConnected && now - lastThresholdFetch >= THRESHOLD_FETCH_INT) {
      lastThresholdFetch = now;
      fetchThresholds();
    }
    if (ethConnected && now - lastStatusFetch >= 30000UL) {
      lastStatusFetch = now;
      fetchStatusTimeout();
    }
    delay(20);
  }
}
