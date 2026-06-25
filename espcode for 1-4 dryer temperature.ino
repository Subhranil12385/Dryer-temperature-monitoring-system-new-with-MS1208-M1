#include <WiFi.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>
#include <ArduinoJson.h>
#include <Wire.h>
#include <LiquidCrystal_I2C.h>
#include <Keypad.h>

// ── Google Sheets Script URL ──────────────────────────────────
const char* SHEET_URL =
  "https://script.google.com/macros/s/AKfycbxWgDPqCGIZjUm18NBW1-mw2_rfi6E9ZMkv2-hl7o2jE0Ep5OdMPIMcOlxTCV92B68/exec";

// ── WiFi Networks ─────────────────────────────────────────────
struct WiFiCredential { const char* ssid; const char* password; };
WiFiCredential wifiNetworks[] = {
  { "SHARONSA", "sharon123#@" },
  { "SHARONBA", "sharon123#@" },
  { "Mi A3",    "123123123"   },
  { "Plylab",   "sharon123#@" }
};
const int NUM_WIFI = sizeof(wifiNetworks) / sizeof(wifiNetworks[0]);

// ── RS-485 Pins ───────────────────────────────────────────────
#define RS485_RX_PIN  5
#define RS485_TX_PIN  18
#define RS485_DE_PIN  19
HardwareSerial rs485Serial(2);

// ── I2C LCD ───────────────────────────────────────────────────
LiquidCrystal_I2C lcd(0x27, 16, 2);

// ── 4×4 Keypad ───────────────────────────────────────────────
const byte KP_ROWS = 4;
const byte KP_COLS = 4;
char keys[KP_ROWS][KP_COLS] = {
  {'1','2','3','A'},
  {'4','5','6','B'},
  {'7','8','9','C'},
  {'*','0','#','D'}
};
byte rowPins[KP_ROWS] = {13, 12, 14, 27};
byte colPins[KP_COLS] = {26, 25, 33, 32};
Keypad keypad = Keypad(makeKeymap(keys), rowPins, colPins, KP_ROWS, KP_COLS);

// ── Relay Pins ────────────────────────────────────────────────
#define RELAY_HORN        23
const int RELAY_DRYER[4] = {16, 15, 4, 2};

// ── Dryer / Modbus Config ─────────────────────────────────────
#define NUM_DRYERS    4
#define NUM_CHANNELS  8
#define TOTAL_CH      (NUM_DRYERS * NUM_CHANNELS)
#define RS485_BAUD    9600
#define START_REG     0x006E

// ── Timing ────────────────────────────────────────────────────
#define READ_INTERVAL         5000UL
#define UPLOAD_INTERVAL      60000UL
// ── FIXED: fetch has a 10-second HTTP timeout; 2 retries then reboot ──
#define THRESHOLD_FETCH_INT  20000UL
#define THRESHOLD_FETCH_RETRIES  2      // attempts before reboot
#define WIFI_RETRY_INT       30000UL
#define LCD_REFRESH_MS        250UL
#define LCD_ALERT_SWAP_MS    1000UL

#define WIFI_FAIL_REBOOT  3
#define HTTP_FAIL_REBOOT  3

#define BLINK_FAST_MS   500UL
#define BLINK_SLOW_MS  2000UL

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

// =============================================================
//  LCD DISPLAY BUFFER
// =============================================================
char lcdBuf[2][17];

char          statusMsg[17]  = "";
unsigned long statusExpiry   = 0;

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
AlertDisplay  lcdAlertList[TOTAL_CH * 2];
volatile int  lcdAlertCount = 0;

volatile bool keypadActive = false;

struct ThreshSaveRequest {
  volatile int   dryer;
  volatile int   chamber;
  volatile float value;
  volatile bool  isLower;
  volatile bool  pending;
};
volatile ThreshSaveRequest pendingSave     = { 0, 0, 0.0f, false, false };
volatile bool              pendingFetch    = false;
volatile bool              saveResultReady = false;
volatile bool              saveResultOk    = false;

bool wifiConnected = false;
int  wifiFailCount = 0;
int  httpFailCount = 0;

volatile int  statusTimeoutSec = 120;

unsigned long lastReadTime       = 0;
unsigned long lastUploadTime     = 0;
unsigned long lastThresholdFetch = 0;
unsigned long lastWifiRetry      = 0;
unsigned long lastStatusFetch    = 0;

enum UIState { UI_HOME, UI_THRESH_DRYER, UI_THRESH_CHAMBER,
               UI_THRESH_TYPE, UI_THRESH_VALUE, UI_WAITING_SAVE };
UIState uiState         = UI_HOME;
int     selectedDryer   = 0;
int     selectedChamber = 0;
bool    selectedIsLower = false;
String  inputValue      = "";

TaskHandle_t keypadTaskHandle = NULL;
TaskHandle_t relayTaskHandle  = NULL;
TaskHandle_t lcdTaskHandle    = NULL;

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
    padTo16(msg, statusMsg);
    statusExpiry = millis() + holdMs;
    xSemaphoreGive(lcdBufMutex);
  }
  Serial.printf("[Status] %s (%lu ms)\n", msg, holdMs);
}
void setStatusMsg(const String& msg, unsigned long holdMs) {
  setStatusMsg(msg.c_str(), holdMs);
}

// =============================================================
//  LCD HARDWARE WRITE (lcdTask Core 0 only — every 250 ms)
// =============================================================

void lcdHardwareFlush() {
  char r0[17], r1[17];
  if (xSemaphoreTake(lcdBufMutex, pdMS_TO_TICKS(30)) == pdTRUE) {
    memcpy(r0, lcdBuf[0], 17);
    memcpy(r1, lcdBuf[1], 17);
    xSemaphoreGive(lcdBufMutex);
  } else return;

  if (xSemaphoreTake(lcdHwMutex, pdMS_TO_TICKS(30)) == pdTRUE) {
    lcd.setCursor(0, 0); lcd.print(r0);
    lcd.setCursor(0, 1); lcd.print(r1);
    xSemaphoreGive(lcdHwMutex);
  }
}

// =============================================================
//  lcdHardwareDirect — safe from ANY core, ANY time
// =============================================================
void lcdHardwareDirect(const char* r0, const char* r1) {
  char b0[17], b1[17];
  padTo16(r0, b0);
  padTo16(r1, b1);

  if (xSemaphoreTake(lcdBufMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    memcpy(lcdBuf[0], b0, 17);
    memcpy(lcdBuf[1], b1, 17);
    xSemaphoreGive(lcdBufMutex);
  }

  if (xSemaphoreTake(lcdHwMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
    lcd.setCursor(0, 0); lcd.print(b0);
    lcd.setCursor(0, 1); lcd.print(b1);
    xSemaphoreGive(lcdHwMutex);
  }

  Serial.printf("[LCD] \"%s\" | \"%s\"\n", b0, b1);
}

// =============================================================
//  BUILD LCD CONTENT  (Core 0, every 250 ms)
// =============================================================

static int           alertIdx    = 0;
static bool          alertToggle = false;
static unsigned long alertSwapAt = 0;

void buildLCDContent() {
  if (!systemReady) return;
  if (keypadActive) return;

  unsigned long now = millis();

  float maxT[NUM_DRYERS] = {};
  for (int d = 0; d < NUM_DRYERS; d++)
    for (int c = 0; c < NUM_CHANNELS; c++)
      if (channelValid[d][c] && temperatures[d][c] > maxT[d])
        maxT[d] = temperatures[d][c];

  char r0[17];
  snprintf(r0, sizeof(r0), "D1:%3d%c D2:%3d%c",
           (int)maxT[0], (dryerAlertHigh[0]||dryerAlertLow[0]) ? '!' : ' ',
           (int)maxT[1], (dryerAlertHigh[1]||dryerAlertLow[1]) ? '!' : ' ');
  setLcdRow(0, r0);

  bool statusActive = false;
  char sBuf[17] = "";
  if (xSemaphoreTake(lcdBufMutex, pdMS_TO_TICKS(20)) == pdTRUE) {
    if (now < statusExpiry) {
      memcpy(sBuf, statusMsg, 17);
      statusActive = true;
    }
    xSemaphoreGive(lcdBufMutex);
  }
  if (statusActive) {
    setLcdRow(1, sBuf);
    return;
  }

  int alertCnt = lcdAlertCount;

  if (alertCnt > 0) {
    if (alertIdx >= alertCnt) alertIdx = 0;

    if (now >= alertSwapAt) {
      alertSwapAt = now + LCD_ALERT_SWAP_MS;
      alertToggle = !alertToggle;
      if (!alertToggle) alertIdx = (alertIdx + 1) % alertCnt;
    }

    AlertDisplay& a = lcdAlertList[alertIdx];
    char r1[17];
    if (!alertToggle) {
      snprintf(r1, sizeof(r1), "D%d Ch%d %s",
               a.dryer, a.chamber, a.isHigh ? "HIGH " : "LOW  ");
      setLcdRow(0, r1);
      snprintf(r1, sizeof(r1), "PV=%-4d SV=%-4d", (int)a.pv, (int)a.sv);
      setLcdRow(1, r1);
    } else {
      snprintf(r1, sizeof(r1), "D%d Ch%d %s",
               a.dryer, a.chamber, a.isHigh ? "HIGH " : "LOW  ");
      setLcdRow(0, r1);
      snprintf(r1, sizeof(r1), a.isHigh ? "Above threshold" : "Below threshold");
      setLcdRow(1, r1);
    }

  } else {
    char r1[17];
    snprintf(r1, sizeof(r1), "D3:%3d%c D4:%3d%c",
             (int)maxT[2], (dryerAlertHigh[2]||dryerAlertLow[2]) ? '!' : ' ',
             (int)maxT[3], (dryerAlertHigh[3]||dryerAlertLow[3]) ? '!' : ' ');
    setLcdRow(1, r1);
  }
}

// =============================================================
//  LCD TASK  (Core 0, priority 1)
// =============================================================

void lcdTask(void* param) {
  Serial.println("[Task] LCD task started on Core 0");
  for (;;) {
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
  delayMicroseconds(500);
  rs485Serial.write(req, 8);
  rs485Serial.flush();
  delayMicroseconds(500);
  digitalWrite(RS485_DE_PIN, LOW);

  uint8_t echo = 0;
  unsigned long t = millis();
  while (millis() - t < 1000 && echo < 8) {
    if (rs485Serial.available()) { rs485Serial.read(); echo++; }
    vTaskDelay(1);
  }
  if (echo < 8) { Serial.printf("[Modbus] Slave %d: echo timeout\n", slaveId); return false; }

  const uint8_t RLEN = 37;
  uint8_t resp[RLEN]; uint8_t idx = 0;
  t = millis();
  while (millis() - t < 1000 && idx < RLEN) {
    if (rs485Serial.available()) resp[idx++] = rs485Serial.read();
    vTaskDelay(1);
  }
  if (idx < RLEN) { Serial.printf("[Modbus] Slave %d: timeout\n", slaveId); return false; }

  uint16_t rCRC = ((uint16_t)resp[RLEN-1] << 8) | resp[RLEN-2];
  if (rCRC != crc16(resp, RLEN-2)) { Serial.printf("[Modbus] Slave %d: CRC err\n", slaveId); return false; }

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
  Serial.println("[Read] Reading all dryers...");
  for (int d = 0; d < NUM_DRYERS; d++) {
    if (keypadActive) return;

    if (!systemReady) {
      char buf[17];
      snprintf(buf, sizeof(buf), "Reading Dryer %d..", d + 1);
      lcdHardwareDirect("Reading Sensors", buf);
    }

    bool ok = readMS1208(d + 1, temperatures[d], channelValid[d]);
    if (ok) {
      hasValidReading[d] = true;
      Serial.printf("[Read] Dryer %d OK\n", d + 1);
    } else {
      Serial.printf("[Read] Dryer %d FAILED\n", d + 1);
      for (int c = 0; c < NUM_CHANNELS; c++) {
        temperatures[d][c] = 0; channelValid[d][c] = false;
      }
    }
    delay(20);
  }
}

// =============================================================
//  RELAY BLINK TASK  (Core 0)
// =============================================================

void relayBlinkTask(void* param) {
  bool          relayOn[NUM_DRYERS]    = {};
  unsigned long lastToggle[NUM_DRYERS] = {};
  bool          hornOn         = false;
  unsigned long hornLastToggle = 0;

  for (;;) {
    unsigned long now = millis();
    for (int d = 0; d < NUM_DRYERS; d++) {
      bool hi = dryerAlertHigh[d], lo = dryerAlertLow[d];
      if (!hi && !lo) {
        if (relayOn[d]) { relayOn[d] = false; digitalWrite(RELAY_DRYER[d], HIGH); }
        continue;
      }
      unsigned long iv = hi ? BLINK_FAST_MS : BLINK_SLOW_MS;
      if (now - lastToggle[d] >= iv) {
        lastToggle[d] = now; relayOn[d] = !relayOn[d];
        digitalWrite(RELAY_DRYER[d], relayOn[d] ? LOW : HIGH);
      }
    }
    bool anyHi = false, anyLo = false;
    for (int d = 0; d < NUM_DRYERS; d++) {
      if (dryerAlertHigh[d]) anyHi = true;
      if (dryerAlertLow[d])  anyLo = true;
    }
    if (!anyHi && !anyLo) {
      if (hornOn) { hornOn = false; digitalWrite(RELAY_HORN, LOW); }
    } else {
      unsigned long hiv = anyHi ? BLINK_FAST_MS : BLINK_SLOW_MS;
      if (now - hornLastToggle >= hiv) {
        hornLastToggle = now; hornOn = !hornOn;
        digitalWrite(RELAY_HORN, hornOn ? HIGH : LOW);
      }
    }
    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

// =============================================================
//  BUILD ALERT LIST  (Core 1)
// =============================================================

void buildAlertList() {
  int cnt = 0;
  for (int d = 0; d < NUM_DRYERS; d++) {
    for (int c = 0; c < NUM_CHANNELS; c++) {
      if (!channelValid[d][c] || temperatures[d][c] == 0.0f) continue;
      float t = temperatures[d][c];
      if (thresholds[d][c]      > 0.0f && t > thresholds[d][c])
        lcdAlertList[cnt++] = { d+1, c+1, thresholds[d][c], t, true  };
      if (lowerThresholds[d][c] > 0.0f && t < lowerThresholds[d][c])
        lcdAlertList[cnt++] = { d+1, c+1, lowerThresholds[d][c], t, false };
    }
  }
  lcdAlertCount = cnt;
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
    dryerAlertHigh[d] = hi;
    dryerAlertLow[d]  = lo;
  }
  buildAlertList();
}

// =============================================================
//  HTTP GET
// =============================================================

String httpGet(const String& url) {
  if (!wifiConnected) return "";
  WiFiClientSecure client; client.setInsecure();
  HTTPClient http;
  http.begin(client, url);
  http.setFollowRedirects(HTTPC_STRICT_FOLLOW_REDIRECTS);
  http.setTimeout(10000);
  int code = http.GET();
  String body = "";
  if (code == HTTP_CODE_OK) {
    body = http.getString(); httpFailCount = 0;
  } else {
    Serial.printf("[HTTP] Error: %d\n", code);
    if (++httpFailCount >= HTTP_FAIL_REBOOT) {
      setStatusMsg("HTTP err-rebooting", 3000UL);
      delay(3000); ESP.restart();
    }
  }
  http.end(); client.stop();
  return body;
}

// =============================================================
//  UPLOAD  (Core 1)
// =============================================================

bool uploadDryer(int dIdx) {
  if (keypadActive || !wifiConnected || !hasValidReading[dIdx]) return false;
  String url = String(SHEET_URL) + "?action=write&dryer=" + String(dIdx + 1);
  for (int c = 0; c < NUM_CHANNELS; c++) {
    float v = (channelValid[dIdx][c] && !isnan(temperatures[dIdx][c]))
              ? temperatures[dIdx][c] : 0.0f;
    url += "&t" + String(c+1) + "=" + String(v, 2);
  }
  String resp = httpGet(url);
  if (keypadActive) return false;
  if (resp.length() > 0) {
    setStatusMsg("D" + String(dIdx+1) + " Sent OK", 2000UL);
    return true;
  } else {
    setStatusMsg("D" + String(dIdx+1) + " FAILED!", 2000UL);
    return false;
  }
}

// =============================================================
//  THRESHOLD FETCH  (Core 1)
//
//  ROOT CAUSE OF OLD BUG:
//  httpGet() is synchronous and blocks for up to 10 s waiting
//  for a server response.  The old code used a flag
//  (thresholdFetchInProgress) that was supposed to be checked
//  by loop(), but loop() never gets to run while this function
//  is blocking — so the 10-second watchdog NEVER fired and the
//  fetch appeared to hang forever.
//
//  FIX:
//  The retry logic now lives entirely inside this function.
//  We attempt the fetch up to THRESHOLD_FETCH_RETRIES times
//  (each attempt already has a 10-second HTTP timeout via
//  http.setTimeout(10000) inside httpGet).  If every attempt
//  fails we reboot immediately — no external watchdog needed.
// =============================================================

void fetchThresholds() {
  if (keypadActive || !wifiConnected) return;

  Serial.printf("[Thresh] Fetching thresholds (max %d attempts)...\n",
                THRESHOLD_FETCH_RETRIES);

  for (int attempt = 1; attempt <= THRESHOLD_FETCH_RETRIES; attempt++) {

    // ── Show progress on LCD ──
    char attemptBuf[17];
    snprintf(attemptBuf, sizeof(attemptBuf), "Try %d/%d ...",
             attempt, THRESHOLD_FETCH_RETRIES);

    if (!systemReady)
      lcdHardwareDirect("Fetching thresh.", attemptBuf);
    else
      setStatusMsg(attemptBuf, 12000UL);   // hold until replaced

    Serial.printf("[Thresh] Attempt %d/%d — sending request...\n",
                  attempt, THRESHOLD_FETCH_RETRIES);

    String payload = httpGet(String(SHEET_URL) + "?action=getThresholds");

    // Abort early if the keypad grabbed control mid-fetch
    if (keypadActive) {
      Serial.println("[Thresh] Aborted — keypad active");
      return;
    }

    // ── Empty response = network/server failure ──
    if (payload.length() == 0) {
      Serial.printf("[Thresh] Attempt %d failed: empty response\n", attempt);
      char failBuf[17];
      snprintf(failBuf, sizeof(failBuf), "Fail %d/%d...",
               attempt, THRESHOLD_FETCH_RETRIES);
      setStatusMsg(failBuf, 1500UL);
      if (!systemReady) lcdHardwareDirect("Thresh fetch err", failBuf);
      delay(1000);   // brief pause before next attempt
      continue;
    }

    // ── Parse JSON ──
    JsonDocument doc;
    if (deserializeJson(doc, payload) || !doc["thresholds"].is<JsonObject>()) {
      Serial.printf("[Thresh] Attempt %d failed: JSON parse error\n", attempt);
      char parseBuf[17];
      snprintf(parseBuf, sizeof(parseBuf), "ParseErr %d/%d",
               attempt, THRESHOLD_FETCH_RETRIES);
      setStatusMsg(parseBuf, 1500UL);
      if (!systemReady) lcdHardwareDirect("Thresh parse err", parseBuf);
      delay(1000);
      continue;
    }

    // ── Success: update threshold arrays ──
    if (xSemaphoreTake(threshMutex, pdMS_TO_TICKS(500)) == pdTRUE) {
      JsonObject thresh = doc["thresholds"].as<JsonObject>();
      for (int d = 1; d <= NUM_DRYERS; d++) {
        String dk = "D" + String(d);
        if (!thresh[dk].is<JsonObject>()) continue;
        JsonObject dObj = thresh[dk].as<JsonObject>();
        for (int c = 1; c <= NUM_CHANNELS; c++) {
          String ck = "C" + String(c);
          if (!dObj[ck].isNull()) thresholds[d-1][c-1] = dObj[ck].as<float>();
        }
      }
      if (doc["lowerThresholds"].is<JsonObject>()) {
        JsonObject lt = doc["lowerThresholds"].as<JsonObject>();
        for (int d = 1; d <= NUM_DRYERS; d++) {
          String dk = "D" + String(d);
          if (!lt[dk].is<JsonObject>()) continue;
          JsonObject dObj = lt[dk].as<JsonObject>();
          for (int c = 1; c <= NUM_CHANNELS; c++) {
            String ck = "C" + String(c);
            if (!dObj[ck].isNull()) lowerThresholds[d-1][c-1] = dObj[ck].as<float>();
          }
        }
      }
      xSemaphoreGive(threshMutex);
    }

    updateRelays();
    Serial.printf("[Thresh] Updated successfully on attempt %d\n", attempt);
    setStatusMsg("Thresh updated!", 2000UL);
    return;   // ← done, no reboot needed
  }

  // ── All THRESHOLD_FETCH_RETRIES attempts failed → reboot ──
  Serial.printf("[Thresh] All %d attempts failed — rebooting!\n",
                THRESHOLD_FETCH_RETRIES);

  const char* r0 = "Thresh FAILED!";
  char r1[17];
  snprintf(r1, sizeof(r1), "Rebooting (%ds)...", 3);
  setStatusMsg("Thresh fail-rebt!", 4000UL);

  if (!systemReady)
    lcdHardwareDirect(r0, r1);
  else
    lcdHardwareDirect(r0, r1);   // always show reboot message

  delay(3000);
  ESP.restart();
}

// =============================================================
//  FETCH STATUS TIMEOUT
// =============================================================

void fetchStatusTimeout() {
  if (keypadActive || !wifiConnected) return;
  String payload = httpGet(String(SHEET_URL) + "?action=getStatusTimeout");
  if (payload.length() == 0) return;
  JsonDocument doc;
  if (!deserializeJson(doc, payload) && !doc["timeoutSeconds"].isNull()) {
    int val = doc["timeoutSeconds"].as<int>();
    if (val >= 10 && val <= 3600) statusTimeoutSec = val;
  }
}

// =============================================================
//  THRESHOLD SAVE  (Core 1, user-triggered)
// =============================================================

bool saveThresholdToSheet(int dryer, int chamber, float value, bool isLower) {
  if (!wifiConnected) {
    setStatusMsg("No WiFi-cant save", 2000UL); return false;
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
      if (isLower) lowerThresholds[dryer-1][chamber-1] = value;
      else         thresholds[dryer-1][chamber-1]      = value;
      xSemaphoreGive(threshMutex);
    }
    updateRelays();
    setStatusMsg("D" + String(dryer) + "C" + String(chamber) + tag
                 + "=" + String((int)value) + " Saved!", 2000UL);
    return true;
  } else {
    setStatusMsg("Save FAILED! Retry", 2000UL);
    return false;
  }
}

// =============================================================
//  connectWiFi
// =============================================================
bool connectWiFi() {
  Serial.println("[WiFi] Starting connection attempts...");
  WiFi.mode(WIFI_STA); WiFi.disconnect(true); delay(200);

  for (int n = 0; n < NUM_WIFI; n++) {
    Serial.printf("[WiFi] Trying %d/%d  SSID: \"%s\"\n",
                  n + 1, NUM_WIFI, wifiNetworks[n].ssid);

    lcdHardwareDirect("WiFi Connecting ", wifiNetworks[n].ssid);

    WiFi.begin(wifiNetworks[n].ssid, wifiNetworks[n].password);
    int att = 0;
    while (WiFi.status() != WL_CONNECTED && att < 20) {
      delay(500);
      att++;
      char progress[17];
      snprintf(progress, sizeof(progress), "Connecting.. %2ds", att / 2);
      lcdHardwareDirect(progress, wifiNetworks[n].ssid);
      Serial.printf("[WiFi]   %s  attempt %d/20\n", wifiNetworks[n].ssid, att);
    }

    if (WiFi.status() == WL_CONNECTED) {
      String ip = WiFi.localIP().toString();
      Serial.printf("[WiFi] Connected!  SSID: \"%s\"  IP: %s\n",
                    wifiNetworks[n].ssid, ip.c_str());
      lcdHardwareDirect("WiFi Connected! ", ip.c_str());
      delay(2000);
      wifiFailCount = 0;
      httpFailCount = 0;
      return true;
    }

    Serial.printf("[WiFi] Failed: \"%s\"\n", wifiNetworks[n].ssid);
    lcdHardwareDirect("WiFi Failed:    ", wifiNetworks[n].ssid);
    delay(1000);
    WiFi.disconnect(true);
    delay(200);
  }

  wifiFailCount++;
  Serial.printf("[WiFi] ALL networks failed.  Count: %d / %d\n",
                wifiFailCount, WIFI_FAIL_REBOOT);

  char buf[17];
  snprintf(buf, sizeof(buf), "Fail %d/%d Rbt@%d",
           wifiFailCount, WIFI_FAIL_REBOOT, WIFI_FAIL_REBOOT);
  lcdHardwareDirect("WiFi ALL FAILED!", buf);
  delay(3000);

  if (wifiFailCount >= WIFI_FAIL_REBOOT) {
    Serial.println("[WiFi] Reboot threshold reached — restarting...");
    lcdHardwareDirect("WiFi x3 FAILED!", "Rebooting now..");
    delay(2000);
    ESP.restart();
  }
  return false;
}

// =============================================================
//  KEYPAD HANDLER  (Core 0)
// =============================================================

void processKey(char key) {
  Serial.printf("[Keypad] Key '%c' State:%d\n", key, (int)uiState);
  if (uiState == UI_WAITING_SAVE) return;

  switch (uiState) {

    case UI_HOME:
      if (key == 'C') {
        keypadActive = true;
        uiState = UI_THRESH_DRYER;
        setLcdBuf("Select Dryer:", "1/2/3/4  *=Cancel");
      } else if (key == 'A') {
        keypadActive = true;
        pendingFetch = true;
        setLcdBuf("Fetch queued...", "Please wait...");
      }
      break;

    case UI_THRESH_DRYER:
      if (key >= '1' && key <= '4') {
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
        setLcdBuf("Select Dryer:", "1/2/3/4  *=Cancel");
      } else if (key >= '1' && key <= '8') {
        selectedChamber = key - '0'; inputValue = "";
        uiState = UI_THRESH_TYPE;
        char r0[17], r1[17];
        snprintf(r0, sizeof(r0), "Up:%-4d  Lo:%-4d",
                 (int)thresholds[selectedDryer-1][selectedChamber-1],
                 (int)lowerThresholds[selectedDryer-1][selectedChamber-1]);
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
        float cur = selectedIsLower
                    ? lowerThresholds[selectedDryer-1][selectedChamber-1]
                    : thresholds[selectedDryer-1][selectedChamber-1];
        String tag = selectedIsLower ? "[Lo]" : "[Hi]";
        setLcdBuf("D" + String(selectedDryer) + "C" + String(selectedChamber)
                  + tag + ":" + String((int)cur), "New:_   #=Save");
      }
      break;

    case UI_THRESH_VALUE:
      if (key == '*') {
        if (inputValue.length() > 0) {
          inputValue = inputValue.substring(0, inputValue.length()-1);
          setLcdRow(1, "New:" + inputValue + "_   #=Save");
        } else {
          uiState = UI_THRESH_TYPE;
          char r0[17];
          snprintf(r0, sizeof(r0), "Up:%-4d  Lo:%-4d",
                   (int)thresholds[selectedDryer-1][selectedChamber-1],
                   (int)lowerThresholds[selectedDryer-1][selectedChamber-1]);
          setLcdBuf(r0, "A=Upper  B=Lower");
        }
      } else if (key == '#') {
        if (inputValue.length() == 0) {
          setLcdRow(1, "Enter value 1st!"); delay(800);
          setLcdRow(1, "New:_   #=Save");
        } else {
          pendingSave.dryer    = selectedDryer;
          pendingSave.chamber  = selectedChamber;
          pendingSave.value    = inputValue.toFloat();
          pendingSave.isLower  = selectedIsLower;
          pendingSave.pending  = true;
          saveResultReady      = false;
          uiState = UI_WAITING_SAVE;
          String tag = selectedIsLower ? "[Lo]" : "[Hi]";
          setLcdBuf("Saving D" + String(selectedDryer) + "C" + String(selectedChamber) + tag,
                    "Sending to sheet");
        }
      } else if (key >= '0' && key <= '9') {
        if ((int)inputValue.length() < 4) {
          inputValue += key;
          setLcdRow(1, "New:" + inputValue + "_   #=Save");
        } else {
          setLcdRow(1, "Max 4 digits!"); delay(500);
          setLcdRow(1, "New:" + inputValue + "   #=Save");
        }
      }
      break;

    default: break;
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
//  SETUP
// =============================================================

void setup() {
  Serial.begin(115200); delay(500);
  Serial.println("\n====================================================");
  Serial.println("  CPIL Chennai — 4-Dryer 8-Ch Monitor v9.3");
  Serial.println("  Threshold fetch: 2 retries x 10s then reboot");
  Serial.println("====================================================");

  lcdBufMutex = xSemaphoreCreateMutex();
  lcdHwMutex  = xSemaphoreCreateMutex();
  threshMutex = xSemaphoreCreateMutex();
  configASSERT(lcdBufMutex);
  configASSERT(lcdHwMutex);
  configASSERT(threshMutex);
  Serial.println("[Setup] Mutexes created OK");

  memset(lcdBuf, ' ', sizeof(lcdBuf));
  lcdBuf[0][16] = lcdBuf[1][16] = '\0';

  Wire.begin(21, 22);
  lcd.init();
  lcd.backlight();

  lcdHardwareDirect("CPIL DRYER TEMP ", "MONITORING SYS  ");
  Serial.println("[Setup] LCD init OK — boot splash");
  delay(1000);

  lcd.clear();
  Serial.println("[Setup] Boot splash done");

  pinMode(RELAY_HORN, OUTPUT); digitalWrite(RELAY_HORN, LOW);
  for (int i = 0; i < NUM_DRYERS; i++) {
    pinMode(RELAY_DRYER[i], OUTPUT);
    digitalWrite(RELAY_DRYER[i], HIGH);
  }
  Serial.println("[Setup] Relays init OK");

  lcdHardwareDirect("Starting...     ", "Relay Self-Test.");
  Serial.println("[Setup] Relay self-test...");
  digitalWrite(RELAY_HORN, HIGH);
  for (int i = 0; i < NUM_DRYERS; i++) digitalWrite(RELAY_DRYER[i], LOW);
  delay(400);
  digitalWrite(RELAY_HORN, LOW);
  for (int i = 0; i < NUM_DRYERS; i++) digitalWrite(RELAY_DRYER[i], HIGH);
  lcdHardwareDirect("Starting...     ", "Relay Test Done ");
  Serial.println("[Setup] Relay self-test done");
  delay(800);

  pinMode(RS485_DE_PIN, OUTPUT); digitalWrite(RS485_DE_PIN, LOW);
  rs485Serial.begin(RS485_BAUD, SERIAL_8N1, RS485_RX_PIN, RS485_TX_PIN);
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
  lcdAlertCount = 0;
  Serial.println("[Setup] Arrays init OK");

  xTaskCreatePinnedToCore(keypadTask,     "KeypadTask", 8192, NULL, 2, &keypadTaskHandle, 0);
  xTaskCreatePinnedToCore(relayBlinkTask, "RelayBlink", 4096, NULL, 3, &relayTaskHandle,  0);
  xTaskCreatePinnedToCore(lcdTask,        "LCDTask",    4096, NULL, 1, &lcdTaskHandle,    0);
  Serial.println("[Setup] Core 0 tasks launched");

  wifiConnected = connectWiFi();

  readAllDryers();

  // fetchThresholds() will now retry 2x then reboot if both fail
  if (wifiConnected) fetchThresholds();
  updateRelays();

  lastReadTime       = millis();
  lastUploadTime     = millis();
  lastThresholdFetch = millis();
  lastStatusFetch    = millis();

  Serial.printf("[Setup] Done. WiFi: %s\n", wifiConnected ? "OK" : "NO");
  lcdHardwareDirect("System READY    ", wifiConnected ? "WiFi: OK        " : "WiFi: NO (retry)");
  delay(2000);

  systemReady = true;
  Serial.println("[Setup] systemReady = true. Main loop starting...");
}

// =============================================================
//  MAIN LOOP  (Core 1)
// =============================================================

void loop() {
  unsigned long now = millis();

  // Pending save (from keypad)
  if (pendingSave.pending) {
    pendingSave.pending = false;
    bool ok = saveThresholdToSheet(pendingSave.dryer, pendingSave.chamber,
                                   pendingSave.value, pendingSave.isLower);
    saveResultOk = ok; saveResultReady = true; keypadActive = false;
    return;
  }

  // Pending fetch (from keypad 'A' key)
  if (pendingFetch) {
    pendingFetch = false;
    keypadActive = false;
    uiState      = UI_HOME;
    fetchThresholds();
    return;
  }

  // Keypad active — Core 1 idles
  if (keypadActive) { delay(20); return; }

  // WiFi check
  bool currWifi = (WiFi.status() == WL_CONNECTED);
  if (currWifi != wifiConnected) {
    wifiConnected = currWifi;
    Serial.printf("[WiFi] Status changed: %s\n",
                  wifiConnected ? "connected" : "lost");
    setStatusMsg(wifiConnected ? "WiFi reconnected" : "WiFi lost!",
                 wifiConnected ? 2000UL : 3000UL);
    if (wifiConnected) { wifiFailCount = 0; httpFailCount = 0; }
  }
  if (!wifiConnected && now - lastWifiRetry >= WIFI_RETRY_INT) {
    lastWifiRetry = now;
    Serial.println("[WiFi] Retrying connection...");
    wifiConnected = connectWiFi();
  }

  // Sensor read
  if (now - lastReadTime >= READ_INTERVAL) {
    lastReadTime = now;
    readAllDryers();
    updateRelays();
  }

  // Upload
  if (wifiConnected && now - lastUploadTime >= UPLOAD_INTERVAL) {
    lastUploadTime = now;
    Serial.println("[Upload] Starting upload cycle...");
    for (int d = 0; d < NUM_DRYERS; d++) {
      if (keypadActive) break;
      uploadDryer(d);
      delay(300);
    }
    Serial.printf("[Heap] Free: %u bytes\n", ESP.getFreeHeap());
  }

  // Threshold fetch (fetchThresholds handles its own retry+reboot)
  if (wifiConnected && now - lastThresholdFetch >= THRESHOLD_FETCH_INT) {
    lastThresholdFetch = now;
    fetchThresholds();
  }

  // Status timeout fetch
  if (wifiConnected && now - lastStatusFetch >= 30000UL) {
    lastStatusFetch = now;
    fetchStatusTimeout();
  }

  delay(20);
}
