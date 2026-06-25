/*
 * Veneer Moisture Monitoring System
 * Target: ESP32 (Normal Dev Module)
 *
 * Architecture:
 *  - Core 1 (Main Loop): Samples ADC every 50ms. Every 5 seconds (100 samples),
 *                        computes trimmed mean, calculates moisture locally using
 *                        cached calibration coefficients, updates LCD, pushes ADC
 *                        average to upload queue.
 *  - Core 0 (Upload Task): Picks up latest ADC average and uploads to Google Sheets.
 *                          Parses updated calibration coefficients from each response.
 *  - sendSecureRequest(): Raw WiFiClientSecure — NO HTTPClient. Calls connect(host)
 *                         so SNI is always set correctly. Follows full redirect chain
 *                         with a fresh client object each hop. Returns 200 only on
 *                         actual success — redirects are never returned as "OK".
 *  - xQueueOverwrite: Main loop never blocks.
 */

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <Wire.h>
#include <LiquidCrystal_I2C.h>

// Struct declared early so auto-generated Arduino prototypes can use it
struct ParsedURL { String host; String pathAndQuery; };




// ==========================================
// CONFIGURATION
// ==========================================
const char* ssid     = "ciscosa";
const char* password = "Abc123#@";
const char* sharon_password = "sharon123#@";

const char* GAS_URL        = "https://script.google.com/macros/s/AKfycbymz36y159wSD6Z8Jbe0o_61FNf4a3Nc1OSDONzJVbUkxrQAgoz6hVmpe_-CcVHwTMW/exec";
const char* SHEET_CSV_HOST = "docs.google.com";
const char* SHEET_ID       = "12QqTI3isHwNFfqKyIERWwwGrMhvpIx8DibwWP3N5kJI";
const char* CAL_SHEET      = "Moisture Calibration";
const char* TEMP_CAL_SHEET = "Temperature Calibration";
const char* SETTINGS_SHEET = "Settings";
// ==========================================

// ==========================================
// MODBUS RTU RS485 CONFIGURATION
// ==========================================
#define RX2_PIN      16  // Connected to MAX485 RO/RX pin (D16 / RX2)
#define TX2_PIN      17  // Connected to MAX485 DI/TX pin (D17 / TX2)
#define DE_RE_PIN    4   // Connected to MAX485 DE and RE pins (joined) (D4)

const uint8_t  MODBUS_SLAVE_ID     = 1;      // Default address of Waveshare Modbus module
const uint16_t MODBUS_START_REG    = 0x0000; // Register for Channel 1 (0x0000 to 0x0007 for Ch 1-8)
const float    TEMP_MIN            = 0.0f;   // Minimum temperature of the sensor (e.g. 0°C)
const float    TEMP_MAX            = 300.0f; // Maximum temperature of the sensor (e.g. 300°C)

// Waveshare module version and signal settings:
// 0 = 4-20mA on Current Input module (Version A), reads uA (4000 to 20000 uA)
// 1 = 0-5V on Voltage Input module (Version B), reads mV (0 to 5000 mV)
// 2 = 1-5V on Voltage Input module (Version B), reads mV (1000 to 5000 mV)
const int      SENSOR_SIGNAL_TYPE  = 0; 
// ==========================================


const int SENSOR_PIN         = 33; // D33 Brown wire from moisture filter system
const int RESISTOR_PIN       = 25; // D25 pin connected to the pull-up resistor
const int PROXIMITY_PIN      = 19; // D19 pin connected to proximity sensor output
const int SAMPLE_COUNT       = 100;
const int SAMPLE_INTERVAL_MS = 50;

int readings[SAMPLE_COUNT];
int readingIndex = 0;

LiquidCrystal_I2C lcd(0x27, 16, 2);
volatile bool pendingPing = false;
volatile bool pendingPingState = false;

volatile bool pendingTelemetry = false;
volatile int pendingMoistureAdc = 0;
volatile float pendingMaVal = -1.0f;
volatile float pendingTemperature = -999.0f;
SemaphoreHandle_t i2cMutex = NULL;

// Calibration coefficients — updated at boot via CSV fetch and after each upload
float cal_a = 0.0f;
float cal_b = 0.1f;
float cal_c = 0.0f;
bool resistorActive = false;
float lastAvgMoisture = -1.0f;

// Temperature calibration coefficients — updated at boot via CSV fetch
float temp_cal_b = 18.75f; // Default slope (mA to C)
float temp_cal_c = -75.0f; // Default intercept (C)
float temp_cal_r2 = 1.0f;
String temp_cal_type = "Default Formula";
float lastAvgTemperature = -999.0f;
unsigned long lastDetectMs = 0;
bool displayIsNA = false;
unsigned long veneerGapMs = 5000; // Default to 5 seconds (5000 ms)
byte solidBlock[8] = {
  0x1F, 0x1F, 0x1F, 0x1F, 0x1F, 0x1F, 0x1F, 0x1F
};

// State variables for status pings
bool lastDetectingState = false;
unsigned long lastPingMs = 0;
const unsigned long PING_INTERVAL_MS = 10000; // 10 seconds keep-alive


void setResistorState(bool active) {
  resistorActive = active;
  if (active) {
    pinMode(RESISTOR_PIN, OUTPUT);
    digitalWrite(RESISTOR_PIN, HIGH);
    Serial.println("[Resistor] Active (HIGH)");
  } else {
    pinMode(RESISTOR_PIN, INPUT); // High-impedance, floating
    Serial.println("[Resistor] Inactive (INPUT/Floating)");
  }

  // Write R1 or R0 at the top-right corner (columns 14-15 of row 0)
  if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
    lcd.setCursor(14, 0);
    if (active) {
      lcd.print("R1");
    } else {
      lcd.print("R0");
    }
    xSemaphoreGive(i2cMutex);
  } else if (i2cMutex == NULL) {
    lcd.setCursor(14, 0);
    if (active) {
      lcd.print("R1");
    } else {
      lcd.print("R0");
    }
  }
}

// Re-initializes LCD and redraws the layout to self-heal from I2C noise/corruption
// Re-initializes LCD and redraws the layout to self-heal from I2C noise/corruption
void refreshLCD() {
  if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
    lcd.init();
    lcd.backlight();
    lcd.createChar(0, solidBlock);

    // Redraw resistor state at top-right
    lcd.setCursor(14, 0);
    if (resistorActive) {
      lcd.print("R1");
    } else {
      lcd.print("R0");
    }

    // Determine if we should show NA (idle for >= veneerGapMs)
    bool showNA = (digitalRead(PROXIMITY_PIN) == HIGH) && (millis() - lastDetectMs >= veneerGapMs);

    // Redraw temperature at row 0 (exactly 12 characters)
    char tempBuf[13];
    if (showNA) {
      strcpy(tempBuf, "TEMP:   NA  ");
    } else if (lastAvgTemperature < -990.0f) {
      strcpy(tempBuf, "TEMP:  --.-C");
    } else if (lastAvgTemperature < 10.0f) {
      sprintf(tempBuf, "TEMP:   %.1fC", lastAvgTemperature);
      tempBuf[12] = '\0';
    } else if (lastAvgTemperature < 100.0f) {
      sprintf(tempBuf, "TEMP:  %.1fC", lastAvgTemperature);
      tempBuf[12] = '\0';
    } else {
      sprintf(tempBuf, "TEMP: %.1fC", lastAvgTemperature);
      tempBuf[12] = '\0';
    }
    lcd.setCursor(0, 0);
    lcd.print(tempBuf);

    // Redraw moisture at row 1 (exactly 12 characters)
    char moistBuf[13];
    if (showNA) {
      strcpy(moistBuf, "MOIST:  NA  ");
    } else if (lastAvgMoisture < 0.0f) {
      strcpy(moistBuf, "MOIST:  --% ");
    } else if (lastAvgMoisture < 10.0f) {
      sprintf(moistBuf, "MOIST:  %.1f%%", lastAvgMoisture);
      moistBuf[12] = '\0';
    } else {
      sprintf(moistBuf, "MOIST: %.1f%%", lastAvgMoisture);
      moistBuf[12] = '\0';
    }
    lcd.setCursor(0, 1);
    lcd.print(moistBuf);

    // Redraw suffix
    lcd.setCursor(12, 1);
    if (digitalRead(PROXIMITY_PIN) == LOW) {
      lcd.print("\x08\x08\x08 "); // temporary placeholder while animating in main loop
    } else {
      lcd.print("  P0");
    }

    xSemaphoreGive(i2cMutex);
    Serial.println("[LCD] Self-heal: re-init & redraw completed.");
  }
}

void updateDisplayIdle() {
  bool showNA = (millis() - lastDetectMs >= veneerGapMs);
  
  if (showNA && !displayIsNA) {
    displayIsNA = true;
    refreshLCD();
    return;
  }
  
  if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, pdMS_TO_TICKS(50)) == pdTRUE) {
    lcd.setCursor(12, 1);
    lcd.print("  P0");
    xSemaphoreGive(i2cMutex);
  } else if (i2cMutex == NULL) {
    lcd.setCursor(12, 1);
    lcd.print("  P0");
  }
}

void updateDisplayActive() {
  static unsigned long lastAnimMs = 0;
  static int animState = 0;
  
  if (millis() - lastAnimMs >= 250) {
    lastAnimMs = millis();
    animState = (animState + 1) % 4;
    
    const char* frames[4] = {
      "\x08   ",
      "\x08\x08  ",
      "\x08\x08\x08 ",
      "\x08\x08\x08\x08"
    };
    
    if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, pdMS_TO_TICKS(50)) == pdTRUE) {
      lcd.setCursor(12, 1);
      lcd.print(frames[animState]);
      xSemaphoreGive(i2cMutex);
    } else if (i2cMutex == NULL) {
      lcd.setCursor(12, 1);
      lcd.print(frames[animState]);
    }
  }
}
// No cal_type string — quadratic is used when |cal_a| > 1e-10, linear otherwise

// Watchdog variables
unsigned long lastMainLoopFeed     = 0;
unsigned long lastUploadTaskActive = 0;
int consecutiveHttpFailures  = 0;
int consecutiveWiFiFailures  = 0;
unsigned long lastWiFiConnectedTime = 0;




// ================================================================
// MOISTURE CALCULATION
// ================================================================
float calculateMoisture(int adcVal) {
  float p = (fabsf(cal_a) > 1e-10f)
            ? cal_a * adcVal * adcVal + cal_b * adcVal + cal_c  // quadratic
            : cal_b * adcVal + cal_c;                           // linear
  if (p < 0.0f)   p = 0.0f;
  if (p > 100.0f) p = 100.0f;
  return p;
}

// ================================================================
// JSON HELPERS
// ================================================================
float parseJsonFloat(const String& payload, const String& key) {
  int idx = payload.indexOf("\"" + key + "\":");
  if (idx == -1) return 0.0f;
  int start = idx + key.length() + 3;
  if (payload.charAt(start) == '"') start++;
  int end = start;
  while (end < (int)payload.length() &&
         payload.charAt(end) != '"' &&
         payload.charAt(end) != ',' &&
         payload.charAt(end) != '}') end++;
  return payload.substring(start, end).toFloat();
}

String parseJsonString(const String& payload, const String& key) {
  int idx = payload.indexOf("\"" + key + "\":");
  if (idx == -1) return "";
  int start = idx + key.length() + 3;
  if (payload.charAt(start) == '"') start++;
  int end = start;
  while (end < (int)payload.length() &&
         payload.charAt(end) != '"' &&
         payload.charAt(end) != ',' &&
         payload.charAt(end) != '}') end++;
  return payload.substring(start, end);
}

void parseCoefficients(const String& payload) {
  if (payload.indexOf("\"cal_b\":") == -1) return;
  cal_a = parseJsonFloat(payload, "cal_a");
  cal_b = parseJsonFloat(payload, "cal_b");
  cal_c = parseJsonFloat(payload, "cal_c");

  String mode = parseJsonString(payload, "resistor_mode");
  if (mode.length() > 0) {
    bool withResistor = (mode.indexOf("With resistor") != -1);
    setResistorState(withResistor);
  }

  Serial.printf("[Coeff] a=%e  b=%f  c=%f  (quadratic=%s)\n",
                cal_a, cal_b, cal_c, fabsf(cal_a) > 1e-10f ? "yes" : "no");
  Serial.printf("[Coeff] ADC=128 → moisture=%.1f%%\n", calculateMoisture(128));
}

// ================================================================
// URL PARSER
// ================================================================

ParsedURL parseURL(const String& url) {
  ParsedURL r;
  int start = url.startsWith("https://") ? 8 : (url.startsWith("http://") ? 7 : 0);
  int slash  = url.indexOf('/', start);
  if (slash == -1) { r.host = url.substring(start); r.pathAndQuery = "/"; }
  else             { r.host = url.substring(start, slash); r.pathAndQuery = url.substring(slash); }
  return r;
}

// Decode chunked transfer encoding payload
String decodeChunked(const String& input) {
  String decoded = "";
  int len = input.length();
  int pos = 0;
  while (pos < len) {
    int nextLine = input.indexOf("\n", pos);
    if (nextLine == -1) break;
    String sizeLine = input.substring(pos, nextLine);
    sizeLine.trim();
    long chunkSize = strtol(sizeLine.c_str(), NULL, 16);
    pos = nextLine + 1;
    if (chunkSize == 0) {
      break;
    }
    if (pos + chunkSize > len) {
      decoded += input.substring(pos);
      break;
    }
    decoded += input.substring(pos, pos + chunkSize);
    pos += chunkSize;
    int nextLF = input.indexOf("\n", pos);
    if (nextLF != -1) {
      pos = nextLF + 1;
    } else {
      break;
    }
  }
  return decoded;
}

// ================================================================
// RAW HTTPS REQUEST
//
// Uses WiFiClientSecure directly — no HTTPClient.
// c.connect(hostname, 443) sets SNI from hostname, which is the
// critical fix: HTTPClient fails to update SNI after a redirect,
// causing -1 on script.googleusercontent.com.
//
// Follows up to 5 redirect hops. Each hop: fresh client, correct SNI.
// Returns the final HTTP status code (200 on success).
// Redirects are fully consumed — caller never sees 302.
// ================================================================
int sendSecureRequest(const String& startUrl, String& payload) {
  String currentUrl = startUrl;

  for (int hop = 0; hop < 5; hop++) {
    ParsedURL pu = parseURL(currentUrl);
    Serial.printf("[HTTP] hop %d  host=%s\n", hop, pu.host.c_str());

    WiFiClientSecure c;
    c.setInsecure();
    c.setHandshakeTimeout(20);   // googleusercontent needs up to ~15s on cold TLS

    if (!c.connect(pu.host.c_str(), 443)) {
      Serial.printf("[HTTP] TCP connect failed (hop %d)\n", hop);
      c.stop();
      return -1;
    }

    // HTTP/1.1 GET — Host header carries SNI value used by connect()
    String req = "GET " + pu.pathAndQuery + " HTTP/1.1\r\n"
                 "Host: " + pu.host + "\r\n"
                 "User-Agent: ESP32\r\n"
                 "Connection: close\r\n\r\n";
    c.print(req);

    // --- Read status line ---
    String statusLine = c.readStringUntil('\n');
    statusLine.trim();
    // e.g. "HTTP/1.1 200 OK"
    int sp1 = statusLine.indexOf(' ');
    int code = (sp1 != -1) ? statusLine.substring(sp1 + 1, sp1 + 4).toInt() : -1;
    Serial.printf("[HTTP] status %d\n", code);

    // --- Read headers ---
    String location = "";
    bool isChunked = false;
    while (true) {
      String line = c.readStringUntil('\n');
      line.trim();
      if (line.length() == 0) break;  // blank line = end of headers
      String lineLower = line;
      lineLower.toLowerCase();
      if (lineLower.startsWith("location:")) {
        location = line.substring(line.indexOf(':') + 1);
        location.trim();
      }
      if (lineLower.startsWith("transfer-encoding:")) {
        if (lineLower.indexOf("chunked") != -1) {
          isChunked = true;
        }
      }
    }

    // --- Handle redirects ---
    if (code == 301 || code == 302 || code == 303 || code == 307 || code == 308) {
      c.stop();
      if (location.length() == 0) {
        Serial.println("[HTTP] Redirect but no Location header.");
        return -1;
      }
      Serial.println("[HTTP] → " + location.substring(0, 80)); // truncate for readability
      currentUrl = location;
      continue;
    }

    // --- Read body ---
    if (code > 0) {
      unsigned long deadline = millis() + 20000;
      while ((c.connected() || c.available()) && millis() < deadline) {
        while (c.available()) payload += (char)c.read();
        delay(1);
      }
      Serial.printf("[HTTP] body len=%d (chunked=%s)\n", payload.length(), isChunked ? "yes" : "no");
      if (isChunked) {
        payload = decodeChunked(payload);
        Serial.printf("[HTTP] decoded body len=%d\n", payload.length());
      }
    }

    c.stop();
    return code;
  }

  Serial.println("[HTTP] Too many redirects.");
  return -1;
}

// ================================================================
// UPLOAD TASK (Core 0)
// ================================================================
void uploadTask(void* pvParameters) {
  while (WiFi.status() != WL_CONNECTED) vTaskDelay(pdMS_TO_TICKS(500));

  unsigned long lastCalFetch = 0;  // tracks last calibration fetch time
  const unsigned long CAL_FETCH_INTERVAL = 10000;  // 10 seconds

  while (true) {
    lastUploadTaskActive = millis();
    ensureWiFiConnection();

    // ── Periodic calibration fetch (every 10s, non-blocking) ──────────────
    if (millis() - lastCalFetch >= CAL_FETCH_INTERVAL) {
      if (WiFi.status() == WL_CONNECTED) {
        Serial.println("[Cal] Fetching latest calibration & settings...");
        fetchEquation();
        fetchTempEquation();
        fetchSettings();
      }
      lastCalFetch = millis();
    }

    vTaskDelay(pdMS_TO_TICKS(100));

    if (lastMainLoopFeed > 0 && (millis() - lastMainLoopFeed > 15000)) {
      Serial.println("[Failsafe] Main loop hung. Rebooting...");
      vTaskDelay(pdMS_TO_TICKS(500));
      ESP.restart();
    }

    bool hasPing = false;
    bool pingState = false;
    bool hasTelemetry = false;
    int telemetryMoistureAdc = 0;
    float telemetryMaVal = -1.0f;
    float telemetryTemperature = -999.0f;

    if (pendingPing) {
      hasPing = true;
      pingState = pendingPingState;
      pendingPing = false;
    } else if (pendingTelemetry) {
      hasTelemetry = true;
      telemetryMoistureAdc = pendingMoistureAdc;
      telemetryMaVal = pendingMaVal;
      telemetryTemperature = pendingTemperature;
      pendingTelemetry = false;
    }

    if (!hasPing && !hasTelemetry) {
      continue;
    }

    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("[Upload] No Wi-Fi — skipped.");
      consecutiveHttpFailures = 0;
      continue;
    }

    if (hasPing) {
      Serial.printf("[Ping] Sending ping: proximity=%s\n", pingState ? "active" : "idle");
      String url = String(GAS_URL) + "?action=ping&proximity=" + (pingState ? "1" : "0");
      String payload = "";
      int code = sendSecureRequest(url, payload);
      if (code == 200) {
        Serial.println("[Ping] OK");
      } else {
        Serial.printf("[Ping] Failed: HTTP %d\n", code);
      }
      continue;
    }

    Serial.printf("[Upload] Sending ADC=%d, mA=%.3f, Temp=%.1f ...\n",
                  telemetryMoistureAdc, telemetryMaVal, telemetryTemperature);
    String url = String(GAS_URL) + "?value=" + String(telemetryMoistureAdc);
    if (telemetryMaVal >= 0.0f) {
      url += "&ma=" + String(telemetryMaVal, 3);
    }
    if (telemetryTemperature > -990.0f) {
      url += "&temp=" + String(telemetryTemperature, 1);
    }

    String payload = "";
    int code = sendSecureRequest(url, payload);

    if (code == 200) {
      Serial.printf("[Upload] OK — %d bytes\n", payload.length());
      Serial.println("[Upload] Response: " + payload);
      if (payload.length() > 0) parseCoefficients(payload);
      consecutiveHttpFailures = 0;
    } else {
      Serial.printf("[Upload] Failed: HTTP %d\n", code);
      if (++consecutiveHttpFailures % 10 == 0) {
        Serial.printf("[Upload] %d consecutive HTTP failures — continuing (no reboot).\n",
                      consecutiveHttpFailures);
      }
    }
  }
}

// ================================================================
// FETCH CALIBRATION FROM GOOGLE SHEETS CSV (boot-time, reliable)
//
// Instead of going through the GAS web app (which hits script.google.com
// and suffers from redirect chains + TLS SNI issues), we fetch the
// Calibration sheet as a CSV export directly from docs.google.com.
// GAS writes r2,a,b,c,type into cells D2:H2 on every calibration update.
// The CSV URL is public-readable if the sheet is shared ("anyone with link").
//
// CSV URL format:
//   https://docs.google.com/spreadsheets/d/SHEET_ID/gviz/tq?tqx=out:csv&sheet=SHEET_NAME&range=D2:H2
// ================================================================
void fetchEquation() {
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[Config] No Wi-Fi — skipping equation fetch.");
    return;
  }

  // URL-encode the sheet name (spaces → %20)
  String sheetEncoded = String(CAL_SHEET);
  sheetEncoded.replace(" ", "%20");

  String path = String("/spreadsheets/d/") + SHEET_ID
              + "/gviz/tq?tqx=out:csv&sheet=" + sheetEncoded
              + "&range=E2:H2"
              + "&t=" + String(millis());  // E=a, F=b, G=c, H=resistor_mode

  Serial.println("[Config] Fetching calibration from Sheets CSV...");
  Serial.println("[Config] host=" + String(SHEET_CSV_HOST) + " path=" + path);

  // Retry up to 3 times
  for (int attempt = 1; attempt <= 3; attempt++) {
    String url     = String("https://") + SHEET_CSV_HOST + path;
    String payload = "";
    int code = sendSecureRequest(url, payload);

    Serial.printf("[Config] attempt %d: HTTP %d  body=%d bytes\n",
                  attempt, code, payload.length());
    if (payload.length() > 0) {
      Serial.println("[Config] CSV body: " + payload);
    }

    if (code == 200 && payload.length() > 0) {
      // Strip CR and quotes. gviz may prepend a header row ("a","b","c","resistor_mode") —
      // we skip any row whose first token is not numeric (toFloat()==0 AND not "0").
      payload.replace("\r", "");
      payload.replace("\"", "");
      payload.trim();

      float a_val = 0, b_val = 0, c_val = 0;
      String resistor_mode_val = "";
      bool  got_data = false;

      // Process line by line; keep the last numeric row
      int lineStart = 0;
      while (lineStart <= (int)payload.length()) {
        int lineEnd = payload.indexOf('\n', lineStart);
        if (lineEnd == -1) lineEnd = payload.length();
        String line = payload.substring(lineStart, lineEnd);
        line.trim();
        lineStart = lineEnd + 1;
        if (line.length() == 0) continue;

        // Split line into 4 comma-separated tokens
        float ta = 0, tb = 0, tc = 0;
        String tmode = "";
        int fc = 0, ts = 0;
        for (int i = 0; i <= (int)line.length(); i++) {
          char ch = (i < (int)line.length()) ? line.charAt(i) : ',';
          if (ch == ',') {
            String tok = line.substring(ts, i); tok.trim();
            if (fc == 0) ta = tok.toFloat();
            else if (fc == 1) tb = tok.toFloat();
            else if (fc == 2) tc = tok.toFloat();
            else if (fc == 3) tmode = tok;
            fc++; ts = i + 1;
          }
        }
        // Last token (no trailing comma)
        if (fc == 3) {
          String tok = line.substring(ts); tok.trim();
          tmode = tok; fc++;
        }

        // Skip header rows: first token text like "a" parses as 0.0 but is not "0"
        String firstTok = line.substring(0, line.indexOf(','));
        firstTok.trim();
        bool firstIsNumeric = (firstTok == "0") || (firstTok.toFloat() != 0.0f);

        if (fc >= 4 && firstIsNumeric) {
          a_val = ta; b_val = tb; c_val = tc;
          resistor_mode_val = tmode;
          got_data = true;
        }
      }

      Serial.printf("[Config] CSV raw: %s\n", payload.c_str());
      Serial.printf("[Config] Parsed: a=%e  b=%f  c=%f  mode=%s  got=%s\n",
                    a_val, b_val, c_val, resistor_mode_val.c_str(), got_data ? "yes" : "no");

      if (got_data && (b_val != 0.0f || c_val != 0.0f)) {
        cal_a = a_val; cal_b = b_val; cal_c = c_val;
        bool withResistor = (resistor_mode_val.indexOf("With resistor") != -1);
        setResistorState(withResistor);
        Serial.printf("[Config] Loaded: a=%e  b=%f  c=%f  quadratic=%s\n",
                      cal_a, cal_b, cal_c, fabsf(cal_a) > 1e-10f ? "yes" : "no");
        Serial.printf("[Config] Check: ADC=128 → %.2f%%\n", calculateMoisture(128));
        return;
      } else {
        Serial.printf("[Config] CSV bad — got_data=%s b=%f c=%f. Run '1. Update Calibration' in Sheets.\n",
                      got_data ? "yes" : "no", b_val, c_val);
        return;
      }
    }

    if (attempt < 3) {
      Serial.printf("[Config] Retrying in 2s...\n");
      delay(2000);
      lastMainLoopFeed     = millis();
      lastUploadTaskActive = millis();
    }
  }

  Serial.println("[Config] All fetch attempts failed — using default coefficients.");
  Serial.printf("[Config] Default: a=%e  b=%f  c=%f\n", cal_a, cal_b, cal_c);
}

void fetchTempEquation() {
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[TempConfig] No Wi-Fi — skipping temp equation fetch.");
    return;
  }

  // URL-encode the sheet name (spaces → %20)
  String sheetEncoded = String(TEMP_CAL_SHEET);
  sheetEncoded.replace(" ", "%20");

  String path = String("/spreadsheets/d/") + SHEET_ID
              + "/gviz/tq?tqx=out:csv&sheet=" + sheetEncoded
              + "&range=H2:K2"
              + "&t=" + String(millis());  // H=R2, I=b, J=c, K=type

  Serial.println("[TempConfig] Fetching temp calibration from Sheets CSV...");
  Serial.println("[TempConfig] host=" + String(SHEET_CSV_HOST) + " path=" + path);

  // Retry up to 3 times
  for (int attempt = 1; attempt <= 3; attempt++) {
    String url     = String("https://") + SHEET_CSV_HOST + path;
    String payload = "";
    int code = sendSecureRequest(url, payload);

    Serial.printf("[TempConfig] attempt %d: HTTP %d  body=%d bytes\n",
                  attempt, code, payload.length());
    if (payload.length() > 0) {
      Serial.println("[TempConfig] CSV body: " + payload);
    }

    if (code == 200 && payload.length() > 0) {
      payload.replace("\r", "");
      payload.replace("\"", "");
      payload.trim();

      float b_val = 18.75f, c_val = -75.0f, r2_val = 1.0f;
      String type_val = "";
      bool got_data = false;

      int lineStart = 0;
      while (lineStart <= (int)payload.length()) {
        int lineEnd = payload.indexOf('\n', lineStart);
        if (lineEnd == -1) lineEnd = payload.length();
        String line = payload.substring(lineStart, lineEnd);
        line.trim();
        lineStart = lineEnd + 1;
        if (line.length() == 0) continue;

        // Split line into 4 comma-separated tokens
        float tr2 = 0, tb = 0, tc = 0;
        String ttype = "";
        int fc = 0, ts = 0;
        for (int i = 0; i <= (int)line.length(); i++) {
          char ch = (i < (int)line.length()) ? line.charAt(i) : ',';
          if (ch == ',') {
            String tok = line.substring(ts, i); tok.trim();
            if (fc == 0) tr2 = tok.toFloat();
            else if (fc == 1) tb = tok.toFloat();
            else if (fc == 2) tc = tok.toFloat();
            else if (fc == 3) ttype = tok;
            fc++; ts = i + 1;
          }
        }
        if (fc == 3) {
          String tok = line.substring(ts); tok.trim();
          ttype = tok; fc++;
        }

        // Check if first token is numeric
        String firstTok = line.substring(0, line.indexOf(','));
        firstTok.trim();
        bool firstIsNumeric = (firstTok == "0") || (firstTok.toFloat() != 0.0f);

        if (fc >= 4 && firstIsNumeric) {
          r2_val = tr2; b_val = tb; c_val = tc;
          type_val = ttype;
          got_data = true;
        }
      }

      if (got_data) {
        temp_cal_b = b_val;
        temp_cal_c = c_val;
        temp_cal_r2 = r2_val;
        temp_cal_type = type_val;
        Serial.printf("[TempConfig] Loaded: b=%f  c=%f  R2=%f  type=%s\n",
                      temp_cal_b, temp_cal_c, temp_cal_r2, temp_cal_type.c_str());
        return;
      } else {
        Serial.println("[TempConfig] CSV bad — got_data=false. Run '2. Update Temp Calibration' in Sheets.");
        return;
      }
    }

    if (attempt < 3) {
      Serial.printf("[TempConfig] Retrying in 2s...\n");
      delay(2000);
      lastMainLoopFeed     = millis();
      lastUploadTaskActive = millis();
    }
  }

  Serial.println("[TempConfig] All fetch attempts failed — using default coefficients.");
  Serial.printf("[TempConfig] Default: b=%f  c=%f\n", temp_cal_b, temp_cal_c);
}

void fetchSettings() {
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[Config] No Wi-Fi — skipping settings fetch.");
    return;
  }

  // URL-encode the sheet name (spaces → %20)
  String sheetEncoded = String(SETTINGS_SHEET);
  sheetEncoded.replace(" ", "%20");

  String path = String("/spreadsheets/d/") + SHEET_ID
              + "/gviz/tq?tqx=out:csv&sheet=" + sheetEncoded
              + "&range=B3"
              + "&t=" + String(millis());

  Serial.println("[Config] Fetching settings from Sheets CSV...");
  Serial.println("[Config] host=" + String(SHEET_CSV_HOST) + " path=" + path);

  // Retry up to 3 times
  for (int attempt = 1; attempt <= 3; attempt++) {
    String url     = String("https://") + SHEET_CSV_HOST + path;
    String payload = "";
    int code = sendSecureRequest(url, payload);

    Serial.printf("[Config] attempt %d: HTTP %d  body=%d bytes\n",
                  attempt, code, payload.length());
    if (payload.length() > 0) {
      Serial.println("[Config] CSV body: " + payload);
    }

    if (code == 200 && payload.length() > 0) {
      payload.replace("\r", "");
      payload.replace("\"", "");
      payload.trim();

      float gapSec = payload.toFloat();
      if (gapSec > 0.1f && gapSec < 3600.0f) {
        veneerGapMs = (unsigned long)(gapSec * 1000.0f);
        Serial.printf("[Config] Loaded veneer gap time: %d ms (%.1f sec)\n", veneerGapMs, gapSec);
        return;
      } else {
        Serial.printf("[Config] CSV bad or invalid gap: %f. Using default %d ms\n", gapSec, veneerGapMs);
        return;
      }
    }

    if (attempt < 3) {
      Serial.printf("[Config] Retrying settings fetch in 2s...\n");
      delay(2000);
      lastMainLoopFeed     = millis();
      lastUploadTaskActive = millis();
    }
  }

  Serial.printf("[Config] Settings fetch failed — using default veneer gap: %d ms\n", veneerGapMs);
}

// ================================================================
// MODBUS RTU RS485 HELPERS
// ================================================================

// Standard Modbus RTU CRC16 calculation
uint16_t calculateCRC16(const uint8_t *data, uint16_t length) {
  uint16_t crc = 0xFFFF;
  for (uint16_t i = 0; i < length; i++) {
    crc ^= data[i];
    for (uint8_t j = 0; j < 8; j++) {
      if (crc & 1) {
        crc = (crc >> 1) ^ 0xA001;
      } else {
        crc >>= 1;
      }
    }
  }
  return crc;
}

// Read a single Modbus input register (using function code 04)
// Returns raw register value on success, or -1 on failure.
int32_t readModbusRegister(uint8_t slaveId, uint16_t regAddress) {
  // Clear any leftover data in RX buffer
  while (Serial2.available() > 0) {
    Serial2.read();
  }

  uint8_t request[8];
  request[0] = slaveId;
  request[1] = 0x04; // Function code 04: Read Input Registers
  request[2] = (regAddress >> 8) & 0xFF;
  request[3] = regAddress & 0xFF;
  request[4] = 0x00; // Number of registers High (0)
  request[5] = 0x01; // Number of registers Low (1)

  uint16_t crc = calculateCRC16(request, 6);
  request[6] = crc & 0xFF;        // CRC Low
  request[7] = (crc >> 8) & 0xFF; // CRC High

  // Transmit request
  digitalWrite(DE_RE_PIN, HIGH); // Enable TX
  delay(1); // Small delay to let pin settle
  Serial2.write(request, 8);
  Serial2.flush(); // Wait for transmission to complete
  delay(1); // Settle time
  digitalWrite(DE_RE_PIN, LOW);  // Enable RX

  // Read response: expected length is 7 bytes:
  // [Slave ID] [Function Code (04)] [Byte Count (02)] [Data High] [Data Low] [CRC Low] [CRC High]
  uint8_t response[7];
  int bytesRead = 0;
  unsigned long startMs = millis();
  const unsigned long timeoutMs = 200;

  while ((millis() - startMs < timeoutMs) && (bytesRead < 7)) {
    if (Serial2.available() > 0) {
      response[bytesRead++] = Serial2.read();
    }
    delay(1);
  }

  if (bytesRead < 7) {
    Serial.printf("[Modbus] Error: Timeout or partial response. Received %d/7 bytes.\n", bytesRead);
    return -1;
  }

  // Verify Slave ID and Function Code
  if (response[0] != slaveId || response[1] != 0x04) {
    Serial.printf("[Modbus] Error: Invalid response header. ID: %d, FC: %d\n", response[0], response[1]);
    return -1;
  }

  // Verify CRC
  uint16_t receivedCRC = response[5] | (response[6] << 8);
  uint16_t calculatedCRC = calculateCRC16(response, 5);
  if (receivedCRC != calculatedCRC) {
    Serial.println("[Modbus] Error: CRC mismatch.");
    return -1;
  }

  // Extract raw 16-bit register value
  uint16_t rawVal = (response[3] << 8) | response[4];
  return rawVal;
}

// Write a single Modbus holding register (using function code 06)
// Returns true on success, or false on failure.
bool writeModbusRegister(uint8_t slaveId, uint16_t regAddress, uint16_t value) {
  // Clear any leftover data in RX buffer
  while (Serial2.available() > 0) {
    Serial2.read();
  }

  uint8_t request[8];
  request[0] = slaveId;
  request[1] = 0x06; // Function code 06: Write Single Register
  request[2] = (regAddress >> 8) & 0xFF;
  request[3] = regAddress & 0xFF;
  request[4] = (value >> 8) & 0xFF;
  request[5] = value & 0xFF;

  uint16_t crc = calculateCRC16(request, 6);
  request[6] = crc & 0xFF;        // CRC Low
  request[7] = (crc >> 8) & 0xFF; // CRC High

  // Transmit request
  digitalWrite(DE_RE_PIN, HIGH); // Enable TX
  delay(1); // Small delay to let pin settle
  Serial2.write(request, 8);
  Serial2.flush(); // Wait for transmission to complete
  delay(1); // Settle time
  digitalWrite(DE_RE_PIN, LOW);  // Enable RX

  // Read response: expected length is 8 bytes (echo of request)
  uint8_t response[8];
  int bytesRead = 0;
  unsigned long startMs = millis();
  const unsigned long timeoutMs = 200;

  while ((millis() - startMs < timeoutMs) && (bytesRead < 8)) {
    if (Serial2.available() > 0) {
      response[bytesRead++] = Serial2.read();
    }
    delay(1);
  }

  if (bytesRead < 8) {
    Serial.printf("[Modbus] Write Error: Timeout. Received %d/8 bytes.\n", bytesRead);
    return false;
  }

  // Verify Slave ID and Function Code
  if (response[0] != slaveId || response[1] != 0x06) {
    Serial.printf("[Modbus] Write Error: Invalid response header. ID: %d, FC: %d\n", response[0], response[1]);
    return false;
  }

  // Verify CRC
  uint16_t receivedCRC = response[6] | (response[7] << 8);
  uint16_t calculatedCRC = calculateCRC16(response, 6);
  if (receivedCRC != calculatedCRC) {
    Serial.println("[Modbus] Write Error: CRC mismatch.");
    return false;
  }

  // Verify value written matches response echo
  uint16_t respAddr = (response[2] << 8) | response[3];
  uint16_t respVal = (response[4] << 8) | response[5];
  if (respAddr != regAddress || respVal != value) {
    Serial.printf("[Modbus] Write Error: Echo mismatch. Addr: 0x%04X, Val: 0x%04X\n", respAddr, respVal);
    return false;
  }

  return true;
}

// Convert raw Modbus register value to temperature
float calculateModbusTemperature(int32_t rawVal) {
  if (rawVal < 0) return -999.0f; // Error indicator

  float mAVal = 0.0f;
  float temp = -999.0f;

  if (SENSOR_SIGNAL_TYPE == 0) {
    // 4-20mA (represented as 4000 to 20000 uA)
    mAVal = (float)rawVal / 1000.0f; // Converted to mA
    Serial.printf("[Modbus] Sensor Current: %.3f mA\n", mAVal);
    
    if (mAVal >= 4.0f && mAVal <= 20.0f) {
      temp = temp_cal_b * mAVal + temp_cal_c;
    } else {
      Serial.println("[Modbus] Warning: Current out of 4-20mA range.");
      temp = temp_cal_b * mAVal + temp_cal_c; // Still calculate using regression
    }
  } else if (SENSOR_SIGNAL_TYPE == 1) {
    // 0-5V (represented as 0 to 5000 mV)
    // Assuming 250 ohm shunt resistor (1-5V for 4-20mA)
    float voltVal = (float)rawVal; // mV
    mAVal = voltVal / 250.0f; // Converted to mA
    Serial.printf("[Modbus] Sensor Voltage: %.1f mV (approx. %.3f mA)\n", voltVal, mAVal);
    
    if (voltVal >= 0.0f && voltVal <= 5000.0f) {
      temp = temp_cal_b * mAVal + temp_cal_c;
    } else {
      Serial.println("[Modbus] Warning: Voltage out of 0-5V range.");
      temp = temp_cal_b * mAVal + temp_cal_c;
    }
  } else if (SENSOR_SIGNAL_TYPE == 2) {
    // 1-5V (represented as 1000 to 5000 mV)
    // Assuming 250 ohm shunt resistor (1-5V for 4-20mA)
    float voltVal = (float)rawVal; // mV
    mAVal = voltVal / 250.0f; // Converted to mA
    Serial.printf("[Modbus] Sensor Voltage: %.1f mV (approx. %.3f mA)\n", voltVal, mAVal);
    
    if (voltVal >= 1000.0f && voltVal <= 5000.0f) {
      temp = temp_cal_b * mAVal + temp_cal_c;
    } else {
      Serial.println("[Modbus] Warning: Voltage out of 1-5V range.");
      temp = temp_cal_b * mAVal + temp_cal_c;
    }
  }

  return temp;
}

// ================================================================
// LCD MARQUEE SCROLLING
// ================================================================
void displayMarquee(String line1, String line2, unsigned long durationMs) {
  unsigned long start = millis();
  int len1 = line1.length();
  int len2 = line2.length();
  int maxLen = max(len1, len2);
  
  if (maxLen <= 16) {
    if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print(line1);
      lcd.setCursor(0, 1);
      lcd.print(line2);
      xSemaphoreGive(i2cMutex);
    } else if (i2cMutex == NULL) {
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print(line1);
      lcd.setCursor(0, 1);
      lcd.print(line2);
    }
    
    while (millis() - start < durationMs) {
      lastMainLoopFeed = millis();
      lastUploadTaskActive = millis();
      delay(100);
    }
    return;
  }
  
  String p1 = line1 + "    ";
  String p2 = line2 + "    ";
  int pMaxLen = max(p1.length(), p2.length());
  
  while (p1.length() < pMaxLen) p1 += " ";
  while (p2.length() < pMaxLen) p2 += " ";
  
  int offset = 0;
  unsigned long lastScrollMs = 0;
  const unsigned long scrollIntervalMs = 350;
  const unsigned long pauseDurationMs = 1500;
  bool pausing = true;
  unsigned long pauseStartMs = millis();
  
  if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print(p1.substring(0, 16));
    lcd.setCursor(0, 1);
    lcd.print(p2.substring(0, 16));
    xSemaphoreGive(i2cMutex);
  } else if (i2cMutex == NULL) {
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print(p1.substring(0, 16));
    lcd.setCursor(0, 1);
    lcd.print(p2.substring(0, 16));
  }
  
  while (millis() - start < durationMs) {
    lastMainLoopFeed = millis();
    lastUploadTaskActive = millis();
    
    if (pausing) {
      if (millis() - pauseStartMs >= pauseDurationMs) {
        pausing = false;
        lastScrollMs = millis();
      }
    } else {
      if (millis() - lastScrollMs >= scrollIntervalMs) {
        lastScrollMs = millis();
        offset++;
        if (offset > pMaxLen - 16) {
          offset = 0;
          pausing = true;
          pauseStartMs = millis();
        }
        
        String disp1 = p1.substring(offset, offset + 16);
        String disp2 = p2.substring(offset, offset + 16);
        
        if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, pdMS_TO_TICKS(50)) == pdTRUE) {
          lcd.setCursor(0, 0);
          lcd.print(disp1);
          lcd.setCursor(0, 1);
          lcd.print(disp2);
          xSemaphoreGive(i2cMutex);
        } else if (i2cMutex == NULL) {
          lcd.setCursor(0, 0);
          lcd.print(disp1);
          lcd.setCursor(0, 1);
          lcd.print(disp2);
        }
      }
    }
    delay(20);
  }
  
  // Reset to offset 0 on exit
  if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    lcd.setCursor(0, 0);
    lcd.print(p1.substring(0, 16));
    lcd.setCursor(0, 1);
    lcd.print(p2.substring(0, 16));
    xSemaphoreGive(i2cMutex);
  } else if (i2cMutex == NULL) {
    lcd.setCursor(0, 0);
    lcd.print(p1.substring(0, 16));
    lcd.setCursor(0, 1);
    lcd.print(p2.substring(0, 16));
  }
}

// ================================================================
// SETUP
// ================================================================

void setup() {
  Serial.begin(115200);
  Serial.println("\nVeneer Moisture Monitor starting...");

  // Initialize MAX485 Flow Control Pin
  pinMode(DE_RE_PIN, OUTPUT);
  digitalWrite(DE_RE_PIN, LOW); // Default to receive mode

  // Initialize Serial2 for Modbus RTU (9600 baud, 8N1)
  Serial2.begin(9600, SERIAL_8N1, RX2_PIN, TX2_PIN);
  Serial.println("[Modbus] Serial2 initialized at 9600 baud (8N1).");

  // Configure the Waveshare module channel range programmatically on boot
  uint16_t configAddress = 0x1000 + MODBUS_START_REG;
  uint16_t targetMode = 3; // Default: 4-20mA
  if (SENSOR_SIGNAL_TYPE == 0)      targetMode = 3; // 4-20mA
  else if (SENSOR_SIGNAL_TYPE == 1) targetMode = 0; // 0-5V
  else if (SENSOR_SIGNAL_TYPE == 2) targetMode = 1; // 1-5V

  Serial.printf("[Modbus] Configuring register 0x%04X to mode %d...\n", configAddress, targetMode);
  
  bool configSuccess = false;
  for (int attempt = 1; attempt <= 3; attempt++) {
    if (writeModbusRegister(MODBUS_SLAVE_ID, configAddress, targetMode)) {
      Serial.println("[Modbus] Channel configuration successful!");
      configSuccess = true;
      break;
    }
    Serial.printf("[Modbus] Configuration attempt %d failed. Retrying...\n", attempt);
    delay(500);
  }
  if (!configSuccess) {
    Serial.println("[Modbus] Warning: Failed to set Channel mode on Waveshare module. Please check hardware settings.");
  }



  i2cMutex = xSemaphoreCreateMutex();

  Wire.begin(22, 21);

  if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, portMAX_DELAY) == pdTRUE) {
    lcd.init();
    lcd.backlight();
    lcd.createChar(0, solidBlock);
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("Booting system...");
    xSemaphoreGive(i2cMutex);
  }

  analogReadResolution(12);
  analogSetPinAttenuation(SENSOR_PIN, ADC_0db);

  lastMainLoopFeed     = millis();
  lastUploadTaskActive = millis();

  xTaskCreatePinnedToCore(uploadTask, "uploadTask", 32768, NULL, 1, NULL, 0);

  connectWiFi();

  if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, portMAX_DELAY) == pdTRUE) {
    lcd.clear();
    xSemaphoreGive(i2cMutex);
  }

  displayMarquee("IP: " + WiFi.localIP().toString(), "MAC: " + WiFi.macAddress(), 10000);

  // Initialize resistor pin to INPUT (inactive) by default
  pinMode(RESISTOR_PIN, INPUT);
  
  // Initialize proximity pin with internal pull-up resistor
  pinMode(PROXIMITY_PIN, INPUT_PULLUP);

  fetchEquation();
  fetchTempEquation();
  fetchSettings();

  // Refresh watchdog after fetch
  lastMainLoopFeed     = millis();
  lastUploadTaskActive = millis();

  delay(2000);

  if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, portMAX_DELAY) == pdTRUE) {
    lcd.clear();
    xSemaphoreGive(i2cMutex);
  }

  setResistorState(resistorActive);

  if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, portMAX_DELAY) == pdTRUE) {
    lcd.setCursor(0, 1);
    lcd.print("MOIST:  --%   P0");
    xSemaphoreGive(i2cMutex);
  } else {
    lcd.setCursor(0, 1);
    lcd.print("MOIST:  --%   P0");
  }

  lastMainLoopFeed     = millis();
  lastUploadTaskActive = millis();
}

// ================================================================
// MAIN LOOP (Core 1 — pure sensing)
// ================================================================
void finalizeAndUpload(int count) {
  if (count <= 0) return;
  
  int avg = getTrimmedMean(readings, count);
  Serial.printf("Filtered Average (%d samples): %d\n", count, avg);

  float moisture = calculateMoisture(avg);
  lastAvgMoisture = moisture;
  Serial.printf("Filtered Moisture: %.1f%%  (a=%e b=%f c=%f quadratic=%s)\n",
                lastAvgMoisture, cal_a, cal_b, cal_c,
                fabsf(cal_a) > 1e-10f ? "yes" : "no");

  // Query Modbus Temperature Sensor (FirstRate FST600-400A via Waveshare Modbus module)
  Serial.println("[Modbus] Querying Temperature Sensor...");
  int32_t rawTempReg = readModbusRegister(MODBUS_SLAVE_ID, MODBUS_START_REG);
  float mAVal = -1.0f;
  float tempVal = -999.0f;
  if (rawTempReg >= 0) {
    if (SENSOR_SIGNAL_TYPE == 0) {
      mAVal = (float)rawTempReg / 1000.0f;
    } else {
      mAVal = (float)rawTempReg / 250.0f; // for 0-5V and 1-5V assuming 250 ohm shunt
    }
    
    tempVal = calculateModbusTemperature(rawTempReg);
    if (tempVal > -990.0f) {
      Serial.printf("[Modbus] Temperature: %.1f C (mA: %.3f, raw reg: %d)\n\n", tempVal, mAVal, rawTempReg);
    } else {
      Serial.printf("[Modbus] Error: Calculated temperature out of bounds (mA: %.3f, raw reg: %d)\n\n", mAVal, rawTempReg);
    }
  } else {
    Serial.println("[Modbus] Error: Failed to read from Modbus RTU module.\n");
  }

  // Perform LCD refresh to show the new average moisture and temperature
  lastAvgTemperature = tempVal;
  refreshLCD();

  pendingMoistureAdc = avg;
  pendingMaVal = mAVal;
  pendingTemperature = tempVal;
  pendingTelemetry = true;
}

void queuePing(bool active) {
  pendingPingState = active;
  pendingPing = true;
}

void loop() {
  lastMainLoopFeed = millis();

  if (lastUploadTaskActive > 0 && (millis() - lastUploadTaskActive > 45000)) {
    Serial.println("[Failsafe] Upload task hung. Rebooting...");
    ESP.restart();
  }

  bool detecting = (digitalRead(PROXIMITY_PIN) == LOW);

  // Detect proximity state change OR keep-alive interval (10 seconds)
  if (detecting != lastDetectingState || (millis() - lastPingMs >= PING_INTERVAL_MS)) {
    lastDetectingState = detecting;
    lastPingMs = millis();
    queuePing(detecting);
  }

  if (!detecting) {
    // Proximity is not detecting anything
    // If we have accumulated enough samples before it went inactive, finalize and upload them
    if (readingIndex >= 10) {
      Serial.printf("[Proximity] Stopped detecting. Finalizing %d samples early...\n", readingIndex);
      finalizeAndUpload(readingIndex);
    }
    readingIndex = 0; // reset counter

    updateDisplayIdle();
    delay(50);
    return;
  }

  // Proximity is detecting!
  if (displayIsNA) {
    displayIsNA = false;
    refreshLCD();
  }
  
  lastDetectMs = millis(); // update detection timestamp while active
  updateDisplayActive();

  int rawRead = analogRead(SENSOR_PIN);
  readings[readingIndex++] = rawRead;
  Serial.printf("Reading [%d/%d]: %d\n", readingIndex, SAMPLE_COUNT, rawRead);

  if (readingIndex >= SAMPLE_COUNT) {
    Serial.println("\n=== 5-Second Window Complete ===");
    finalizeAndUpload(SAMPLE_COUNT);
    readingIndex = 0;
  }

  delay(SAMPLE_INTERVAL_MS);
}

// ================================================================
// WIFI HELPERS
// ================================================================
void connectWiFi() {
  Serial.println("\nScanning WiFi networks...");
  
  if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, portMAX_DELAY) == pdTRUE) {
    lcd.setCursor(0, 1);
    lcd.print("Scanning WiFi...");
    xSemaphoreGive(i2cMutex);
  } else if (i2cMutex == NULL) {
    lcd.setCursor(0, 1);
    lcd.print("Scanning WiFi...");
  }

  // Set WiFi to station mode and disconnect
  WiFi.mode(WIFI_STA);
  WiFi.disconnect();
  delay(100);
  
  int n = WiFi.scanNetworks();
  Serial.printf("Scan complete. Found %d networks.\n", n);
  
  bool ciscosaFound = false;
  bool sharonsaFound = false;
  bool sharonbaFound = false;
  
  for (int i = 0; i < n; ++i) {
    String foundSsid = WiFi.SSID(i);
    Serial.printf("  %d: %s (%d dBm)\n", i + 1, foundSsid.c_str(), WiFi.RSSI(i));
    if (foundSsid == "ciscosa") {
      ciscosaFound = true;
    } else if (foundSsid == "SHARONSA") {
      sharonsaFound = true;
    } else if (foundSsid == "SHARONBA") {
      sharonbaFound = true;
    }
  }
  
  if (ciscosaFound) {
    ssid = "ciscosa";
    password = "Abc123#@";
    Serial.println("Selected network: ciscosa");
  } else if (sharonsaFound) {
    ssid = "SHARONSA";
    password = sharon_password;
    Serial.println("Selected network: SHARONSA");
  } else if (sharonbaFound) {
    ssid = "SHARONBA";
    password = sharon_password;
    Serial.println("Selected network: SHARONBA");
  } else {
    Serial.println("None of the configured networks (ciscosa, SHARONSA, SHARONBA) found. Rebooting...");
    if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, portMAX_DELAY) == pdTRUE) {
      lcd.setCursor(0, 1);
      lcd.print("WiFi not found! ");
      xSemaphoreGive(i2cMutex);
    } else if (i2cMutex == NULL) {
      lcd.setCursor(0, 1);
      lcd.print("WiFi not found! ");
    }
    delay(2000);
    ESP.restart();
  }
  
  Serial.printf("Connecting to %s...", ssid);
  if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, portMAX_DELAY) == pdTRUE) {
    lcd.setCursor(0, 1);
    String connectingMsg = "Conn:" + String(ssid);
    while (connectingMsg.length() < 16) connectingMsg += " ";
    lcd.print(connectingMsg);
    xSemaphoreGive(i2cMutex);
  } else if (i2cMutex == NULL) {
    lcd.setCursor(0, 1);
    String connectingMsg = "Conn:" + String(ssid);
    while (connectingMsg.length() < 16) connectingMsg += " ";
    lcd.print(connectingMsg);
  }
  
  WiFi.begin(ssid, password);
  int attempts = 0;
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
    if (++attempts > 30) {
      Serial.println("\n[Failsafe] WiFi timeout. Rebooting...");
      if (i2cMutex != NULL && xSemaphoreTake(i2cMutex, portMAX_DELAY) == pdTRUE) {
        lcd.setCursor(0, 1);
        lcd.print("Conn Timeout!   ");
        xSemaphoreGive(i2cMutex);
      } else if (i2cMutex == NULL) {
        lcd.setCursor(0, 1);
        lcd.print("Conn Timeout!   ");
      }
      delay(2000);
      ESP.restart();
    }
  }
  Serial.println(" Connected! IP: " + WiFi.localIP().toString());
  lastWiFiConnectedTime = millis();
}

void ensureWiFiConnection() {
  if (WiFi.status() == WL_CONNECTED) {
    lastWiFiConnectedTime   = millis();
    consecutiveWiFiFailures = 0;
    return;
  }
  Serial.println("Wi-Fi lost. Reconnecting...");
  WiFi.disconnect();
  WiFi.begin(ssid, password);
  int attempts = 0;
  while (attempts < 20 && WiFi.status() != WL_CONNECTED) {
    vTaskDelay(pdMS_TO_TICKS(500));
    attempts++;
  }
  if (WiFi.status() != WL_CONNECTED) {
    if (++consecutiveWiFiFailures >= 3) {
      Serial.println("[Failsafe] WiFi failed 3 times. Rebooting...");
      vTaskDelay(pdMS_TO_TICKS(500));
      ESP.restart();
    }
    Serial.printf("[Failsafe] WiFi reconnect failed (%d/3)\n", consecutiveWiFiFailures);
  } else {
    consecutiveWiFiFailures = 0;
    lastWiFiConnectedTime   = millis();
  }
  if (WiFi.status() != WL_CONNECTED && (millis() - lastWiFiConnectedTime > 60000)) {
    Serial.println("[Failsafe] No WiFi for 1 min. Rebooting...");
    vTaskDelay(pdMS_TO_TICKS(500));
    ESP.restart();
  }
}

// ================================================================
// TRIMMED MEAN (10% each end)
// ================================================================
void sortArray(int arr[], int size) {
  for (int i = 0; i < size - 1; i++)
    for (int j = 0; j < size - i - 1; j++)
      if (arr[j] > arr[j + 1]) { int t = arr[j]; arr[j] = arr[j+1]; arr[j+1] = t; }
}

int getTrimmedMean(int arr[], int size) {
  sortArray(arr, size);
  int cut = size / 10;
  Serial.print("  Low  discarded: ");
  for (int i = 0; i < cut; i++) Serial.printf("%d ", arr[i]);
  Serial.println();
  Serial.print("  Kept (middle):  ");
  long sum = 0; int count = 0;
  for (int i = cut; i < size - cut; i++) {
    Serial.printf("%d ", arr[i]);
    sum += arr[i]; count++;
  }
  Serial.println();
  Serial.print("  High discarded: ");
  for (int i = size - cut; i < size; i++) Serial.printf("%d ", arr[i]);
  Serial.println();
  return sum / count;
}
