# ใบงานการทดลองที่ 13.2
### การพัฒนา Hardware Serial Bridge ดักฟังข้อมูล USB-UART จาก ESP32 แปลงเข้าสู่ MQTT Topics และ REST API

> [!NOTE] **คำชี้แจง**
> ในใบงานนี้นักศึกษาจะได้เชื่อมต่อบอร์ด ESP32 เข้ากับคอมพิวเตอร์ผ่านสาย USB-UART และพัฒนาคอมโพเนนต์ **`SerialBridgeWorker`** ซึ่งเป็น BackgroundService ใน .NET 8 ทำหน้าที่ดักฟังข้อมูลสตรีมมิ่งที่บอร์ดส่งขึ้นมาทางพอร์ตสื่อสารอนุกรม (COM Port) แล้วแปลงข้อมูลนั้นเป็นทั้ง **MQTT Telemetry Topic** และแคชไว้ใน **RESTful API** พร้อมทั้งเปิดช่องทางให้ REST API สามารถส่งคำสั่งเปิด-ปิดไฟย้อนกลับไปหาบอร์ดทางสาย Serial ได้

---

## 1. วัตถุประสงค์การทดลอง
1. สามารถติดตั้งและใช้งานไลบรารี `System.IO.Ports` ในโปรเจกต์ ASP.NET Core ได้
2. สามารถพัฒนา `BackgroundService` เพื่อเปิดพอร์ต Serial อ่านข้อมูลแบบ Non-blocking ได้
3. สามารถแปลงข้อมูลที่อ่านได้จากสาย Serial ไป Publish ต่อใน Embedded MQTT Broker ได้
4. สามารถพัฒนา REST API เพื่ออ่านค่าเซนเซอร์ล่าสุด และส่งคำสั่งควบคุมหลอดไฟผ่านสาย Serial ได้

---

## 2. โครงสร้างการเชื่อมโยงข้อมูล (Data Bridge Topology)

<p align="center"><img src="Images/Fig_07-Labsheet-13-2_academic.svg" alt="07-Labsheet-13-2-Hardware-Serial-to-MQTT-and-REST-Bridge"  style=" height: auto;" ></p>
<p align="center"><b></b>โครงสร้างการเชื่อมโยงข้อมูล (Data Bridge Topology)</p>

---

## 3. ขั้นตอนการทดลอง (Step-by-Step Activities)

ใช้บอร์ด **ESP32** รุ่นปกติ โดยกำหนดขา ADC Input เป็น **GPIO34** (ADC1_CH6) และ LED Status เป็น **GPIO2** ในเฟิร์มแวร์ของบอร์ด ส่วนการเชื่อมต่อ USB-UART Serial ใช้ความเร็ว **115200 bps**

### กิจกรรมที่ 13-2.1: การติดตั้งคอมโพเนนต์ Serial Port

ในโฟลเดอร์โปรเจกต์ `ConvergedEdgeGateway` รันคำสั่ง:
```powershell
dotnet add package System.IO.Ports --version 8.0.0
```

---

### กิจกรรมที่ 13-2.2: การสร้างคลาสเก็บสถานะและ Worker อ่าน Serial

สร้างไฟล์ `Services.cs` ในโปรเจกต์:

```csharp
using System.Collections.Concurrent;
using System.IO.Ports;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Server;

// 1. คลาสจัดเก็บสถานะระบบแบบ Thread-Safe
public class GatewayStateStore
{
    private readonly ConcurrentDictionary<string, object> _state = new();

    public void Set(string key, object val) => _state[key] = val;
    public T? Get<T>(string key) => _state.TryGetValue(key, out var val) && val is T t ? t : default;
    public object? Get(string key) => _state.TryGetValue(key, out var val) ? val : null;
}

// 2. โมเดลข้อมูล Telemetry
public record TelemetryData(double Voltage, int Raw, long Timestamp);

// 3. งานเบื้องหลังสำหรับเชื่อมต่อและดักฟัง Serial Port
public class SerialBridgeWorker : BackgroundService
{
    private SerialPort? _serial;
    private readonly string _portName = "COM3"; // ปรับให้ตรงกับพอร์ตของ ESP32 บนเครื่องคุณ
    private readonly int _baudRate = 115200;
    private readonly GatewayStateStore _store;
    private readonly MqttServer _mqttServer;
    private readonly ILogger<SerialBridgeWorker> _logger;

    public SerialBridgeWorker(GatewayStateStore store, MqttServer mqttServer, ILogger<SerialBridgeWorker> logger)
    {
        _store = store;
        _mqttServer = mqttServer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("🔌 พยายามเปิด Serial Port {Port}...", _portName);

        while (!stoppingToken.IsCancellationRequested)
        {
            if (_serial == null || !_serial.IsOpen)
            {
                try
                {
                    _serial = new SerialPort(_portName, _baudRate)
                    {
                        NewLine = "\n",
                        ReadTimeout = 1000
                    };
                    _serial.Open();
                    _logger.LogInformation("✅ เชื่อมต่อฮาร์ดแวร์ Serial Port {Port} สำเร็จ!", _portName);
                }
                catch
                {
                    // หากยังไม่เสียบบอร์ด ให้รอ 3 วินาทีแล้วลองใหม่
                    await Task.Delay(3000, stoppingToken);
                    continue;
                }
            }

            try
            {
                string line = _serial.ReadLine().Trim();
                if (!string.IsNullOrEmpty(line))
                {
                    await HandleIncomingSerialLine(line);
                }
            }
            catch (TimeoutException) { /* ปกติเมื่อไม่มีข้อมูลส่งมาใน 1 วิ */ }
            catch (Exception ex)
            {
                _logger.LogWarning("Serial Read Warning: {Message}", ex.Message);
                _serial?.Close();
            }
        }
    }

    private async Task HandleIncomingSerialLine(string line)
    {
        try
        {
            // ตัวอย่างข้อมูลที่ ESP32 ยิงมา: {"voltage": 1.65, "raw": 2048}
            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;
            double v = root.GetProperty("voltage").GetDouble();
            int r = root.GetProperty("raw").GetInt32();

            var telemetry = new TelemetryData(v, r, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            
            // 1. อัปเดตลง State Store ในหน่วยความจำ
            _store.Set("latest_telemetry", telemetry);

            // 2. แปลงและผลักเข้าสู่ Embedded MQTT Broker
            var mqttMsg = new MqttApplicationMessageBuilder()
                .WithTopic("iot/esp32/telemetry")
                .WithPayload(line)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce)
                .Build();

            await _mqttServer.InjectApplicationMessage(new InjectedMqttApplicationMessage(mqttMsg));
        }
        catch { /* หากไม่ใช่ JSON ให้ข้ามไป */ }
    }

    // ฟังก์ชันส่งคำสั่งย้อนกลับไปหาบอร์ดทาง Serial
    public bool SendSerialCommand(string cmd)
    {
        if (_serial?.IsOpen == true)
        {
            _serial.WriteLine(cmd);
            return true;
        }
        return false;
    }

    public override void Dispose()
    {
        _serial?.Close();
        base.Dispose();
    }
}
```

---

### กิจกรรมที่ 13-2.3: ปรับแต่ง `Program.cs` ให้เปิดใช้ REST APIs

เปิด `Program.cs` และปรับปรุงส่วนการลงทะเบียนบริการและเส้นทาง API:

```csharp
using MQTTnet.AspNetCore;
using MQTTnet.Server;

var builder = WebApplication.CreateBuilder(args);

// ตั้งค่า Kestrel
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(5000);
    options.ListenAnyIP(1883, l => l.UseMqtt());
});

// ลงทะเบียน Services
builder.Services.AddSingleton<GatewayStateStore>();
builder.Services.AddMqttServer(options => options.WithDefaultEndpoint());
builder.Services.AddHostedService<SerialBridgeWorker>();
// เพื่อให้ API เรียกใช้งาน Worker ได้
builder.Services.AddSingleton(sp => 
    sp.GetServices<IHostedService>().OfType<SerialBridgeWorker>().First());

var app = builder.Build();

app.UseMqttServer(server => { /* callbacks */ });

// 1. API อ่านข้อมูลเซนเซอร์ล่าสุด
app.MapGet("/api/telemetry/latest", (GatewayStateStore store) =>
{
    var data = store.Get("latest_telemetry");
    return data != null ? Results.Ok(data) : Results.NotFound(new { error = "No telemetry yet" });
});

// 2. API ควบคุมหลอดไฟผ่าน Serial Bridge
app.MapPost("/api/led/{state}", (string state, SerialBridgeWorker worker) =>
{
    string cmd = state.ToLower() == "on" ? "LED:ON" : "LED:OFF";
    bool success = worker.SendSerialCommand(cmd);

    return Results.Ok(new
    {
        dispatchedCommand = cmd,
        sentToSerial = success,
        serverTime = DateTime.Now.ToString("HH:mm:ss")
    });
});

app.Run();
```

---

### กิจกรรมที่ 13-2.4: โค้ดฝั่ง ESP32 สำหรับสื่อสารสองทางผ่าน Serial (`main.c`)

สำหรับฝั่งไมโครคอนโทรลเลอร์ ให้เขียนโค้ดในโปรเจกต์ ESP-IDF เพื่ออ่านค่าแอนะล็อกและสตรีม JSON ออกทาง Serial รวมถึงอ่านคำสั่งควบคุมหลอดไฟจาก C# Gateway:

```c
#include <stdio.h>
#include <string.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "driver/gpio.h"
#include "driver/uart.h"
#include "esp_log.h"
#include "esp_adc/adc_oneshot.h"

#define LED_PIN             GPIO_NUM_2       // Onboard LED บน ESP32
#define POT_ADC_CHANNEL     ADC_CHANNEL_6    // GPIO 34 (ADC1 Channel 6)

void app_main(void)
{
    // 1. ตั้งค่า GPIO สำหรับไฟ LED
    gpio_reset_pin(LED_PIN);
    gpio_set_direction(LED_PIN, GPIO_MODE_OUTPUT);
    gpio_set_level(LED_PIN, 0);

    // 2. ตั้งค่า ADC Unit 1
    adc_oneshot_unit_handle_t adc1_handle;
    adc_oneshot_unit_init_cfg_t init_config = {
        .unit_id = ADC_UNIT_1,
        .ulp_mode = ADC_ULP_MODE_DISABLE,
    };
    ESP_ERROR_CHECK(adc_oneshot_new_unit(&init_config, &adc1_handle));

    // 3. กำหนดความละเอียด 12-bit (0-4095) และ Attenuation 12dB (0 - 3.3V)
    adc_oneshot_chan_cfg_t chan_config = {
        .bitwidth = ADC_BITWIDTH_12,
        .atten = ADC_ATTEN_DB_12,
    };
    ESP_ERROR_CHECK(adc_oneshot_config_channel(adc1_handle, POT_ADC_CHANNEL, &chan_config));

    // 4. ติดตั้ง UART Driver บนพอร์ตเริ่มต้น เพื่อให้อ่านคำสั่งผ่าน Serial แบบ Non-blocking ได้
    uart_driver_install(UART_NUM_0, 256, 0, 0, NULL, 0);

    uint8_t rx_buf[64];

    while (1) {
        // --- ส่วนที่ 1: อ่านค่า ADC และส่งออก JSON ทาง Serial ---
        int raw = 0;
        ESP_ERROR_CHECK(adc_oneshot_read(adc1_handle, POT_ADC_CHANNEL, &raw));
        float voltage = (raw / 4095.0f) * 3.3f;

        // พิมพ์ JSON สตรีมมิ่งออก Serial
        printf("{\"voltage\":%.2f,\"raw\":%d}\n", voltage, raw);

        // --- ส่วนที่ 2: ตรวจจับคำสั่งจาก C# Kestrel (LED:ON / LED:OFF) ---
        int len = uart_read_bytes(UART_NUM_0, rx_buf, sizeof(rx_buf) - 1, pdMS_TO_TICKS(50));
        if (len > 0) {
            rx_buf[len] = '\0';
            if (strstr((char *)rx_buf, "LED:ON") != NULL) {
                gpio_set_level(LED_PIN, 1);
            } else if (strstr((char *)rx_buf, "LED:OFF") != NULL) {
                gpio_set_level(LED_PIN, 0);
            }
        }

        vTaskDelay(pdMS_TO_TICKS(450));
    }
}
```


---

### กิจกรรมที่ 13-2.5: การทดสอบสะพานเชื่อมต่อ

> [!WARNING] **ข้อพึงระวังเรื่องพอร์ตสื่อสารอนุกรม (Serial Port Exclusive Lock):**
> 1. ตรวจสอบหมายเลข COM Port ของบอร์ด ESP32 ผ่าน Device Manager แล้วแก้ไขตัวแปร `_portName` ในไฟล์ `Services.cs` ให้ตรงกับเครื่องของคุณ (เช่น `COM24`)
> 2. **ต้องปิด `idf monitor`, PuTTY หรือ Serial Monitor อื่นๆ ก่อนรัน Kestrel เสมอ** เพราะบน Windows พอร์ต COM จะถูกล็อกใช้งานแบบ Exclusive Access (เปิดได้เพียง 1 โปรแกรมในเวลาเดียวกัน) หากมีโปรแกรมอื่นเปิดพอร์ตค้างไว้ C# จะเปิดพอร์ตไม่สำเร็จและส่งคำสั่งไม่ได้ (`sentToSerial: false`)

1. เสียบบอร์ด ESP32 เข้ากับเครื่อง และสั่งรัน Kestrel:
```powershell
dotnet run
```
2. ทดสอบเรียกดูค่าเซนเซอร์ผ่านเบราว์เซอร์หรือ cURL:
```powershell
curl http://localhost:5000/api/telemetry/latest
```
*สังเกตว่าจะได้ข้อมูล JSON ของแรงดันไฟฟ้าทันที!*
3. เปิด `mosquitto_sub`:
```powershell
mosquitto_sub -h 127.0.0.1 -p 1883 -t "iot/esp32/telemetry" -i "sub_client" -v
```
*สังเกตว่าข้อมูลที่อ่านจากสาย USB Serial ถูกส่งต่อเข้าหัวข้อ MQTT อัตโนมัติ!*
4. ทดสอบสั่งเปิด-ปิดไฟผ่าน REST API:
```powershell
curl -X POST http://localhost:5000/api/led/on
```
*สังเกตว่าคำสั่ง `LED:ON` ถูกส่งผ่านสาย USB เข้าไปควบคุมบอร์ดทันที!*

---

## 4. คำถามท้ายการทดลอง พร้อมเฉลยคำตอบ

#### 1. สถาปัตยกรรมแบบ Serial-to-MQTT Bridge นี้ช่วยให้อุปกรณ์รุ่นเก่า (Legacy Hardware) ที่ไม่มี Wi-Fi สามารถเข้าร่วมระบบนิเวศ IoT สมัยใหม่ได้อย่างไร?
* **คำตอบ:**
  * **Protocol Transformation & Gateway Pattern:** อุปกรณ์ฮาร์ดแวร์รุ่นเก่าหรือเครื่องจักรในงานอุตสาหกรรม (Industrial Legacy Systems) มักใช้การสื่อสารแบบอนุกรม (Serial RS-232, RS-485 Modbus, UART) และไม่มี Network Stack (TCP/IP) หรือโมดูล Wi-Fi/Ethernet ในตัวเอง อีกทั้งไม่สามารถเข้ารหัสความปลอดภัยสมัยใหม่ (TLS/SSL) ได้
  * **บทบาทของ Edge Gateway ในการเชื่อมต่อ:**
    1. **Physical & Protocol Adaptation:** Edge Gateway ทำหน้าที่เป็น "ล่ามสื่อสาร" ดักฟังและรับแพ็กเก็ตข้อมูลดิบ (Raw Bytes, ASCII, JSON) จากสาย Serial โดยตรง
    2. **Modernization to Standard Protocols:** นำข้อมูลที่สกัดได้มาแปลงให้อยู่ในรูปแบบสากล (JSON Schema) แล้ว Publish เข้าสู่ MQTT Topics หรือเปิดช่องทาง RESTful API ให้อุปกรณ์หรือระบบ Cloud สมัยใหม่ดึงไปใช้งานได้ทันที
    3. **Secure Proxy:** ทำหน้าที่เป็นตัวแทนด้านความปลอดภัย (Security Proxy) ในการเชื่อมต่อกับเครือข่ายภายนอก โดยเพิ่มการเข้ารหัสและระบบความปลอดภัยระดับสากลแทนอุปกรณ์ปลายทาง
  * **คุ้มค่าทางเศรษฐศาสตร์ (Preserving Legacy Investments):** โรงงานไม่จำเป็นต้องรื้อถอนหรือจัดซื้อเครื่องจักรราคาแพงใหม่ แต่สามารถนำอุปกรณ์เดิมมาผสานรวมเข้าสู่ระบบ Industry 4.0, Real-time Dashboard และ Cloud Analytics ได้ทันที

---

#### 2. เพราะเหตุใดเราจึงต้องใช้ `BackgroundService` ในการอ่านข้อมูล Serial แทนที่จะเปิดพอร์ตอ่านตรงๆ ภายในฟังก์ชันของ REST API?
* **คำตอบ:**
  * **ปัญหาด้าน I/O Blocking และ Request-Response Lifecycle:**
    * สัญญาณ Serial จากไมโครคอนโทรลเลอร์เป็นสตรีมข้อมูลต่อเนื่อง (Continuous Stream) หรือเกิดขึ้นแบบ Event-Driven หากนำการเปิดพอร์ตและคำสั่งอ่าน `_serial.ReadLine()` ไปใส่ไว้ในฟังก์ชันของ REST API เธรดของ HTTP Request จะต้องถูกบล็อก (Block Thread) เพื่อรอข้อมูลจากฮาร์ดแวร์ ซึ่งอาจทำให้เกิด Request Timeout และกิน Connection Pool ของ Kestrel จนระบบไม่สามารถรองรับผู้ใช้คนอื่นได้
  * **ปัญหาการแย่งชิงพอร์ตสื่อสาร (Exclusive Port Lock / Concurrency Conflict):**
    * ในระบบปฏิบัติการ (โดยเฉพาะ Windows) พอร์ต COM จะถูกเปิดใช้งานได้เพียง **1 โปรเซส/1 เธรดในเวลาเดียวกันเท่านั้น (Exclusive Access)** หากมีไคลเอนต์เรียก REST API เข้ามาพร้อมกันหลายคำขอ (Concurrent Requests) คำขอที่สองเป็นต้นไปจะเปิดพอร์ตไม่สำเร็จทันทีและเกิดข้อผิดพลาด `IOException: Port is already open` หรือ Access Denied
  * **ข้อได้เปรียบของการใช้ `BackgroundService` (`SerialBridgeWorker`):**
    1. **Dedicated Long-Running Worker:** ทำงานเป็นเบื้องหลังแบบต่อเนื่องตลอดอายุของ Gateway ถือครอง Handle ของ Serial Port เพียงตัวเดียวอย่างปลอดภัย (Thread-Safe)
    2. **Decoupling Data Ingestion from Serving:** แยกส่วนการรับข้อมูล (Ingestion) ออกจากส่วนการให้บริการข้อมูล (Serving) อย่างชัดเจน โดย BackgroundService จะคอยอ่านข้อมูลแล้วนำไปบันทึกลงแคชหน่วยความจำ (`GatewayStateStore`)
    3. **Instantaneous Response Time ($O(1)$ Lookup):** เมื่อมีคำขอเรียกเข้ามาที่ REST API (`GET /api/telemetry/latest`) ฟังก์ชันสามารถหยิบค่าล่าสุดจากหน่วยความจำส่งคืนให้ผู้ใช้ได้ทันทีในเวลาไม่ถึง 1 มิลลิวินาที โดยไม่ต้องรอการตอบสนองทางกายภาพจากสาย Serial เลย
