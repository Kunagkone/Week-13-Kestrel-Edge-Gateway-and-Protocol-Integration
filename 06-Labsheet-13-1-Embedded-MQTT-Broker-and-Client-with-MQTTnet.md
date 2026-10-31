# ใบงานการทดลองที่ 13.1
### การสร้าง Embedded MQTT Broker และ Client ภายใน Kestrel .NET 8 ด้วยไลบรารี MQTTnet

> [!NOTE] **คำชี้แจง**
> ในใบงานนี้นักศึกษาจะได้เปลี่ยนเว็บเซิร์ฟเวอร์ ASP.NET Core Kestrel ให้กลายเป็น **Embedded MQTT Broker** แบบเบ็ดเสร็จในตัวเอง โดยใช้ไลบรารี **`MQTTnet`** ทำให้แอปพลิเคชัน C# เพียงตัวเดียวสามารถรับการเชื่อมต่อจาก MQTT Client ภายนอกผ่านพอร์ต 1883 และสามารถดักจับ (Intercept) ข้อความเพื่อนำมาประมวลผลภายในได้อย่างมีประสิทธิภาพ โดยไม่ต้องติดตั้ง Mosquitto ภายนอก

---

## 1. วัตถุประสงค์การทดลอง
1. สามารถสร้างโปรเจกต์ ASP.NET Core Web Application (.NET 8) และติดตั้งแพ็กเกจ `MQTTnet.AspNetCore` ได้
2. สามารถกำหนดค่า Kestrel ให้เปิดพอร์ต TCP 1883 สำหรับให้บริการ Embedded MQTT Server ได้
3. สามารถเขียน Event Interceptor เพื่อดักจับข้อความ Inbound และบันทึก Log ลงในระบบได้
4. สามารถทดสอบการเชื่อมต่อ การ Publish และการ Subscribe ผ่านเครื่องมือ Mosquitto CLI หรือ MQTTX ได้

---

## 2. โครงสร้างสถาปัตยกรรม (Architecture Diagram)

<p align="center"><img src="Images/Fig_06-Labsheet-13-1_academic.svg" alt="06-Labsheet-13-1-Embedded-MQTT-Broker-and-Client-with-MQTTnet" width="400" style="  height: auto;" ></p>
<p align="center"><b></b>โครงสร้างสถาปัตยกรรม (Architecture Diagram)</p>

---

## 3. ขั้นตอนการทดลอง (Step-by-Step Activities)

### กิจกรรมที่ 13-1.1: การสร้างโปรเจกต์และติดตั้งแพ็กเกจ

#### 1. สร้างโปรเจกต์ .NET 8 Web ใหม่
เปิด PowerShell ในโฟลเดอร์สำหรับทำงาน:
```powershell
dotnet new web -n ConvergedEdgeGateway
cd ConvergedEdgeGateway
```

#### 2. ติดตั้งแพ็กเกจ `MQTTnet` และ `MQTTnet.AspNetCore`
```powershell
dotnet add package MQTTnet --version 4.3.7.1207
dotnet add package MQTTnet.AspNetCore --version 4.3.7.1207
```

---

### กิจกรรมที่ 13-1.2: การเขียนโค้ดตั้งค่าเซิร์ฟเวอร์ (`Program.cs`)

เปิดไฟล์ `Program.cs` และแทนที่ด้วยโค้ดต่อไปนี้:

```csharp
using MQTTnet;
using MQTTnet.AspNetCore;
using MQTTnet.Server;

var builder = WebApplication.CreateBuilder(args);

// 1. กำหนดค่า Kestrel ให้เปิดทั้งพอร์ต HTTP (5000) และ MQTT TCP (1883)
builder.WebHost.ConfigureKestrel(options =>
{
    // พอร์ตสำหรับ HTTP REST APIs และเว็บแดชบอร์ด
    options.ListenAnyIP(5000);

    // พอร์ตสำหรับ Embedded MQTT Broker
    options.ListenAnyIP(1883, listenOptions =>
    {
        listenOptions.UseMqtt();
    });
});

// 2. ลงทะเบียนบริการ MQTT Server
builder.Services.AddMqttServer(options =>
{
    options.WithDefaultEndpoint();
});

var app = builder.Build();

// 3. ผูก Event Handlers สำหรับจัดการข้อความใน Broker
app.UseMqttServer(server =>
{
    // ตรวจสอบการเชื่อมต่อ: กรณี Client ไม่ได้ระบุ ClientId (เช่น mosquitto CLI ที่ไม่ได้ใส่ -i)
    // ให้ Broker สุ่ม ClientId ให้โดยอัตโนมัติ เพื่อป้องกัน Connection Refused: identifier rejected
    server.ValidatingConnectionAsync += args =>
    {
        if (string.IsNullOrEmpty(args.ClientId))
        {
            args.AssignedClientIdentifier = Guid.NewGuid().ToString("N");
        }
        return Task.CompletedTask;
    };

    // เมื่อมี Client เชื่อมต่อเข้ามา
    server.ClientConnectedAsync += args =>
    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"[MQTT Server] 🟢 Client เชื่อมต่อ: {args.ClientId} (จาก Endpoint: {args.Endpoint})");
        Console.ResetColor();
        return Task.CompletedTask;
    };

    // เมื่อ Client ตัดการเชื่อมต่อ
    server.ClientDisconnectedAsync += args =>
    {
        Console.ForegroundColor = ConsoleColor.DarkYellow;
        Console.WriteLine($"[MQTT Server] 🔴 Client ตัดการเชื่อมต่อ: {args.ClientId}");
        Console.ResetColor();
        return Task.CompletedTask;
    };

    // ดักจับและอ่านข้อความที่ถูก Publish ทุกฉบับ
    server.InterceptingPublishAsync += args =>
    {
        var topic = args.ApplicationMessage.Topic;
        var payload = args.ApplicationMessage.ConvertPayloadToString();
        var qos = args.ApplicationMessage.QualityOfServiceLevel;

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine($"[MQTT Message] 📩 หัวข้อ: {topic} | QoS: {qos} | ข้อความ: {payload}");
        Console.ResetColor();

        return Task.CompletedTask;
    };
});

// 4. จุดทดสอบ REST Endpoint เบื้องต้น
app.MapGet("/", () => "🚀 Kestrel Converged Edge Gateway with Embedded MQTTnet Broker is RUNNING!");

app.Run();
```

---

### กิจกรรมที่ 13-1.3: การสั่งรันและทดสอบด้วยเครื่องมือ CLI

#### 1. สั่งรัน Kestrel Edge Gateway
```powershell
dotnet run
```
*สังเกตข้อความบนคอนโซลว่าเซิร์ฟเวอร์กำลังฟังที่พอร์ต 5000 (HTTP) และ 1883 (MQTT)*

#### 2. เปิดหน้าต่าง PowerShell ที่สอง: ทดสอบ Subscribe หัวข้อ
ใช้คำสั่ง `mosquitto_sub` เพื่อดักฟังข้อความจาก Embedded Broker ของเรา (ระบุพารามิเตอร์ `-i` เพื่อกำหนด Client ID):
```powershell
mosquitto_sub -h 127.0.0.1 -p 1883 -t "test/edge/topic" -i "sub_client" -v
```
> **ข้อสังเกต:** มาตรฐาน MQTT กำหนดให้ Client ต้องมี Client ID เสมอ สำหรับเครื่องมือ `mosquitto_sub` บน Windows หากไม่ระบุออปชัน `-i` มันจะส่ง Client ID ว่างเปล่ามา ซึ่งในโค้ดฝั่ง Broker เราได้ตั้งค่า `ValidatingConnectionAsync` ให้สุ่ม Client ID ให้โดยอัตโนมัติแล้ว

*สังเกตหน้าต่างคอนโซลของ Kestrel จะขึ้นข้อความสีเขียวว่ามี Client เชื่อมต่อเข้ามา!*

#### 3. เปิดหน้าต่าง PowerShell ที่สาม: ทดสอบ Publish ข้อความ
ส่งข้อความผ่าน `mosquitto_pub`:
```powershell
mosquitto_pub -h 127.0.0.1 -p 1883 -t "test/edge/topic" -m "Hello from CLI via Kestrel Embedded MQTT!" -i "pub_client"
```

#### 4. สังเกตผลลัพธ์
* หน้าต่าง Kestrel จะพิมพ์ Log สีฟ้าว่าดักจับข้อความได้ถูกต้อง
* หน้าต่าง `mosquitto_sub` จะได้รับข้อความที่ถูกส่งต่อผ่าน Broker สำเร็จทันที

---

## 4. คำถามท้ายการทดลอง (Review Questions) พร้อมเฉลยคำตอบ

#### 1. การรัน Embedded MQTT Broker ภายใน Kestrel มีข้อได้เปรียบกว่าการติดตั้งซอฟต์แวร์ Mosquitto แยกต่างหากอย่างไรในแง่ของการกระจายงาน (Deployment) และการประมวลผลข้อมูลในหน่วยความจำ?
* **คำตอบ:**
  1. **ความสะดวกในการกระจายงานและการบำรุงรักษา (Deployment & Single-Process Simplicity):**
     * **Self-Contained All-in-One Application:** ยุบรวมเว็บเซิร์ฟเวอร์ (Kestrel HTTP/REST), SignalR (WebSocket) และ MQTT Broker ไว้ในโปรเซสของแอปพลิเคชัน C# เพียงตัวเดียว ทำให้สะดวกในการนำไปติดตั้ง (Deploy) สามารถรันเป็น Single Binary หรือ Docker Container เดี่ยวได้ทันที โดยไม่ต้องติดตั้ง ดูแลรักษา หรือคอนฟิก Service Daemon ของ Mosquitto ภายนอก
     * **ลดภาระด้านโครงสร้างพื้นฐาน:** ลดปัญหาความเข้ากันได้ของเวอร์ชันซอฟต์แวร์ (Version Incompatibility), ปัญหา Port Collision บนเครื่องโฮสต์ และลดจุดล้มเหลว (Single Point of Failure) ในการบริหารจัดการเซอร์วิสของระบบปฏิบัติการ
  2. **ประสิทธิภาพในการประมวลผลข้อมูลในหน่วยความจำ (In-Memory Processing & Zero Network Latency):**
     * **Zero Network Overhead:** เซอร์วิสอื่นๆ ภายในแอปพลิเคชัน C# (เช่น SerialBridgeWorker, GatewayHub หรือ REST Controllers) สามารถดักจับและฉีดข้อมูลเข้า Broker ได้โดยตรงผ่าน In-Memory API (InjectApplicationMessage) โดยไม่ต้องเปิด TCP Socket วนลูปกลับมาที่ Loopback Interface (127.0.0.1) ซ้ำซ้อน ซึ่งช่วยประหยัด CPU และ Socket Descriptors
     * **Direct Shared State:** ข้อมูล Payload สามารถส่งเข้าสู่โครงสร้างข้อมูลหน่วยความจำ (เช่น ConcurrentDictionary หรือ In-Memory Cache) ได้ทันที ลดความหน่วงเวลา (Latency) เหลือระดับไมโครวินาที และลดภาระทรัพยากรใน Edge Device ที่มีสเปกจำกัด

---

#### 2. ฟังก์ชัน InterceptingPublishAsync ใน MQTTnet สามารถนำไปประยุกต์ใช้ในการกรองข้อมูลที่ผิดปกติ (Edge Validation) ก่อนส่งต่อให้ Client อื่นได้อย่างไร?
* **คำตอบ:**
  * **หลักการทำงาน:** InterceptingPublishAsync ทำหน้าที่เป็น Middleware Pipeline Event Hook ภายใน Broker ที่จะถูกเรียกใช้งานแบบ Asynchronous ทุกครั้งที่มี Client ส่งข้อความ (Publish) เข้ามา **ก่อน** ที่ข้อความนั้นจะถูกกระจาย (Dispatch) ไปยัง Client อื่นๆ ที่ Subscribe ไว้
  * **แนวทางการนำไปประยุกต์ใช้:**
    1. **Data Validation & Range Checking (ตรวจสอบความถูกต้องของข้อมูล):** แปลง Payload (เช่น JSON) และตรวจสอบช่วงค่าเซนเซอร์ที่สมเหตุสมผล เช่น ตรวจสอบว่าแรงดันไฟฟ้าต้องอยู่ระหว่าง .0 - 3.3\text{ V}$ หากพบค่าที่หลุดนอกช่วง (Outlier/Sensor Fault) สามารถตรวจจับและติด Flag ได้ทันทีที่ระดับ Edge
    2. **Message Dropping (การสั่งทิ้งข้อความที่ผิดปกติ):** หากตรวจสอบพบว่าข้อมูลมีรูปแบบไม่ปลอดภัย ผิดมาตรฐาน หรือเป็นสแปม สามารถสั่งตัดทิ้งได้ทันทีโดยการตั้งค่า rgs.ProcessPublish = false; ซึ่งทำให้ Broker ยกเลิกการกระจายข้อความนั้นไปยัง Subscribers ปลายทางทันที
    3. **Data Sanitization & Enrichment (การเติมเต็มข้อมูล):** สามารถแทรก Metadata สำคัญ เช่น เพิ่ม Timestamp เวลาสากลจาก Gateway, พิกัด Edge Gateway หรือระดับความปลอดภัย ก่อนส่งต่อไปยัง Cloud หรือ Dashboard
    4. **Access Control & Rate Limiting:** สามารถตรวจเช็กว่า rgs.ClientId มีสิทธิ์ในการ Publish ลง Topic ดังกล่าวหรือไม่ และสามารถตรวจจับอัตราความถี่ (Rate Limiting) เพื่อป้องกันเซนเซอร์หรืออุปกรณ์ที่ทำงานผิดปกติยิงทราฟฟิกล้น Broker (Flood/DoS Attack)
