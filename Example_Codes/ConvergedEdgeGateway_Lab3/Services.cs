using System.Collections.Concurrent;
using System.IO.Ports;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
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

// 3. งานเบื้องหลังสำหรับเชื่อมต่อและดักฟัง Serial Port พร้อมโหมดจำลอง (Simulator Mode)
public class SerialBridgeWorker : BackgroundService
{
    private SerialPort? _serial;
    private readonly string _portName = "COM24"; // ปรับให้ตรงกับพอร์ตของ ESP32 บนเครื่องคุณ
    private readonly int _baudRate = 115200;
    private readonly GatewayStateStore _store;
    private readonly MqttServer _mqttServer;
    private readonly ILogger<SerialBridgeWorker> _logger;
    private readonly IHubContext<GatewayHub> _hubContext;
    private readonly ProtocolMetrics _metrics;

    private bool _isSimulationMode = false;
    private bool _simulatedLedState = false;

    public bool IsSimulationMode => _isSimulationMode;
    public bool SimulatedLedState => _simulatedLedState;

    public SerialBridgeWorker(
        GatewayStateStore store,
        MqttServer mqttServer,
        IHubContext<GatewayHub> hubContext,
        ProtocolMetrics metrics,
        ILogger<SerialBridgeWorker> logger)
    {
        _store = store;
        _mqttServer = mqttServer;
        _hubContext = hubContext;
        _metrics = metrics;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("🔌 พยายามเปิด Serial Port {Port}...", _portName);

        // ลองเชื่อมต่อฮาร์ดแวร์จริงก่อน
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
        catch (Exception ex)
        {
            _logger.LogWarning("⚠️ ไม่สามารถเปิด Serial Port จริงได้ ({Message}) -> เข้าสู่โหมดจำลองข้อมูล (ESP32 SIMULATOR MODE) อัตโนมัติ", ex.Message);
            _isSimulationMode = true;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            if (_isSimulationMode)
            {
                // โหมดจำลอง ESP32: สุ่มสร้างค่า Potentiometer (แรงดัน 0.0 - 3.3V และ Raw ADC 0 - 4095)
                double simAngle = DateTime.UtcNow.Ticks / 10000000.0 * 0.8;
                double simVoltage = Math.Round(1.65 + 1.45 * Math.Sin(simAngle), 2);
                int simRaw = (int)Math.Clamp(Math.Round((simVoltage / 3.3) * 4095), 0, 4095);

                string simLine = $"{{\"voltage\":{simVoltage:F2},\"raw\":{simRaw}}}";
                await HandleIncomingSerialLine(simLine);

                await Task.Delay(500, stoppingToken);
                continue;
            }

            // โหมดฮาร์ดแวร์จริง
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
                    _logger.LogInformation("⏳ ยังไม่พบบอร์ด ESP32 ที่ {Port} -> กำลังส่งข้อมูลในโหมดจำลอง...", _portName);
                    _isSimulationMode = true;
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
                _isSimulationMode = true;
            }
        }
    }

    private async Task HandleIncomingSerialLine(string line)
    {
        try
        {
            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;
            double v = root.GetProperty("voltage").GetDouble();
            int r = root.GetProperty("raw").GetInt32();

            var telemetry = new TelemetryData(v, r, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            _store.Set("latest_telemetry", telemetry);

            _metrics.SerialPackets++;

            // 1. ผลักเข้า MQTT
            _metrics.MqttMessages++;
            var mqttMsg = new MqttApplicationMessageBuilder()
                .WithTopic("iot/esp32/telemetry")
                .WithPayload(line)
                .Build();
            await _mqttServer.InjectApplicationMessage(new InjectedMqttApplicationMessage(mqttMsg));

            // 2. ผลักเข้า SignalR หาหน้าเว็บทันที
            _metrics.SignalRPushes++;
            await _hubContext.Clients.All.SendAsync("ReceiveTelemetry", new
            {
                voltage = v,
                raw = r,
                metrics = _metrics
            });
        }
        catch { }
    }

    // ฟังก์ชันส่งคำสั่งย้อนกลับไปหาบอร์ดทาง Serial (รองรับทั้งบอร์ดจริงและ Simulator)
    public bool SendSerialCommand(string cmd)
    {
        if (_serial?.IsOpen == true)
        {
            _serial.WriteLine(cmd);
            _logger.LogInformation("📤 ส่งคำสั่งทาง Serial Port จริง: {Cmd}", cmd);
            return true;
        }

        if (_isSimulationMode)
        {
            if (cmd.Contains("LED:ON", StringComparison.OrdinalIgnoreCase))
            {
                _simulatedLedState = true;
                _logger.LogInformation("💡 [SIMULATOR] จำลองเปิดไฟ LED Status: ON (GPIO2 = HIGH)");
            }
            else if (cmd.Contains("LED:OFF", StringComparison.OrdinalIgnoreCase))
            {
                _simulatedLedState = false;
                _logger.LogInformation("🌑 [SIMULATOR] จำลองปิดไฟ LED Status: OFF (GPIO2 = LOW)");
            }
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
