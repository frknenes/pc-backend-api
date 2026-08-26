using BackendServer.Models;
using BackendServer.Hubs;
using Microsoft.AspNetCore.SignalR;
using System.Globalization;

namespace BackendServer.Services;

public class TelemetryService
{
    private readonly IHubContext<TelemetryHub> _hub;
    private readonly LoggingService _logger;

    public TelemetryService(IHubContext<TelemetryHub> hub, LoggingService logger)
    {
        _hub = hub;
        _logger = logger;
    }

    public async Task HandleRawTelemetry(string rawData)
    {
        var telemetry = ParseTelemetry(rawData);

        if (telemetry == null)
        {
            _logger.TelemetryError("Telemetry parse edilemedi: " + rawData);
            return;
        }

        // UI'ya model olarak gönderiyor
        await _hub.Clients.All.SendAsync("telemetry", telemetry);
    }

    private TelemetryData? ParseTelemetry(string raw)
    {
        try
        {
            var separatorIndex = raw.IndexOf('|');
            var deviceId = separatorIndex >= 0 ? raw[..separatorIndex] : "SYSTEM";
            var payload = separatorIndex >= 0 ? raw[(separatorIndex + 1)..] : raw;

            if (string.IsNullOrWhiteSpace(payload)) return null;

            var telemetry = new TelemetryData
            {
                DeviceId = deviceId,
            };

            var fields = payload.Split(',');
            var hasTelemetryData = false;

            foreach (var field in fields)
            {
                var kv = field.Split(':');
                if (kv.Length != 2) continue;

                var key = kv[0].Trim().ToUpperInvariant();
                var valueStr = kv[1];

                if (!double.TryParse(valueStr, NumberStyles.Any, CultureInfo.InvariantCulture, out double value))
                    continue;

                if (double.IsNaN(value) || double.IsInfinity(value))
                    continue;

                switch (key)
                {
                    // Temperature
                    case "BT1":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT1 = value; 
                        hasTelemetryData = true;
                        break;
                    case "BT2": 
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT2 = value; 
                        hasTelemetryData = true;
                        break;
                    case "BT3": 
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT3 = value; 
                        hasTelemetryData = true;
                        break;
                    case "BT4":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT4 = value;
                        hasTelemetryData = true;
                        break;
                    case "BT5":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT5 = value;
                        hasTelemetryData = true;
                        break;
                    case "BT6":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT6 = value;
                        hasTelemetryData = true;
                        break;
                    case "BT7":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT7 = value;
                        hasTelemetryData = true;
                        break;
                    case "BT8":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT8 = value;
                        hasTelemetryData = true;
                        break;
                    case "BT9":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT9 = value;
                        hasTelemetryData = true;
                        break;
                    case "BT10":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT10 = value;
                        hasTelemetryData = true;
                        break;
                    case "BT11":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT11 = value;
                        hasTelemetryData = true;
                        break;
                    case "BT12":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT12 = value;
                        hasTelemetryData = true;
                        break;
                    case "BT13":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT13 = value;
                        hasTelemetryData = true;
                        break;
                    case "BT14":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT14 = value;
                        hasTelemetryData = true;
                        break;
                    case "BT15":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT15 = value;
                        hasTelemetryData = true;
                        break;
                    case "BT16":
                        telemetry.Temperature ??= new TemperatureData();
                        telemetry.Temperature.BT16 = value;
                        hasTelemetryData = true;
                        break;
                    
                    // Current
                    case "I1": 
                        telemetry.Current ??= new CurrentData();
                        telemetry.Current.I1 = value; 
                        hasTelemetryData = true;
                        break;
                    case "I2": 
                        telemetry.Current ??= new CurrentData();
                        telemetry.Current.I2 = value; 
                        hasTelemetryData = true;
                        break;
                    case "I3": 
                        telemetry.Current ??= new CurrentData();
                        telemetry.Current.I3 = value; 
                        hasTelemetryData = true;
                        break;
                   
                    // Voltage
                    case "V1": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V1 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V2": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V2 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V3": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V3 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V4":
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V4 = value;
                        hasTelemetryData = true;
                        break;
                    case "V5":
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V5 = value;
                        hasTelemetryData = true;
                        break;
                    case "V6": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V6 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V7": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V7 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V8": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V8 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V9": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V9 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V10": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V10 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V11": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V11 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V12": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V12 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V13": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V13 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V14": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V14 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V15": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V15 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V16": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V16 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V17": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V17 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V18": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V18 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V19": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V19 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V20": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V20 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V21": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V21 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V22": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V22 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V23": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V23 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V24": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V24 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V25": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V25 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V26": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V26 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V27": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V27 = value; 
                        hasTelemetryData = true;
                        break;
                    case "V28": 
                        telemetry.Voltage ??= new VoltageData();
                        telemetry.Voltage.V28 = value; 
                        hasTelemetryData = true;
                        break;

                    // Motion Temperature
                    case "MT1": 
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.MT1 = value; 
                        hasTelemetryData = true;
                        break;
                    case "MT2": 
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.MT2 = value; 
                        hasTelemetryData = true;
                        break;
                    case "MT3": 
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.MT3 = value; 
                        hasTelemetryData = true;
                        break;

                    // Motion Speed
                    case "SX": 
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.SX = value; 
                        hasTelemetryData = true;
                        break;
                    case "SY": 
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.SY = value; 
                        hasTelemetryData = true;
                        break;
                    case "SZ": 
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.SZ = value; 
                        hasTelemetryData = true;
                        break;

                    // Location
                    case "LX": 
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.LX = value; 
                        hasTelemetryData = true;
                        break;
                    case "LY": 
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.LY = value; 
                        hasTelemetryData = true;
                        break;
                    case "LZ": 
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.LZ = value; 
                        hasTelemetryData = true;
                        break;

                    // Acceleration
                    case "AX": 
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.AX = value; 
                        hasTelemetryData = true;
                        break;
                    case "AY": 
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.AY = value; 
                        hasTelemetryData = true;
                        break;
                    case "AZ": 
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.AZ = value; 
                        hasTelemetryData = true;
                        break;

                    // Roll-Pıtch-Yaw
                    case "RX":
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.RX = value;
                        hasTelemetryData = true;
                        break;
                    case "PX":
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.PX = value;
                        hasTelemetryData = true;
                        break;
                    case "YX":
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.YX = value;
                        hasTelemetryData = true;
                        break;

                    // Momentary/Average speed
                    /*case "MS":
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.MS = value;
                        break;
                    */

                    case "AS":
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.AS = value;
                        hasTelemetryData = true;
                        break;

                    // Reflector counter
                    case "RC1":
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.RC1= value;
                        hasTelemetryData = true;
                        break;
                    case "RC2":
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.RC2 = value;
                        hasTelemetryData = true;
                        break;
                    case "RC3":
                        telemetry.Motion ??= new MotionData();
                        telemetry.Motion.RC3 = value;
                        hasTelemetryData = true;
                        break;

                    // Pressure
                    case "P1":
                        telemetry.Pressure ??= new PressureData();
                        telemetry.Pressure.P1  = value;
                        hasTelemetryData = true;
                        break;
                    case "P2":
                        telemetry.Pressure ??= new PressureData();
                        telemetry.Pressure.P2 = value;
                        hasTelemetryData = true;
                        break;

                    // Power
                    case "PW1":
                        telemetry.Power ??= new PowerData();
                        telemetry.Power.PW1 = value;
                        hasTelemetryData = true;
                        break;

                    // Emergency
                    case "ACIL_DURUM":
                        telemetry.Emergency ??= new EmergencyData();
                        telemetry.Emergency.ACIL_DURUM = (int)value ;
                        hasTelemetryData = true;
                        break;

                    // STM32 autonomous drive feedback
                    case "F":
                        telemetry.AutonomousDrive ??= new AutonomousDriveData();
                        telemetry.AutonomousDrive.F = (int)value;
                        hasTelemetryData = true;
                        break;
                    case "B":
                        telemetry.AutonomousDrive ??= new AutonomousDriveData();
                        telemetry.AutonomousDrive.B = (int)value;
                        hasTelemetryData = true;
                        break;
                    case "BR":
                        telemetry.AutonomousDrive ??= new AutonomousDriveData();
                        telemetry.AutonomousDrive.BR = (int)value;
                        hasTelemetryData = true;
                        break;
                    case "E":
                        telemetry.AutonomousDrive ??= new AutonomousDriveData();
                        telemetry.AutonomousDrive.E = (int)value;
                        hasTelemetryData = true;
                        break;
                }
            }

            return hasTelemetryData ? telemetry : null;
        }
        catch (Exception ex)
        {
            _logger.TelemetryError("ParseTelemetry exception: " + ex.Message);
            return null;
        }
    }
}
