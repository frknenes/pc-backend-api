using BackendServer.Models;
using BackendServer.Hubs;
using Microsoft.AspNetCore.SignalR;
using System.Globalization;
using System.Text.RegularExpressions;

namespace BackendServer.Services;

public class TelemetryService : IHostedService, IDisposable
{
    private readonly IHubContext<TelemetryHub> _hub;
    private readonly LoggingService _logger;
    private readonly object _telemetryLock = new();
    private readonly TimeSpan _fieldStaleTimeout;
    private readonly Dictionary<string, DateTime> _fieldLastSeenUtc = new(StringComparer.Ordinal);
    private static readonly TimeSpan UiPublishInterval = TimeSpan.FromMilliseconds(200);
    private const double WattsPerKilowatt = 1000d;
    private static readonly Regex TelemetryFieldRegex = new(
        @"(?<key>[A-Za-z_][A-Za-z0-9_]*):(?<value>[-+]?(?:\d+(?:[\.,]\d+)?|[\.,]\d+)(?:[eE][-+]?\d+)?)",
        RegexOptions.Compiled);
    private TelemetryData? _latestTelemetry;
    private bool _hasUnpublishedTelemetry;
    private CancellationTokenSource? _publishCts;
    private Task? _publishTask;

    public TelemetryService(IHubContext<TelemetryHub> hub, LoggingService logger, IConfiguration configuration)
    {
        _hub = hub;
        _logger = logger;
        _fieldStaleTimeout = TimeSpan.FromMilliseconds(Math.Max(
            100,
            configuration.GetValue<int?>("Telemetry:FieldStaleTimeoutMs") ?? 2500));
    }

    public async Task HandleRawTelemetry(string rawData)
    {
        var telemetry = ParseTelemetry(rawData);

        if (telemetry == null)
        {
            _logger.TelemetryError("Telemetry parse edilemedi: " + rawData);
            return;
        }

        lock (_telemetryLock)
        {
            RecordFieldUpdates(telemetry, DateTime.UtcNow);
            _latestTelemetry = MergeTelemetry(_latestTelemetry, telemetry);
            _hasUnpublishedTelemetry = true;
        }

        await Task.CompletedTask;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _publishCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _publishTask = Task.Run(() => PublishTelemetryLoop(_publishCts.Token), CancellationToken.None);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_publishCts == null || _publishTask == null) return;

        await _publishCts.CancelAsync();

        try
        {
            await _publishTask.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
    }

    public void Dispose()
    {
        _publishCts?.Dispose();
    }

    private async Task PublishTelemetryLoop(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(UiPublishInterval);

        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            TelemetryData? snapshot;

            lock (_telemetryLock)
            {
                if (!_hasUnpublishedTelemetry || _latestTelemetry == null) continue;

                snapshot = CloneTelemetry(_latestTelemetry);
                ExpireStaleFields(snapshot, DateTime.UtcNow);
                CalculatePowerConsumption(snapshot);
                _hasUnpublishedTelemetry = false;
            }

            await _hub.Clients.All.SendAsync("telemetry", snapshot, cancellationToken);
        }
    }

    private static TelemetryData MergeTelemetry(TelemetryData? current, TelemetryData incoming)
    {
        current ??= new TelemetryData();
        current.DeviceId = incoming.DeviceId;
        current.TimeStamp = incoming.TimeStamp;

        current.Temperature = MergeSection(current.Temperature, incoming.Temperature);
        current.Current = MergeSection(current.Current, incoming.Current);
        current.Voltage = MergeSection(current.Voltage, incoming.Voltage);
        current.Motion = MergeSection(current.Motion, incoming.Motion);
        current.Pressure = MergeSection(current.Pressure, incoming.Pressure);
        current.Power = MergeSection(current.Power, incoming.Power);
        current.Emergency = MergeSection(current.Emergency, incoming.Emergency);
        current.AutonomousDrive = MergeSection(current.AutonomousDrive, incoming.AutonomousDrive);

        return current;
    }

    private static TelemetryData CloneTelemetry(TelemetryData telemetry)
    {
        return MergeTelemetry(null, telemetry);
    }

    private static T? MergeSection<T>(T? current, T? incoming) where T : class, new()
    {
        if (incoming == null) return current;

        current ??= new T();

        foreach (var property in typeof(T).GetProperties())
        {
            var value = property.GetValue(incoming);
            if (value != null)
            {
                property.SetValue(current, value);
            }
        }

        return current;
    }

    private void RecordFieldUpdates(TelemetryData telemetry, DateTime receivedAtUtc)
    {
        RecordSectionUpdates(nameof(TelemetryData.Temperature), telemetry.Temperature, receivedAtUtc);
        RecordSectionUpdates(nameof(TelemetryData.Current), telemetry.Current, receivedAtUtc);
        RecordSectionUpdates(nameof(TelemetryData.Voltage), telemetry.Voltage, receivedAtUtc);
        RecordSectionUpdates(nameof(TelemetryData.Motion), telemetry.Motion, receivedAtUtc);
        RecordSectionUpdates(nameof(TelemetryData.Pressure), telemetry.Pressure, receivedAtUtc);
        RecordSectionUpdates(nameof(TelemetryData.Power), telemetry.Power, receivedAtUtc);
        RecordSectionUpdates(nameof(TelemetryData.Emergency), telemetry.Emergency, receivedAtUtc);
        RecordSectionUpdates(nameof(TelemetryData.AutonomousDrive), telemetry.AutonomousDrive, receivedAtUtc);
    }

    private void RecordSectionUpdates<T>(string sectionName, T? section, DateTime receivedAtUtc) where T : class
    {
        if (section == null) return;

        foreach (var property in typeof(T).GetProperties())
        {
            if (property.GetValue(section) != null)
                _fieldLastSeenUtc[$"{sectionName}.{property.Name}"] = receivedAtUtc;
        }
    }

    private void ExpireStaleFields(TelemetryData telemetry, DateTime nowUtc)
    {
        ExpireSection(nameof(TelemetryData.Temperature), telemetry.Temperature, nowUtc);
        ExpireSection(nameof(TelemetryData.Current), telemetry.Current, nowUtc);
        ExpireSection(nameof(TelemetryData.Voltage), telemetry.Voltage, nowUtc);
        ExpireSection(nameof(TelemetryData.Motion), telemetry.Motion, nowUtc);
        ExpireSection(nameof(TelemetryData.Pressure), telemetry.Pressure, nowUtc);
        ExpireSection(nameof(TelemetryData.Power), telemetry.Power, nowUtc);
        ExpireSection(nameof(TelemetryData.Emergency), telemetry.Emergency, nowUtc);
        ExpireSection(nameof(TelemetryData.AutonomousDrive), telemetry.AutonomousDrive, nowUtc);
    }

    private void ExpireSection<T>(string sectionName, T? section, DateTime nowUtc) where T : class
    {
        if (section == null) return;

        foreach (var property in typeof(T).GetProperties())
        {
            var key = $"{sectionName}.{property.Name}";
            if (!_fieldLastSeenUtc.TryGetValue(key, out var lastSeenUtc) ||
                nowUtc - lastSeenUtc >= _fieldStaleTimeout)
            {
                property.SetValue(section, null);
            }
        }
    }

    private static void CalculatePowerConsumption(TelemetryData telemetry)
    {
        telemetry.Power ??= new PowerData();

        telemetry.Power.PW1 = SumAvailableValues(
            telemetry.Voltage?.V1,
            telemetry.Voltage?.V2,
            telemetry.Voltage?.V3,
            telemetry.Voltage?.V4,
            telemetry.Voltage?.V5,
            telemetry.Voltage?.V6,
            telemetry.Voltage?.V7,
            telemetry.Voltage?.V8,
            telemetry.Voltage?.V9,
            telemetry.Voltage?.V10,
            telemetry.Voltage?.V11,
            telemetry.Voltage?.V12,
            telemetry.Voltage?.V13,
            telemetry.Voltage?.V14,
            telemetry.Voltage?.V15,
            telemetry.Voltage?.V16,
            telemetry.Voltage?.V17,
            telemetry.Voltage?.V18,
            telemetry.Voltage?.V19,
            telemetry.Voltage?.V20,
            telemetry.Voltage?.V21,
            telemetry.Voltage?.V22,
            telemetry.Voltage?.V23,
            telemetry.Voltage?.V24,
            telemetry.Voltage?.V25,
            telemetry.Voltage?.V26) is { } hvVoltageSum &&
            telemetry.Current?.I3 is { } hvCurrent
                ? ToKilowatts(hvVoltageSum * hvCurrent)
                : null;

        telemetry.Power.PW2 = telemetry.Voltage?.V27 is { } altSystemVoltage &&
                              telemetry.Current?.I1 is { } altSystemCurrent
            ? ToKilowatts(altSystemVoltage * altSystemCurrent)
            : null;

        telemetry.Power.PW3 = telemetry.Voltage?.V28 is { } emergencyVoltage &&
                              telemetry.Current?.I2 is { } emergencyCurrent
            ? ToKilowatts(emergencyVoltage * emergencyCurrent)
            : null;

        telemetry.Power.PW4 = SumAvailableValues(telemetry.Power.PW1, telemetry.Power.PW2, telemetry.Power.PW3);
    }

    private static double ToKilowatts(double watts)
    {
        return watts / WattsPerKilowatt;
    }

    private static double? SumAvailableValues(params double?[] values)
    {
        var sum = 0d;
        var hasValue = false;

        foreach (var value in values)
        {
            if (!value.HasValue) continue;
            hasValue = true;
            sum += value.Value;
        }

        return hasValue ? sum : null;
    }

    private void LogPowerConsumption(TelemetryData telemetry)
    {
        if (telemetry.Power is not { } power ||
            power.PW1 is null && power.PW2 is null && power.PW3 is null && power.PW4 is null)
        {
            return;
        }

        _logger.TelemetryInfo(
            "TX POWER: " +
            $"PW1(HV):{FormatPowerValue(power.PW1)}, " +
            $"PW2(AS):{FormatPowerValue(power.PW2)}, " +
            $"PW3(AD):{FormatPowerValue(power.PW3)}, " +
            $"PW4(T):{FormatPowerValue(power.PW4)}");
    }

    private static string FormatPowerValue(double? value)
    {
        return value.HasValue
            ? value.Value.ToString("0.###", CultureInfo.InvariantCulture)
            : "null";
    }

    private static IEnumerable<(string Key, string Value)> ParseTelemetryFields(string payload)
    {
        foreach (Match match in TelemetryFieldRegex.Matches(payload))
            yield return (match.Groups["key"].Value, match.Groups["value"].Value);
    }

    private static bool TryParseTelemetryDouble(string valueStr, out double value)
    {
        return double.TryParse(
            valueStr.Trim().Replace(',', '.'),
            NumberStyles.Any,
            CultureInfo.InvariantCulture,
            out value);
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

            var hasTelemetryData = false;

            foreach (var (rawKey, valueStr) in ParseTelemetryFields(payload))
            {
                var key = rawKey.Trim().ToUpperInvariant();

                if (!TryParseTelemetryDouble(valueStr, out double value))
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
                    case "PW2":
                        telemetry.Power ??= new PowerData();
                        telemetry.Power.PW2 = value;
                        hasTelemetryData = true;
                        break;
                    case "PW3":
                        telemetry.Power ??= new PowerData();
                        telemetry.Power.PW3 = value;
                        hasTelemetryData = true;
                        break;
                    case "PW4":
                        telemetry.Power ??= new PowerData();
                        telemetry.Power.PW4 = value;
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
