namespace BackendServer.Models;

public static class CommandNames
{
    public const string Forward = "FORWARD";
    public const string Backward = "BACKWARD";
    public const string VfdIncrease = "VFD_INCREASE";
    public const string VfdDecrease = "VFD_DECREASE";
    public const string FrontBrake = "FRONT_BRAKE";
    public const string RearBrake = "REAR_BRAKE";
    public const string Brake = "BRAKE";
    public const string Emergency = "EMERGENCY";
    public const string AutonomousOn = "AUTONOMOUS_ON";
    public const string AutonomousOff = "AUTONOMOUS_OFF";
    public const string Reset = "RESET";

    public static readonly HashSet<string> All = new()
    {
        Forward,
        Backward,
        VfdIncrease,
        VfdDecrease,
        FrontBrake,
        RearBrake,
        Brake,
        Emergency,
        AutonomousOn,
        AutonomousOff,
        Reset
    };
}
