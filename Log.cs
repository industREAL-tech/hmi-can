// Minimal logging facade. Drop-in shim until a proper logger (NLog,
// Serilog, Microsoft.Extensions.Logging) is wired. Keeps OnboardAsyncCAN
// compiling and routes output to the trace listener.

using System;
using System.Diagnostics;

namespace industREAL.HMI.CAN
{
    public static class Log
    {
        public static void Trace(string fmt, params object?[] args) => Write("TRACE", fmt, args);
        public static void Debug(string fmt, params object?[] args) => Write("DEBUG", fmt, args);
        public static void Info (string fmt, params object?[] args) => Write("INFO ", fmt, args);
        public static void Warn (string fmt, params object?[] args) => Write("WARN ", fmt, args);

        public static void Error(Exception ex, string fmt, params object?[] args)
            => Write("ERROR", fmt + " :: " + ex.Message, args);

        public static void Fatal(Exception ex, string fmt, params object?[] args)
            => Write("FATAL", fmt + " :: " + ex.Message, args);

        private static void Write(string level, string fmt, object?[] args)
        {
            string msg;
            try   { msg = string.Format(fmt, args); }
            catch { msg = fmt; }
            System.Diagnostics.Debug.WriteLine($"[{level}] {msg}");
            Console.Error.WriteLine($"[{level}] {msg}");
        }
    }
}
