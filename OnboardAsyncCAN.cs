// Async TX/RX loops over a CAN-FD-capable SocketCAN raw socket.
//
// Replaces the older Iot.Device.SocketCan path which silently dropped
// every CAN-FD frame because it never enabled CAN_RAW_FD_FRAMES.
//
// Public API is unchanged in shape — only the ctor now takes the iface
// name so multiple instances on the same host can talk to can0 / can1
// independently.

using industREAL.CAN.CORE;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace industREAL.HMI.CAN
{
    public sealed class OnboardAsyncCAN : IDisposable, IAsyncDisposable
    {
        // ---- TX queue + signal --------------------------------------------------
        private readonly ConcurrentQueue<CanFrame> _txQueue = new();
        private readonly SemaphoreSlim             _txSignal = new(0);

        // ---- Batch buffer -------------------------------------------------------
        private readonly object              _multiLock   = new();
        private          System.Collections.Generic.List<CanFrame> _multiFrames = new();
        public event Action<int>?            OnMultipleFramesFlushed;

        // ---- Loop state ---------------------------------------------------------
        private CancellationTokenSource? _cts;
        private Task?                    _writerTask, _readerTask;
        private LinuxCanFdSocket?        _socket;
        private readonly string          _iface;

        // ---- Events -------------------------------------------------------------
        public delegate void CanFrameReceived(CanFrame receivedFrame);
        public event CanFrameReceived?   FrameReceived;

        public OnboardAsyncCAN(string iface = "can0") { _iface = iface; }

        public async Task StartCanLoopAsync()
        {
            Log.Info("Starting SocketCAN loop on {0}.", _iface);

            _socket = new LinuxCanFdSocket();
            _socket.Open(_iface);

            _cts?.Cancel();
            _cts?.Dispose();
            _cts  = new CancellationTokenSource();
            var t = _cts.Token;

            _writerTask = Task.Run(() => WriteLoopAsync(t), t);
            _readerTask = Task.Run(() => ReadLoopAsync(t),  t);

            try
            {
                await Task.WhenAll(_writerTask, _readerTask);
            }
            catch (OperationCanceledException) { Log.Info("CAN loop cancelled."); }
            catch (Exception ex)                { Log.Fatal(ex, "SocketCAN loop crashed."); }
            finally
            {
                _socket?.Dispose();
                _socket = null;
                Log.Info("SocketCAN loop finished.");
            }
        }

        public async Task StopCanLoopAsync()
        {
            Log.Info("Stopping CAN loops.");
            if (_cts == null) return;
            _cts.Cancel();
            _txSignal.Release();

            try { if (_writerTask != null) await _writerTask; }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Log.Error(ex, "Error waiting for writer task."); }

            try { if (_readerTask != null) await _readerTask; }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Log.Error(ex, "Error waiting for reader task."); }

            _cts.Dispose();
            _cts = null;
            _writerTask = _readerTask = null;
        }

        // -------- TX queueing API --------

        public void Enqueue(CanFrame frame)
        {
            _txQueue.Enqueue(DeepCopy(frame));
            _txSignal.Release();
        }

        public void addToMultipleFrame(CanFrame outFrame)
        {
            lock (_multiLock) _multiFrames.Add(DeepCopy(outFrame));
        }

        public void FlushMultipleFrames()
        {
            System.Collections.Generic.List<CanFrame> batch;
            lock (_multiLock)
            {
                if (_multiFrames.Count == 0) return;
                batch        = _multiFrames;
                _multiFrames = new(capacity: 16);
            }
            foreach (var f in batch) _txQueue.Enqueue(f);
            _txSignal.Release(batch.Count);
            OnMultipleFramesFlushed?.Invoke(batch.Count);
        }

        // -------- Loops --------

        // A bus that is down (bus-off, a cable out) fails every write -- one line per frame
        // was 266 identical lines in 34 s for a cyclic 15 ms command. The first failure is
        // logged, the rest are counted, and one line says when writes went through again.
        private int _failedWrites;
        private DateTime _firstFailure;

        private async Task WriteLoopAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try { await _txSignal.WaitAsync(token); }
                catch (OperationCanceledException) { break; }

                while (_txQueue.TryDequeue(out var f) && _socket is not null)
                {
                    try
                    {
                        uint id = BuildCanId(f);
                        byte fdFlags = 0;
                        if ((f._flags & CanFlags.FdCanFormat) != 0) fdFlags |= LinuxCanFdSocket.CANFD_FDF;
                        if ((f._flags & CanFlags.BrsOn)       != 0) fdFlags |= LinuxCanFdSocket.CANFD_BRS;
                        _socket.Write(id, new ReadOnlySpan<byte>(f._data, 0, (byte)f._dataLength), fdFlags);
                        if (_failedWrites > 0)
                        {
                            Log.Info("CAN writes go through again, after {0} failed in {1:0.0} s.",
                                     _failedWrites, (DateTime.UtcNow - _firstFailure).TotalSeconds);
                            _failedWrites = 0;
                        }
                    }
                    catch (Exception ex)
                    {
                        if (_failedWrites++ == 0)
                        {
                            _firstFailure = DateTime.UtcNow;
                            Log.Error(ex, "CAN write failed for ID 0x{0:X}; further failures are counted until writes go through again.", f._canID);
                        }
                    }
                }
            }
            if (_failedWrites > 0)
                Log.Warn("CAN writer stopping with {0} write(s) failed in the last {1:0.0} s.",
                         _failedWrites, (DateTime.UtcNow - _firstFailure).TotalSeconds);
            Log.Debug("Writer loop exiting.");
        }

        private async Task ReadLoopAsync(CancellationToken token)
        {
            byte[] buf = new byte[64];
            while (!token.IsCancellationRequested && _socket is not null)
            {
                try
                {
                    if (_socket.TryRead(out uint id, out byte len, out byte fdFlags, buf, timeoutMs: 100))
                    {
                        var (canId, flags) = SplitId(id);
                        if ((fdFlags & LinuxCanFdSocket.CANFD_FDF) != 0) flags |= CanFlags.FdCanFormat;
                        if ((fdFlags & LinuxCanFdSocket.CANFD_BRS) != 0) flags |= CanFlags.BrsOn;

                        var frame = new CanFrame(LengthToDlc(len))
                        {
                            _canID    = canId,
                            _flags    = flags,
                            _channel  = CanInterface.A,   // SocketCAN: single iface per link
                            HostUtc   = DateTime.UtcNow,
                        };
                        Buffer.BlockCopy(buf, 0, frame._data, 0, Math.Min(len, frame._data.Length));
                        FrameReceived?.Invoke(frame);
                    }
                    else
                    {
                        // poll() timed out — yield to scheduler, then loop.
                        await Task.Yield();
                    }
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex)
                {
                    Log.Error(ex, "CAN read failed; backing off.");
                    try { await Task.Delay(20, token); }
                    catch (OperationCanceledException) { return; }
                }
            }
            Log.Debug("Reader loop exiting.");
        }

        // -------- Helpers --------

        private static (uint id, CanFlags flags) SplitId(uint raw)
        {
            CanFlags fl = CanFlags.None;
            if ((raw & LinuxCanFdSocket.CAN_EFF_FLAG) != 0) fl |= CanFlags.ExtendedId;
            if ((raw & LinuxCanFdSocket.CAN_RTR_FLAG) != 0) fl |= CanFlags.RemoteFrame;
            uint id = (fl & CanFlags.ExtendedId) != 0
                ? raw & LinuxCanFdSocket.CAN_EFF_MASK
                : raw & LinuxCanFdSocket.CAN_SFF_MASK;
            return (id, fl);
        }

        private static uint BuildCanId(CanFrame f)
        {
            uint id = (f._flags & CanFlags.ExtendedId) != 0
                ? (f._canID & LinuxCanFdSocket.CAN_EFF_MASK) | LinuxCanFdSocket.CAN_EFF_FLAG
                :  f._canID & LinuxCanFdSocket.CAN_SFF_MASK;
            if ((f._flags & CanFlags.RemoteFrame) != 0) id |= LinuxCanFdSocket.CAN_RTR_FLAG;
            return id;
        }

        private static CanDLC LengthToDlc(byte len) => len switch
        {
            0  => CanDLC.DLC_0,  1  => CanDLC.DLC_1,  2  => CanDLC.DLC_2,  3  => CanDLC.DLC_3,
            4  => CanDLC.DLC_4,  5  => CanDLC.DLC_5,  6  => CanDLC.DLC_6,  7  => CanDLC.DLC_7,
            8  => CanDLC.DLC_8,  12 => CanDLC.DLC_12, 16 => CanDLC.DLC_16, 20 => CanDLC.DLC_20,
            24 => CanDLC.DLC_24, 32 => CanDLC.DLC_32, 48 => CanDLC.DLC_48, _  => CanDLC.DLC_64,
        };

        private static CanFrame DeepCopy(CanFrame src)
        {
            var copy = new CanFrame(src._dataLength)
            {
                _canID   = src._canID,
                _flags   = src._flags,
                _frameID = src._frameID,
                _channel = src._channel,
            };
            Buffer.BlockCopy(src._data, 0, copy._data, 0, (byte)src._dataLength);
            return copy;
        }

        public async ValueTask DisposeAsync()
        {
            await StopCanLoopAsync();
            _txSignal.Dispose();
            _socket?.Dispose();
        }

        public void Dispose()
        {
            StopCanLoopAsync().GetAwaiter().GetResult();
            _txSignal.Dispose();
            _socket?.Dispose();
        }
    }
}
