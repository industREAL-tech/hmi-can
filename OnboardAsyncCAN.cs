using industREAL.CAN.CORE;
using Iot.Device.SocketCan;
using System;
using System.Collections.Concurrent;


namespace industREAL.HMI.CAN
{
    public sealed class OnboardAsyncCAN : IDisposable, IAsyncDisposable
    {
        #region LOCALS
        // ------- TX queue + signal for single-writer pattern -------
        private readonly ConcurrentQueue<CanFrame> _txQueue = new();
        private readonly SemaphoreSlim _txSignal = new(0);

        // ------- Batch buffer for "multiple frame" mode -------
        private readonly object _multiLock = new();
        private List<CanFrame> _multiFrames = new();

        // Raised by FlushMultipleFrames after queuing N frames
        public event Action<int>? OnMultipleFramesFlushed;

        private CancellationTokenSource? _cts;
        private Task? _writerTask, _readerTask;
        #endregion LOCALS

        #region DELEGATES
        public delegate void CanFrameReceived(CanFrame receivedFrame);
        public event CanFrameReceived? FrameReceived;
        #endregion DELEGATES

        public async Task StartCanLoopAsync(/*string iface = "can0"*/)
        {
            Log.Info("Starting SocketCAN loop setup.");
            var iface = "can0"; // Assuming a default interface for logging

            // var can = new CanRaw(iface);
            var can = new CanRaw();

            // Setup CTS and Tasks
            _cts?.Cancel();
            _cts?.Dispose();
            _cts = new CancellationTokenSource();
            var token = _cts.Token;

            _writerTask = Task.Run(() => WriteLoopAsync(can, token), token);
            _readerTask = Task.Run(() => ReadLoopAsync(can, token), token);

            Log.Info("CAN reader and writer tasks started on interface {0}.", iface);

            try
            {
                await Task.WhenAll(_writerTask, _readerTask);
            }
            catch (OperationCanceledException)
            {
                Log.Info("CAN loop explicitly cancelled.");
            }
            catch (Exception ex)
            {
                // Log unhandled exceptions that crash the loop
                Log.Fatal(ex, "SocketCAN loop crashed unexpectedly.");
            }
            finally
            {
                can.Dispose();
                Log.Info("SocketCAN loop finished and interface disposed.");
            }
        }

        public async Task StopCanLoopAsync()
        {
            Log.Info("Attempting to stop CAN loops.");
            if (_cts == null) return;
            _cts.Cancel();

            // Unblock writer if it's waiting
            _txSignal.Release();

            try { if (_writerTask != null) await _writerTask; }
            catch (OperationCanceledException) { /* expected */ }
            catch (Exception ex) { Log.Error(ex, "Error waiting for writer task to stop."); }

            try { if (_readerTask != null) await _readerTask; }
            catch (OperationCanceledException) { /* expected */ }
            catch (Exception ex) { Log.Error(ex, "Error waiting for reader task to stop."); }

            _cts.Dispose();
            _cts = null;
            _writerTask = _readerTask = null;
            Log.Debug("CAN loop tasks successfully stopped and disposed.");
        }

        /// <summary>
        /// Enqueue a single frame for immediate TX (outside of "multiple" mode).
        /// </summary>
        public void Enqueue(CanFrame frame)
        {
            _txQueue.Enqueue(DeepCopy(frame));
            _txSignal.Release();
            Log.Trace("Enqueued single frame ID 0x{0:X8}.", frame._canID);
        }

        // ===================== REQUIRED METHODS =====================

        /// <summary>
        /// Adds a frame to the current "multiple frames" batch.
        /// (Deep-copied so caller can reuse buffers.)
        /// </summary>
        public void addToMultipleFrame(CanFrame outFrame)
        {
            lock (_multiLock)
            {
                _multiFrames.Add(DeepCopy(outFrame));
            }
            // Log.Trace("Added frame ID 0x{0:X8} to batch.", outFrame._canID); // Too noisy for trace
        }

        /// <summary>
        /// Flushes the accumulated batch into the TX queue and signals the writer.
        /// Indicates the flush by logging and raising OnMultipleFramesFlushed(count).
        /// </summary>
        public void FlushMultipleFrames()
        {
            List<CanFrame> batch;
            lock (_multiLock)
            {
                if (_multiFrames.Count == 0)
                {
                    Log.Trace("FlushMultipleFrames called, but batch was empty.");
                    return;
                }

                // Swap-out to minimize lock time
                batch = _multiFrames;
                _multiFrames = new List<CanFrame>(capacity: 16);
            }

            foreach (var f in batch)
                _txQueue.Enqueue(f);

            // Wake writer; one release is enough (writer drains), but releasing count is fine too.
            _txSignal.Release(batch.Count);

            Log.Debug("Flushed {Count} CAN frame(s) to TX queue.", batch.Count);

            OnMultipleFramesFlushed?.Invoke(batch.Count);
        }

        // ==================== INTERNAL LOOPS ========================

        private async Task WriteLoopAsync(CanRaw can, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    await _txSignal.WaitAsync(token);
                }
                catch (OperationCanceledException) { break; } // Exit loop if cancelled

                while (_txQueue.TryDequeue(out var f))
                {
                    try
                    {
                        var id = BuildCanId(f);
                        can.WriteFrame(f._data, id);

                        // Use Debug level to log individual frame transmission
                        Log.Debug("TX 0x{0:X8} | Data: {1}", id.Value, string.Join(" ", f._data.Select(b => b.ToString("X2"))));
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex)
                    {
                        // Log the error but continue trying to process the queue
                        Log.Error(ex, "CAN write failed for frame ID 0x{0:X8}.", f._canID);
                        // Optional: break; // if you want to bail out on repeated errors
                    }
                }
            }
            Log.Debug("Writer loop exiting.");
        }

        private async Task ReadLoopAsync(CanRaw can, CancellationToken token)
        {
            var buf = new byte[64];
            while (!token.IsCancellationRequested)
            {
                try
                {
                    if (can.TryReadFrame(buf, out int len, out CanId id))
                    {
                        // Log successful read at Trace or Debug level
                        Log.Trace("RX 0x{0:X8} | Data length: {1}", id.Value, len);

                        CanFrame receivedFrame = new((CanDLC)len);
                        receivedFrame._canID = id.Value;
                        Array.Copy(buf, receivedFrame._data, len);

                        // Log received data at Debug level for analysis
                        Log.Debug("RX 0x{0:X8} | Data: {1}", id.Value, string.Join(" ", buf.AsSpan(0, len).ToArray().Select(b => b.ToString("X2"))));

                        FrameReceived?.Invoke(receivedFrame);
                    }
                    else
                    {
                        // Slight delay to prevent busy-waiting if TryReadFrame returns immediately false
                        await Task.Delay(1, token);
                    }
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex)
                {
                    Log.Error(ex, "CAN read failed. Delaying before retry.");
                    // Delay is important here to avoid continuously hammering the CPU on a persistent error
                    await Task.Delay(10, token);
                }
            }
            Log.Debug("Reader loop exiting.");
        }

        // ===================== HELPERS and DISPOSE ==============================

        private static CanFrame DeepCopy(CanFrame src)
        {
            // ... (DeepCopy logic remains unchanged) ...
            var copy = new CanFrame(src._dataLength)
            {
                _canID = src._canID,
                _flags = src._flags,
                _frameID = src._frameID,
                _channel = src._channel
            };
            Buffer.BlockCopy(src._data, 0, copy._data, 0, (byte)src._dataLength);
            return copy;
        }

        private static CanId BuildCanId(CanFrame f)
        {
            // ... (BuildCanId logic remains unchanged) ...
            var id = new CanId();

            if ((f._flags & CanFlags.ExtendedId) != 0)
            {
                // 29-bit EID
                id.Extended = f._canID & 0x1FFFFFFFu;
            }
            else
            {
                // 11-bit SID
                id.Standard = (ushort)(f._canID & 0x7FFu);
            }
            return id;
        }

        public async ValueTask DisposeAsync()
        {
            await StopCanLoopAsync();
            _txSignal.Dispose();
            Log.Debug("AsyncCAN component fully disposed asynchronously.");
        }

        public void Dispose()
        {
            // Note: Calling GetResult() on StopCanLoopAsync can block in some environments, 
            // but is required for synchronous Dispose().
            StopCanLoopAsync().GetAwaiter().GetResult();
            _txSignal.Dispose();
            Log.Debug("AsyncCAN component fully disposed synchronously.");
        }
    }
}