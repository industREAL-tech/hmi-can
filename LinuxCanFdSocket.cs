// Native SocketCAN raw layer with CAN-FD support.
//
// Iot.Device.SocketCan.CanRaw does not enable CAN_RAW_FD_FRAMES, so the
// kernel silently drops every received CAN-FD frame. We bypass it and
// talk to the kernel directly:
//
//   fd = socket(PF_CAN, SOCK_RAW, CAN_RAW)
//   setsockopt(fd, SOL_CAN_RAW, CAN_RAW_FD_FRAMES, 1)
//   ioctl(fd, SIOCGIFINDEX, &ifr)            // resolve canX -> ifindex
//   bind(fd, &sockaddr_can, sizeof(...))
//   read/write canfd_frame (72 B) or can_frame (16 B)
//
// Read length tells classical vs FD apart: 16 = struct can_frame,
// 72 = struct canfd_frame. Writes pick the size based on the frame's
// FD flag and payload length.
//
// Linux only — see RuntimeInformation guard at the top of Open().

using System;
using System.IO;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace industREAL.HMI.CAN
{
    /// Linux SocketCAN raw socket with CAN-FD enabled.
    public sealed class LinuxCanFdSocket : IDisposable
    {
        // ---- SocketCAN constants ----
        private const int AF_CAN              = 29;
        private const int PF_CAN              = AF_CAN;
        private const int SOCK_RAW            = 3;
        private const int CAN_RAW             = 1;
        private const int SOL_CAN_BASE        = 100;
        private const int SOL_CAN_RAW         = SOL_CAN_BASE + CAN_RAW;
        private const int CAN_RAW_FILTER      = 1;
        private const int CAN_RAW_FD_FRAMES   = 5;

        private const int SIOCGIFINDEX        = 0x8933;

        public const uint CAN_EFF_FLAG        = 0x80000000U;
        public const uint CAN_RTR_FLAG        = 0x40000000U;
        public const uint CAN_ERR_FLAG        = 0x20000000U;
        public const uint CAN_SFF_MASK        = 0x000007FFU;
        public const uint CAN_EFF_MASK        = 0x1FFFFFFFU;

        public const byte CANFD_BRS           = 0x01;
        public const byte CANFD_ESI           = 0x02;
        public const byte CANFD_FDF           = 0x04;     // Linux ≥ 5.16

        public const int  CAN_MTU             = 16;
        public const int  CANFD_MTU           = 72;

        // ---- libc ----
        [DllImport("libc", SetLastError = true)] static extern int  socket(int domain, int type, int protocol);
        [DllImport("libc", SetLastError = true)] static extern int  close(int fd);
        [DllImport("libc", SetLastError = true)] static extern int  bind(int fd, ref SockaddrCan addr, int addrlen);
        [DllImport("libc", SetLastError = true)] static extern int  ioctl(int fd, ulong req, ref Ifreq ifr);
        [DllImport("libc", SetLastError = true)] static extern int  setsockopt(int fd, int level, int optname, ref int optval, int optlen);
        [DllImport("libc", SetLastError = true)] static extern long read (int fd, byte[] buf, UIntPtr count);
        [DllImport("libc", SetLastError = true)] static extern long write(int fd, byte[] buf, UIntPtr count);
        [DllImport("libc", SetLastError = true)] static extern int  fcntl(int fd, int cmd, int arg);
        [DllImport("libc", SetLastError = true)] static extern int  poll([In, Out] PollFd[] fds, uint nfds, int timeout);

        [StructLayout(LayoutKind.Sequential)]
        private struct Ifreq
        {
            // sizeof(struct ifreq) on Linux is 40 (16 + 24 union).
            // We only need ifr_name + ifr_ifindex.
            public Ifreq16 Name;
            public int     IfIndex;
            // Pad to 40 bytes total.
            public int Pad0;
            public int Pad1;
            public int Pad2;
            public int Pad3;
            public int Pad4;
        }

        [StructLayout(LayoutKind.Sequential, Size = 16)]
        private struct Ifreq16 { public long L0; public long L1; }

        [StructLayout(LayoutKind.Sequential)]
        private struct SockaddrCan
        {
            public ushort can_family;
            public int    can_ifindex;
            public uint   rx_id;
            public uint   tx_id;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct PollFd
        {
            public int    fd;
            public short  events;
            public short  revents;
        }

        private const short POLLIN  = 0x0001;

        // ---- State ----
        private int    _fd = -1;
        private string _iface = "";

        public bool   IsOpen => _fd >= 0;
        public string Iface  => _iface;

        public void Open(string iface)
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                throw new PlatformNotSupportedException("SocketCAN is Linux-only.");

            int fd = socket(PF_CAN, SOCK_RAW, CAN_RAW);
            if (fd < 0) throw Last("socket(PF_CAN)");

            try
            {
                int one = 1;
                if (setsockopt(fd, SOL_CAN_RAW, CAN_RAW_FD_FRAMES, ref one, sizeof(int)) < 0)
                    throw Last("setsockopt(CAN_RAW_FD_FRAMES)");

                var ifr = new Ifreq();
                WriteIfaceName(ref ifr, iface);
                if (ioctl(fd, SIOCGIFINDEX, ref ifr) < 0)
                    throw Last($"ioctl(SIOCGIFINDEX, {iface})");

                var addr = new SockaddrCan { can_family = AF_CAN, can_ifindex = ifr.IfIndex };
                if (bind(fd, ref addr, Marshal.SizeOf<SockaddrCan>()) < 0)
                    throw Last("bind(sockaddr_can)");
            }
            catch
            {
                close(fd);
                throw;
            }

            _fd    = fd;
            _iface = iface;
        }

        private static void WriteIfaceName(ref Ifreq ifr, string iface)
        {
            // ifr_name is char[16] NUL-terminated.
            var bytes = Encoding.ASCII.GetBytes(iface);
            if (bytes.Length >= 16) throw new ArgumentException("iface name too long");
            unsafe
            {
                fixed (Ifreq16* p = &ifr.Name)
                {
                    byte* b = (byte*)p;
                    for (int i = 0; i < 16; i++) b[i] = 0;
                    for (int i = 0; i < bytes.Length; i++) b[i] = bytes[i];
                }
            }
        }

        // ---- Read ----

        /// Blocks up to `timeoutMs` for a frame. Returns false on timeout.
        public bool TryRead(out uint canId, out byte length, out byte fdFlags, byte[] data, int timeoutMs)
        {
            canId = 0; length = 0; fdFlags = 0;
            if (_fd < 0) throw new InvalidOperationException("socket closed");

            // Wait for data with poll() so the cancellation path can interrupt.
            var pfd = new PollFd[] { new() { fd = _fd, events = POLLIN } };
            int pr = poll(pfd, 1, timeoutMs);
            if (pr <= 0) return false;
            if ((pfd[0].revents & POLLIN) == 0) return false;

            // Buffer must hold the largest frame (CANFD_MTU = 72).
            byte[] buf = new byte[CANFD_MTU];
            long n = read(_fd, buf, (UIntPtr)buf.Length);
            if (n < 0)
            {
                int err = Marshal.GetLastWin32Error();
                if (err == 11 /*EAGAIN*/ || err == 4 /*EINTR*/) return false;
                throw new IOException($"read failed errno={err}");
            }
            if (n == CAN_MTU)
            {
                canId   = BitConverter.ToUInt32(buf, 0);
                length  = buf[4];           // can_dlc 0..8
                fdFlags = 0;
                int copy = Math.Min(length, data.Length);
                Buffer.BlockCopy(buf, 8, data, 0, copy);
                return true;
            }
            if (n == CANFD_MTU)
            {
                canId   = BitConverter.ToUInt32(buf, 0);
                length  = buf[4];           // canfd_frame.len 0..64
                fdFlags = (byte)(buf[5] | CANFD_FDF);  // mark as FD even on older kernels
                int copy = Math.Min(length, data.Length);
                Buffer.BlockCopy(buf, 8, data, 0, copy);
                return true;
            }
            // Unexpected size — drop the frame, do not block.
            return false;
        }

        // ---- Write ----

        /// Send a frame. fdFlags may include CANFD_BRS/ESI/FDF; if any
        /// is set or `length > 8`, the canfd_frame layout is used.
        public void Write(uint canId, ReadOnlySpan<byte> data, byte fdFlags)
        {
            if (_fd < 0) throw new InvalidOperationException("socket closed");
            bool fd = fdFlags != 0 || data.Length > 8;
            if (!fd) WriteClassic(canId, data);
            else     WriteFd     (canId, data, fdFlags);
        }

        private void WriteClassic(uint canId, ReadOnlySpan<byte> data)
        {
            if (data.Length > 8) throw new ArgumentException("classic frame > 8 bytes");
            var buf = new byte[CAN_MTU];
            BitConverter.TryWriteBytes(buf.AsSpan(0, 4), canId);
            buf[4] = (byte)data.Length;
            data.CopyTo(buf.AsSpan(8));
            long w = write(_fd, buf, (UIntPtr)buf.Length);
            if (w != CAN_MTU) throw Last("write(can_frame)");
        }

        private void WriteFd(uint canId, ReadOnlySpan<byte> data, byte fdFlags)
        {
            if (data.Length > 64) throw new ArgumentException("FD frame > 64 bytes");
            var buf = new byte[CANFD_MTU];
            BitConverter.TryWriteBytes(buf.AsSpan(0, 4), canId);
            buf[4] = (byte)data.Length;
            buf[5] = (byte)(fdFlags & 0x07);
            data.CopyTo(buf.AsSpan(8));
            long w = write(_fd, buf, (UIntPtr)buf.Length);
            if (w != CANFD_MTU) throw Last("write(canfd_frame)");
        }

        private static IOException Last(string what)
        {
            int err = Marshal.GetLastWin32Error();
            return new IOException($"{what} failed errno={err}");
        }

        public void Dispose()
        {
            if (_fd >= 0) { close(_fd); _fd = -1; }
        }
    }

}
