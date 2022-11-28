using System.Runtime.CompilerServices;

[module: SkipLocalsInit]

namespace Demikernel;

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;

public readonly struct DKSocket : IDisposable
{
    private readonly int _qd;
    private readonly DKSocketManager _manager;
    // ~DKSocket() => ReleaseHandle(); // uncomment if we go for class

    public static unsafe void Initialize(params string[] args)
    {
        CheckSizes();
        args ??= Array.Empty<string>();
        int len = args.Length; // for the NUL terminators
        if (args.Length > 128) throw new ArgumentOutOfRangeException("Too many args, sorry");
        foreach (var arg in args)
        {
            len += Encoding.ASCII.GetByteCount(arg);
        }
        var lease = ArrayPool<byte>.Shared.Rent(len);
        fixed (byte* raw = lease)
        {
            byte** argv = stackalloc byte*[args.Length];
            int byteOffset = 0;
            for (int i = 0; i < args.Length; i++)
            {
                argv[i] = &raw[byteOffset];
                var arg = args[i];
                var count = Encoding.ASCII.GetBytes(arg, 0, arg.Length, lease, byteOffset);
                byteOffset += count;
                lease[byteOffset++] = 0; // NUL terminator
            }
            int result;
            lock (Interop.GlobalLock)
            {
                result = Interop.init(args.Length, argv);
            }
            Interop.Assert(result, nameof(Interop.init));
        }
        ArrayPool<byte>.Shared.Return(lease);
    }

    private static void CheckSizes()
    {
        if (Unsafe.SizeOf<ScatterGatherSegment>() != Sizes.SCATTER_GATHER_SEGMENT)
            throw new System.PlatformNotSupportedException("Invalid size for ScatterGatterSegment structure.");
        if (Unsafe.SizeOf<ScatterGatherArray>() != Sizes.SCATTER_GATHER_ARRAY)
            throw new System.PlatformNotSupportedException("Invalid size for ScatterGatterArray structure.");
        if (Unsafe.SizeOf<AcceptResult>() != Sizes.ACCEPT_RESULT)
            throw new System.PlatformNotSupportedException("Invalid size for AcceptResult structure.");
        if (Unsafe.SizeOf<QueueResult>() != Sizes.QUEUE_RESULT)
            throw new System.PlatformNotSupportedException("Invalid size for QueueResult structure.");
    }

    public static unsafe DKSocket Create(AddressFamily addressFamily, SocketType socketType, ProtocolType protocolType, DKSocketManager? manager = null)
    {
        int qd = 0, result;
        lock (Interop.GlobalLock)
        {
            result = Interop.socket(&qd, addressFamily, socketType, protocolType);
        }
        Interop.Assert(result, nameof(Interop.socket));
        return new DKSocket(qd, manager);
    }
    internal DKSocket(int qd, DKSocketManager? manager)
    {
        _qd = qd;
        _manager = manager ?? DKSocketManager.Shared;
    }
    public void Close() => Dispose();
    public void Dispose()
    {
        GC.SuppressFinalize(this);
        Free();
    }

    private void Free()
    {
        // do our best to prevent double-release, noting that this isn't perfect (think: struct copies)
        var qd = Interlocked.Exchange(ref Unsafe.AsRef(in _qd), 0);
        if (qd != 0)
        {
            int result;
            lock (Interop.GlobalLock)
            {
                result = Interop.close(qd);
            }
            Interop.Assert(result, nameof(Interop.close));
        }
    }

    public override string ToString() => $"LibOS socket {_qd}";

    public void Listen(int backlog)
    {
        int result;
        lock (Interop.GlobalLock)
        {
            result = Interop.listen(_qd, backlog);
        }
        Interop.Assert(result, nameof(Interop.listen));
    }

    public unsafe void Bind(EndPoint endpoint)
    {
        var socketAddress = endpoint.Serialize();
        var len = socketAddress.Size;
        if (len > 128) throw new ArgumentException($"Socket address is oversized: {len} bytes");

        var saddr = stackalloc byte[len];
        for (int i = 0; i < len; i++)
        {
            saddr[i] = socketAddress[i];
        }
        int result;
        lock (Interop.GlobalLock)
        {
            result = Interop.bind(_qd, saddr, len);
        }
        Interop.Assert(result, nameof(Interop.bind));
    }

    public unsafe ValueTask<AcceptResult> AcceptAsync(CancellationToken cancellationToken = default)
    {
        Unsafe.SkipInit(out QueueToken qt);
        Unsafe.SkipInit(out QueueResult qr);
        int result, syncResult = -1;
        lock (Interop.GlobalLock)
        {
            result = Interop.accept(&qt.qt, _qd);
            if (result == 0)
            {
                fixed (TimeSpec* ptr = &TimeSpec.Zero)
                {
                    syncResult = Interop.wait(&qr, qt.qt, ptr);
                }
            }
        }
        Interop.Assert(result, nameof(Interop.accept));
        if (syncResult == 0)
        {
            qr.Assert(Opcode.Accept);
            return new(qr.ares);
        }
        return new(_manager.AddAccept(qt, cancellationToken));
    }

    public unsafe AcceptResult Accept()
    {
        Unsafe.SkipInit(out QueueToken qt);
        int result;
        lock (Interop.GlobalLock)
        {
            result = Interop.accept(&qt.qt, _qd);
        }
        Interop.Assert(result, nameof(Interop.accept));
        return qt.WaitAccept();
    }

    public unsafe ValueTask<ScatterGatherArray> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        Unsafe.SkipInit(out QueueToken qt);
        Unsafe.SkipInit(out QueueResult qr);
        int result, syncResult = -1;
        lock (Interop.GlobalLock)
        {
            result = Interop.pop(&qt.qt, _qd);
            if (result == 0)
            {
                fixed (TimeSpec* ptr = &TimeSpec.Zero)
                {
                    syncResult = Interop.wait(&qr, qt.qt, ptr);
                }
            }
        }
        Interop.Assert(result, nameof(Interop.pop));
        if (syncResult == 0)
        {
            qr.Assert(Opcode.Pop);
            return new(qr.sga);
        }
        return new(_manager.AddReceive(qt, cancellationToken));
    }

    public unsafe ScatterGatherArray Receive()
    {
        Unsafe.SkipInit(out QueueToken qt);
        int result;
        lock (Interop.GlobalLock)
        {
            result = Interop.pop(&qt.qt, _qd);
        }
        Interop.Assert(result, nameof(Interop.pop));
        return qt.WaitReceive();
    }

    public unsafe ValueTask SendAsync(in ScatterGatherArray payload, CancellationToken cancellationToken = default)
    {
        if (payload.IsEmpty) return default;
        Unsafe.SkipInit(out QueueToken qt);
        Unsafe.SkipInit(out QueueResult qr);
        int result, syncResult = -1;
        fixed (ScatterGatherArray* ptr = &payload)
        {
            lock (Interop.GlobalLock)
            {
                result = Interop.push(&qt.qt, _qd, ptr);
                if (result == 0)
                {
                    fixed (TimeSpec* timePtr = &TimeSpec.Zero)
                    {
                        syncResult = Interop.wait(&qr, qt.qt, timePtr);
                    }
                }
            }
        }
        Interop.Assert(result, nameof(Interop.push));
        if (syncResult == 0)
        {
            qr.Assert(Opcode.Push);
            return default;
        }
        return new(_manager.AddSend(qt, cancellationToken));
    }
    public unsafe void Send(in ScatterGatherArray payload)
    {
        if (payload.IsEmpty) return;
        Unsafe.SkipInit(out QueueToken qt);
        int result;
        fixed (ScatterGatherArray* ptr = &payload)
        {
            lock (Interop.GlobalLock)
            {
                result = Interop.push(&qt.qt, _qd, ptr);
            }
        }
        Interop.Assert(result, nameof(Interop.push));
        qt.WaitSend();
    }


    public ValueTask SendAsync(ReadOnlySpan<byte> payload, CancellationToken cancellationToken = default)
    {
        if (payload.IsEmpty) return default;
        using var sga = ScatterGatherArray.Create(payload);
        // this looks like we're disposing too soon, but actually it is
        // fine; you can "sgafree" as soon as the "push" has been started
        return SendAsync(in sga, cancellationToken);
    }

    public void Send(ReadOnlySpan<byte> payload)
    {
        if (payload.IsEmpty) return;
        using var sga = ScatterGatherArray.Create(payload);
        Send(sga);
    }
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = sizeof(long))]
internal readonly struct QueueToken : IEquatable<QueueToken>
{
    [FieldOffset(0)]
    internal readonly long qt;

    internal unsafe void WaitSend()
    {
        Unsafe.SkipInit(out QueueResult qr);
        int result;
        lock (Interop.GlobalLock)
        {
            result = Interop.wait(&qr, this.qt, null);
        }
        Interop.Assert(result, nameof(Interop.wait));
        qr.Assert(Opcode.Push);
    }

    internal unsafe AcceptResult WaitAccept()
    {
        Unsafe.SkipInit(out QueueResult qr);
        int result;
        lock (Interop.GlobalLock)
        {
            result = Interop.wait(&qr, this.qt, null);
        }
        Interop.Assert(result, nameof(Interop.wait));
        qr.Assert(Opcode.Accept);
        return qr.ares;
    }

    internal unsafe ScatterGatherArray WaitReceive()
    {
        Unsafe.SkipInit(out QueueResult qr);
        int result;
        lock (Interop.GlobalLock)
        {
            result = Interop.wait(&qr, this.qt, null);
        }
        Interop.Assert(result, nameof(Interop.wait));
        qr.Assert(Opcode.Pop);
        return qr.sga;
    }

    public override string ToString() => $"Queue-token {qt}";

    public override int GetHashCode() => qt.GetHashCode();

    public override bool Equals([NotNullWhen(true)] object? obj)
        => obj is QueueToken other && other.qt == qt;

    public bool Equals(QueueToken other) => other.qt == qt;
}

internal static class Sizes
{
    public const int SOCKET_ADDRESS = 16;
    public const int SCATTER_GATHER_SEGMENT = 16;
    public const int SCATTER_GATHER_ARRAY = 48;
    public const int ACCEPT_RESULT = 20;
    public const int QUEUE_RESULT_VALUE = 48;
    public const int QUEUE_RESULT = 64;
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 16)]
internal readonly unsafe struct ScatterGatherSegment
{
    [FieldOffset(0)] private readonly byte* _buf;

    [FieldOffset(8)] private readonly uint _len;

    public Span<byte> Span => new Span<byte>(_buf, checked((int)_len));
    public uint Length => _len;

    public string Raw => new IntPtr(_buf).ToString();

    internal ScatterGatherSegment UncheckedSlice(uint start, uint length)
        => new ScatterGatherSegment(_buf + start, length);

    private ScatterGatherSegment(byte* buf, uint len)
    {
        _buf = buf;
        _len = len;
    }
}
[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Sizes.SCATTER_GATHER_ARRAY)]
public unsafe readonly struct ScatterGatherArray : IDisposable
{
    public const int MAX_SEGMENTS = 1;
    [FieldOffset(0)] private readonly void* buf;
    [FieldOffset(8)] private readonly uint _numsegs;
    [FieldOffset(16)] private readonly ScatterGatherSegment _firstSegment; // [Sizes.SCATTER_GATHER_SEGMENT * MAX_SEGMENTS];
    [FieldOffset(32)] private readonly byte _saddrStart; //[Sizes.SOCKET_ADDRESS];

    static void ThrowMultiSegmentNotExpected()
        => throw new NotSupportedException("Multi-segment buffers not currently anticipated");

    public uint Count => _numsegs;


    public ScatterGatherArray Slice(uint start, uint length)
    {
        static void ThrowOutOfRange() => throw new ArgumentOutOfRangeException();
        var currentLen = TotalBytes;
        if (start + length > currentLen) ThrowOutOfRange();
        if (length == 0) return default;

        if (_numsegs != 1) ThrowMultiSegmentNotExpected();

        var result = this; // copy
        var typed = &result._firstSegment;
        *typed = typed->UncheckedSlice(start, length);
        return result;
    }

    public bool IsEmpty => _numsegs switch
    {
        0 => true,
        1 => _firstSegment.Length == 0,
        _ => IsEmptySlow(),
    };
    private bool IsEmptySlow()
    {
        ThrowMultiSegmentNotExpected(); // but impl shown for future ref
        fixed (ScatterGatherSegment* segs = &_firstSegment)
        {
            for (int i = 0; i < _numsegs; i++)
            {
                if (segs[i].Length != 0) return false;
            }
        }
        return true;
    }
    public bool IsSingleSegment => _numsegs == 1;

    public Span<byte> FirstSpan => _numsegs switch
    {
        0 => default,
        _ => _firstSegment.Span,
    };

    public uint TotalBytes => _numsegs switch
    {
        0 => 0,
        1 => _firstSegment.Length,
        _ => TotalBytesSlow(),
    };

    private uint TotalBytesSlow()
    {
        ThrowMultiSegmentNotExpected(); // but impl shown for future ref
        uint total = 0;
        fixed (ScatterGatherSegment* segs = &_firstSegment)
        {
            for (int i = 0; i < _numsegs; i++)
            {
                total += segs[i].Length;
            }
        }
        return total;
    }

    public Span<byte> this[int index]
        => index == 0 & _numsegs != 0 ? _firstSegment.Span : IndexerSlow(index);

    private Span<byte> IndexerSlow(int index)
    {
        if (index < 0 || index >= _numsegs) Throw();
        ThrowMultiSegmentNotExpected(); // but impl shown for future ref

        fixed (ScatterGatherSegment* segs = &_firstSegment)
        {
            return segs[index].Span;
        }
        static void Throw() => throw new IndexOutOfRangeException();
    }

    public override string ToString()
        => $"{TotalBytes} bytes over {Count} segments";

    public void Dispose()
    {
        if (!IsEmpty)
        {
            int result;
            fixed (ScatterGatherArray* ptr = &this)
            {
                lock (Interop.GlobalLock)
                {
                    result = Interop.sgafree(ptr);
                }
            }
            Interop.Assert(result, nameof(Interop.sgafree));
            Unsafe.AsRef(in this) = default; // pure evil, but: we do what we can
        }
    }

    private static readonly ScatterGatherArray _empty;
    public ref readonly ScatterGatherArray Empty => ref _empty;
    public static ScatterGatherArray Create(uint size)
    {
        if (size == 0) return default;
        ScatterGatherArray sga;
        lock (Interop.GlobalLock)
        {
            sga = Interop.sgaalloc(size);
        }

        if (sga._numsegs == 0 | sga._numsegs > MAX_SEGMENTS) Throw(sga._numsegs, size);

        static void Throw(uint numsegs, uint size) =>
            throw new InvalidOperationException($"Invalid segment count: {numsegs} ({nameof(Create)} requested {size} bytes)");
        return sga;
    }

    public bool TryCopyFrom(ReadOnlySpan<byte> payload)
    {
        if (payload.IsEmpty) return true;
        if (IsSingleSegment) return payload.TryCopyTo(FirstSpan);
        return TrySlowCopyFrom(payload);
    }
    private bool TrySlowCopyFrom(ReadOnlySpan<byte> payload)
    {
        for (int i = 0; i < _numsegs; i++)
        {
            var available = this[i];
            if (available.Length >= payload.Length)
            {
                // all fits
                payload.CopyTo(available);
                return true;
            }
            // partial fit
            payload.Slice(0, available.Length).CopyTo(available);
            payload = payload.Slice(available.Length);
        }
        return payload.IsEmpty;
    }

    internal static ScatterGatherArray Create(ReadOnlySpan<byte> payload)
    {
        var sga = Create((uint)payload.Length);
        if (!sga.TryCopyFrom(payload))
        {
            sga.Dispose();
            Throw();
        }
        return sga;

        static void Throw() => throw new InvalidOperationException("Unable to copy payload to ScatterGatherArray");
    }
}
public enum Opcode
{
    Invalid = 0,
    Push,
    Pop,
    Accept,
    Connect,
    Failed,
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 20)]
public readonly struct AcceptResult
{
    public const int AddressLength = Sizes.SOCKET_ADDRESS;

    [FieldOffset(0)]
    private readonly int _qd;

    [FieldOffset(4)]
    private readonly byte _saddrStart; // [Sizes.SOCKET_ADDRESS];

    public DKSocket AsSocket(DKSocketManager? manager = null) => new DKSocket(_qd, manager);

    public unsafe void CopyAddressTo(Span<byte> bytes)
    {
        fixed (byte* ptr = &_saddrStart)
        {
            new Span<byte>(ptr, Sizes.SOCKET_ADDRESS).CopyTo(bytes);
        }
    }

    public override int GetHashCode() => _qd;

    public override bool Equals([NotNullWhen(true)] object? obj)
        => obj is AcceptResult other && other._qd == _qd;

    public override unsafe string ToString()
    {
        var c = stackalloc char[Sizes.SOCKET_ADDRESS * 2];
        int offset = 0;
        fixed (byte* ptr = &_saddrStart)
        {
            string Hex = "0123456789abcdef";
            for (int i = 0; i < Sizes.SOCKET_ADDRESS; i++)
            {
                c[offset++] = Hex[ptr[i] & 0x0F];
                c[offset++] = Hex[ptr[i] >> 4];
            }
        }
        return new string(c, 0, Sizes.SOCKET_ADDRESS * 2);
    }
}

[StructLayout(LayoutKind.Explicit, Pack = 1)]
internal readonly struct QueueResult
{
    [FieldOffset(0)]
    private readonly Opcode _opcode;
    [FieldOffset(4)]
    private readonly int _qd;
    [FieldOffset(8)]
    private readonly long _qt;
    [FieldOffset(16)]
    internal readonly ScatterGatherArray sga;
    [FieldOffset(16)]
    internal readonly AcceptResult ares;

    public Opcode Opcode => _opcode;
    public override string ToString() => $"Queue-result for '{_opcode}' on socket {_qd}/{_qt}";

    internal void Assert(Opcode expected)
    {
        if (expected != _opcode) ThrowUnexpected(expected);
    }
    private void ThrowUnexpected(Opcode expected) => throw CreateUnexpected(expected);
    internal Exception CreateUnexpected(Opcode expected) => new InvalidOperationException($"Opcode failure; expected {expected}, actually {_opcode}");
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 16)]
internal readonly struct TimeSpec
{
    [FieldOffset(0)]
    internal readonly long _seconds;
    [FieldOffset(8)]
    internal readonly ulong _nanoseconds;

    internal TimeSpec(long seconds, ulong nanoseconds)
    {
        this._seconds = seconds;
        this._nanoseconds = nanoseconds;
    }

    private static readonly TimeSpec _zero = new(0, 0);

    internal static ref readonly TimeSpec Zero => ref _zero;

    internal static TimeSpec Create(TimeSpan value)
    {
        var ticksSubSecond = value.Ticks % TimeSpan.TicksPerSecond;
        const long TICKS_PER_NANOSECOND = 100;
        var seconds = (long)value.TotalSeconds;
        var nanos = ticksSubSecond / TICKS_PER_NANOSECOND;
        if (nanos < 0)
        {
            seconds--;
            nanos = nanos + 1_000_000_000;
        }
        return new TimeSpec(seconds, (ulong)nanos);
    }
}

#pragma warning disable CA1401 // P/Invokes should not be visible
internal static unsafe class Interop
{
    internal static void Assert(int err, string cause)
    {
        if (err != 0) Throw(err, cause);
        static void Throw(int err, string cause) => throw new LibOsException(err, cause);
    }
    private sealed class LibOsException : IOException
    {
        public int ErrorNumber { get; set; }
        public LibOsException(int err, string cause) : base($"LibOS reported error from '{cause}': {err}")
            => ErrorNumber = err;
    }

    internal static readonly object GlobalLock = new object();

    [DllImport("libdemikernel", EntryPoint = "demi_init")]

    public static extern int init(int argc, byte** args);


    [DllImport("libdemikernel", EntryPoint = "demi_socket")]
    public static extern int socket(int* qd, AddressFamily domain, SocketType type, ProtocolType protocol);

    [DllImport("libdemikernel", EntryPoint = "demi_listen")]
    public static extern int listen(int qd, int backlog);

    [DllImport("libdemikernel", EntryPoint = "demi_bind")]
    public static extern int bind(int qd, byte* saddr, int size);

    [DllImport("libdemikernel", EntryPoint = "demi_accept")]
    public static extern int accept(long* qt, int sockqd);

    [DllImport("libdemikernel", EntryPoint = "demi_connect")]
    public static extern int connect(long* qt, int qd, byte* saddr, int size);

    [DllImport("libdemikernel", EntryPoint = "demi_close")]
    public static extern int close(int qd);

    [DllImport("libdemikernel", EntryPoint = "demi_push")]
    internal static extern int push(long* qt, int qd, ScatterGatherArray* sga);

    [DllImport("libdemikernel", EntryPoint = "demi_pushto")]
    internal static extern int pushto(long* qt, int qd, ScatterGatherArray* sga, byte* saddr, int size);

    [DllImport("libdemikernel", EntryPoint = "demi_pop")]
    public static extern int pop(long* qt, int qd);

    [DllImport("libdemikernel", EntryPoint = "demi_wait")]
    public static extern int wait(QueueResult* qr, long qt, TimeSpec* timeout);

    [DllImport("libdemikernel", EntryPoint = "demi_wait_any")]
    public static extern int wait_any(QueueResult* qr, int* offset, long* qt, int num_qts, TimeSpec* timeout);

    [DllImport("libdemikernel", EntryPoint = "demi_sgaalloc")]
    public static extern ScatterGatherArray sgaalloc(ulong size);

    [DllImport("libdemikernel", EntryPoint = "demi_sgafree")]
    public static extern int sgafree(ScatterGatherArray* sga);
}