using System.Runtime.CompilerServices;

[module: SkipLocalsInit]

namespace Demikernel;

using System;
using System.Buffers;
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
            Interop.Assert(Interop.init(args.Length, argv), nameof(Interop.init));
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
        int qd = 0;
        Interop.Assert(Interop.socket(&qd, addressFamily, socketType, protocolType), nameof(Interop.socket));
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
            Interop.Assert(Interop.close(qd), nameof(Interop.close));
        }
    }

    public override string ToString() => $"LibOS socket {_qd}";

    public void Listen(int backlog)
    {
        Interop.Assert(Interop.listen(_qd, backlog), nameof(Interop.listen));
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
        Interop.Assert(Interop.bind(_qd, saddr, len), nameof(Interop.bind));
    }

    public unsafe ValueTask<AcceptResult> AcceptAsync()
    {
        Unsafe.SkipInit(out QueueToken qt);
        Interop.Assert(Interop.accept(&qt.qt, _qd), nameof(Interop.accept));
        // TODO: sync check
        return new(_manager.AddAccept(qt));
    }

    public unsafe AcceptResult Accept()
    {
        Unsafe.SkipInit(out QueueToken qt);
        Interop.Assert(Interop.accept(&qt.qt, _qd), nameof(Interop.accept));
        return qt.WaitAccept();
    }

    public unsafe ValueTask<ScatterGatherArray> ReceiveAsync()
    {
        Unsafe.SkipInit(out QueueToken qt);
        Interop.Assert(Interop.pop(&qt.qt, _qd), nameof(Interop.pop));
        // TODO: sync check
        return new(_manager.AddReceive(qt));
    }

    public unsafe ScatterGatherArray Receive()
    {
        Unsafe.SkipInit(out QueueToken qt);
        Interop.Assert(Interop.pop(&qt.qt, _qd), nameof(Interop.pop));
        return qt.WaitReceive();
    }

    public unsafe ValueTask SendAsync(in ScatterGatherArray payload) // would nice to be "in", but that needs readonly
    {
        payload.AssertValid();
        Unsafe.SkipInit(out QueueToken qt);
        fixed (ScatterGatherArray* ptr = &payload)
        {
            Interop.Assert(Interop.push(&qt.qt, _qd, ptr), nameof(Interop.push));
        }
        // TODO: sync check
        return new(_manager.AddSend(qt));
    }
    public unsafe void Send(in ScatterGatherArray payload)
    {
        payload.AssertValid();
        Unsafe.SkipInit(out QueueToken qt);
        fixed (ScatterGatherArray* ptr = &payload)
        {
            Interop.Assert(Interop.push(&qt.qt, _qd, ptr), nameof(Interop.push));
        }
        qt.WaitSend();
    }


    public ValueTask SendAsync(ReadOnlySpan<byte> payload)
    {
        using var sga = ScatterGatherArray.Create(payload);
        // this looks like we're disposing too soon, but actually it is
        // fine; you can "sgafree" as soon as the "push" has been started
        return SendAsync(in sga);
    }

    public void Send(ReadOnlySpan<byte> payload)
    {
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
        Interop.Assert(Interop.wait(&qr, this.qt), nameof(Interop.wait));
        qr.Assert(Opcode.Push);
    }

    internal unsafe AcceptResult WaitAccept()
    {
        Unsafe.SkipInit(out QueueResult qr);
        Interop.Assert(Interop.wait(&qr, this.qt), nameof(Interop.wait));
        qr.Assert(Opcode.Accept);
        return qr.ares;
    }

    internal unsafe ScatterGatherArray WaitReceive()
    {
        Unsafe.SkipInit(out QueueResult qr);
        Interop.Assert(Interop.wait(&qr, this.qt), nameof(Interop.wait));
        qr.Assert(Opcode.Pop);
        qr.sga.AssertValid();
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
}
[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Sizes.SCATTER_GATHER_ARRAY)]
public unsafe readonly struct ScatterGatherArray : IDisposable
{
    public const int MAX_SEGMENTS = 1;
    [FieldOffset(0)] private readonly void* buf;
    [FieldOffset(8)] private readonly uint _numsegs;
    [FieldOffset(16)] private readonly byte _segsStart; // [Sizes.SCATTER_GATHER_SEGMENT * MAX_SEGMENTS];
    [FieldOffset(32)] private readonly byte _saddrStart; //[Sizes.SOCKET_ADDRESS];

    public uint Count => _numsegs;

    public unsafe bool IsEmpty
    {
        get
        {
            if (_numsegs == 0) return true;
            fixed (byte* segs = &_segsStart)
            {
                var typed = (ScatterGatherSegment*)segs;
                for (int i = 0; i < _numsegs; i++)
                {
                    if ((typed++)->Length != 0) return false;
                }
            }
            return true;
        }
    }
    public bool IsSingleSegment => _numsegs == 1;

    public Span<byte> FirstSpan
    {
        get
        {
            if (_numsegs == 0) return default;
            fixed (byte* segs = &_segsStart)
            {
                var typed = (ScatterGatherSegment*)segs;
                return typed[0].Span;
            }
        }
    }

    public uint TotalBytes
    {
        get
        {
            ulong totalBytes = 0;
            if (_numsegs != 0)
            {
                fixed (byte* segs = &_segsStart)
                {
                    var typed = (ScatterGatherSegment*)segs;
                    for (int i = 0; i < _numsegs; i++)
                    {
                        totalBytes += (typed++)->Length;
                    }
                }
            }
            return checked((uint)totalBytes);
        }
    }
    public Span<byte> this[int index]
    {
        get
        {
            if (index < 0 || index >= _numsegs) Throw();
            fixed (byte* segs = &_segsStart)
            {
                var typed = (ScatterGatherSegment*)segs;
                return typed[index].Span;
            }

            static void Throw() => throw new IndexOutOfRangeException();
        }
    }

    public override string ToString()
        => $"{TotalBytes} bytes over {Count} segments";

    internal void AssertValid()
    {
        if (_numsegs == 0 | _numsegs > MAX_SEGMENTS) Throw(_numsegs);

        static void Throw(uint numsegs) =>
            throw new InvalidOperationException($"Invalid segment count: {numsegs}");
    }

    public void Dispose()
    {
        if (!IsEmpty)
        {
            fixed (ScatterGatherArray* ptr = &this)
            {
                Interop.Assert(Interop.sgafree(ptr), nameof(Interop.sgafree));
            }
            Unsafe.AsRef(in this) = default; // pure evil, but: we do what we can
        }
    }

    private static readonly ScatterGatherArray _empty;
    public ref readonly ScatterGatherArray Empty => ref _empty;
    public static ScatterGatherArray Create(uint size)
    {
        if (size == 0) Throw();
        var sga = Interop.sgaalloc(size);
        sga.AssertValid();
        return sga;

        static void Throw() => throw new ArgumentOutOfRangeException(nameof(size));
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
            string Hex = "012345789abcdef";
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
    public static extern int wait(QueueResult* qr, long qt);

    [DllImport("libdemikernel", EntryPoint = "demi_wait_any")]
    public static extern int wait_any(QueueResult* qr, int* offset, long* qt, int num_qts);

    [DllImport("libdemikernel", EntryPoint = "demi_sgaalloc")]
    public static extern ScatterGatherArray sgaalloc(ulong size);

    [DllImport("libdemikernel", EntryPoint = "demi_sgafree")]
    public static extern int sgafree(ScatterGatherArray* sga);
}