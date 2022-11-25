using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Demikernel;

/// <summary>
/// A <see cref="DKSocketManager"/> co-ordinates multiple outstanding pending async operations, acting as
/// a driver for `demi_wait_any`; all items added to a single manager instance become part of the same
/// `demi_wait_any` invoke, with new items added as soon as possible (allowing for any existing
/// `demi_wait_any` call to complete first); only the live worker has access to the live queue
/// </summary>
public sealed class DKSocketManager
{
    private readonly object _pendingItemsSyncLock = new();
    private readonly List<(long Token, object Tcs)> _pending = new();
    private bool _doomed = false;

    public DKSocketManager()
    {
        ThreadPool.UnsafeQueueUserWorkItem(DriveCallback, this); // use non-pool thread? probably should...
    }

    private void Add(long token, object tcs)
    {
        lock (_pendingItemsSyncLock)
        {
            if (_doomed)
            {
                TryCancel(tcs);
            }
            else
            {
                // we can't change what wait_any is doing, so: add to a holding pen,
                // and add to wait_any on the *next* iteration
                _pending.Add((token, tcs));
                if (_pending.Count == 1) Monitor.Pulse(_pendingItemsSyncLock); // wake the worker
            }
        }
    }

    static void EnsureCapacity(ref long[] liveTokens, ref object[] liveTasks, int oldCount, int newCount)
    {
        newCount = (int)BitOperations.RoundUpToPowerOf2((uint)newCount);
        if (liveTokens is null)
        {
            liveTokens = GC.AllocateUninitializedArray<long>(newCount, pinned: true);
            liveTasks = new object[newCount];
        }
        else if (newCount > liveTokens.Length)
        {
            var newTokens = GC.AllocateArray<long>(newCount, pinned: true);
            var newTasks = new object[newCount];
            if (oldCount != 0)
            {
                new Span<long>(liveTokens, 0, oldCount).CopyTo(newTokens);
                new Span<object>(liveTasks, 0, oldCount).CopyTo(newTasks);
            }
            liveTokens = newTokens; // drop the old on the floor
            liveTasks = newTasks;
        }
    }

    static readonly WaitCallback DriveCallback = state => Unsafe.As<DKSocketManager>(state!).Drive();

    public static DKSocketManager Shared { get; } = new DKSocketManager();

    private unsafe void Drive()
    {
        long[] liveTokens = Array.Empty<long>();
        object[] liveTasks = Array.Empty<object>();
        int liveCount = 0;
        Console.WriteLine("[server] entering dedicated work loop");
        try
        {
            Unsafe.SkipInit(out QueueResult qr);

            int offset = 0;
            while (true)
            {
                lock (_pendingItemsSyncLock)
                {
                    if (_doomed) break;
                    if (_pending.Count != 0)
                    {
                        EnsureCapacity(ref liveTokens, ref liveTasks, liveCount, liveCount + _pending.Count);
                        var index = liveCount;
                        foreach (ref readonly var item in CollectionsMarshal.AsSpan(_pending))
                        {
                            liveTokens[index] = item.Token;
                            liveTasks[index++] = item.Tcs;
                        }
                        liveCount += _pending.Count;
                        _pending.Clear();
                    }
                }

                if (liveCount == 0)
                {
                    lock (_pendingItemsSyncLock)
                    {
                        if (_pending.Count == 0)
                        {
                            Monitor.Wait(_pendingItemsSyncLock);
                        }
                        continue;
                    }
                }

                // we're using the pinned heap; we can do this without "fixed"
                var qts = (long*)Unsafe.AsPointer(ref liveTokens[0]);
                if (offset < 0 | offset >= liveCount) offset = 0; // ensure valid range
                // Console.WriteLine($"[server]: wait-any {liveCount}...");
                Interop.Assert(Interop.wait_any(&qr, &offset, qts, liveCount), nameof(Interop.wait_any));

                // Console.WriteLine($"[server]: wait-any index {offset} reported {qr}...");
                if (qr.Opcode == Opcode.Invalid || offset < 0)
                {
                    // reset drive (perhaps to add new items)
                    offset = 0;
                    continue;
                }

                // right, so we're consuming an item; let's juggle the list
                // by moving the *last* item into this space (we don't
                // need to clean up the tokens; they're just integers)
                var tcs = liveTasks[offset];
                if (liveCount > 1)
                {
                    liveTasks[offset] = liveTasks[liveCount - 1];
                }
                // decrement the size and allow the task the be collected
                liveTasks[--liveCount] = default!;

                // signal async completion of the pending activity
                TryComplete(tcs, in qr);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[manager]: {ex.Message}");
            Debug.WriteLine(ex);
        }
        finally
        {
            Console.WriteLine($"[manager]: exiting loop");
            lock (_pendingItemsSyncLock)
            {
                _doomed = true;
                foreach (var item in _pending)
                {
                    TryCancel(item.Tcs);
                }
                _pending.Clear();
            }
            for (int i = 0; i < liveCount; i++)
            {
                TryCancel(liveTasks[i]);
                liveTasks[i] = null!; 
            }
            liveCount = 0;
        }
    }

    static void TryComplete(object tcs, in QueueResult qr)
    {

        if (tcs is TaskCompletionSource raw)
        {
            switch (qr.Opcode)
            {
                case Opcode.Push:
                    raw.TrySetResult();
                    break;
                case Opcode.Failed:
                    raw.TrySetException(CreateFailed());
                    break;
                default:
                    raw.TrySetException(qr.CreateUnexpected(Opcode.Push));
                    break;
            }
        }
        else if (tcs is TaskCompletionSource<ScatterGatherArray> sga)
        {
            switch (qr.Opcode)
            {
                case Opcode.Pop:
                    sga.TrySetResult(qr.sga);
                    break;
                case Opcode.Failed:
                    sga.TrySetException(CreateFailed());
                    break;
                default:
                    sga.TrySetException(qr.CreateUnexpected(Opcode.Push));
                    break;
            }
        }
        else if (tcs is TaskCompletionSource<AcceptResult> ar)
        {
            switch (qr.Opcode)
            {
                case Opcode.Accept:
                    ar.TrySetResult(qr.ares);
                    break;
                case Opcode.Failed:
                    ar.TrySetException(CreateFailed());
                    break;
                default:
                    ar.TrySetException(qr.CreateUnexpected(Opcode.Push));
                    break;
            }
        }
        static Exception CreateFailed() => throw new IOException();
    }
    static void TryCancel(object tcs)
    {
        if (tcs is TaskCompletionSource raw)
        {
            raw.TrySetCanceled();
        }
        else if (tcs is TaskCompletionSource<ScatterGatherArray> sga)
        {
            sga.TrySetCanceled();
        }
        else if (tcs is TaskCompletionSource<AcceptResult> ar)
        {
            ar.TrySetCanceled();
        }
    }

    internal Task AddSend(in QueueToken pending)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Add(pending.qt, tcs);
        return tcs.Task;
    }
    internal Task<ScatterGatherArray> AddReceive(in QueueToken pending)
    {
        var tcs = new TaskCompletionSource<ScatterGatherArray>(TaskCreationOptions.RunContinuationsAsynchronously);
        Add(pending.qt, tcs);
        return tcs.Task;
    }

    internal Task<AcceptResult> AddAccept(in QueueToken pending)
    {
        var tcs = new TaskCompletionSource<AcceptResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        Add(pending.qt, tcs);
        return tcs.Task;
    }
}