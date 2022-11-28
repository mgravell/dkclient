
using Demikernel;
using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;

sealed class DemoServer
{
    public static void Execute()
    {
        using var ct = new CancellationTokenSource();
        _ = new DemoServer(5245, false, ct.Token).ListenAsync();
        // _ = new DemoServer(5255, true, ct.Token).ListenAsync();
        Console.WriteLine("executing, press any key...");
        Console.ReadLine();
        Console.WriteLine("closing...");
        ct.Cancel();
    }
    static readonly X509Certificate2? s_cert = LoadCert();

    private static X509Certificate2? LoadCert()
    {
        X509Store store = new X509Store(StoreName.My, StoreLocation.LocalMachine);

        store.Open(OpenFlags.ReadOnly);

        foreach (X509Certificate2 certificate in store.Certificates)
        {
            if (certificate.SubjectName.Name == "CN=localhost")
            {
                if (string.Equals("bfd6bad3e6c5f3aa67f912ebf9b07423731525ae", certificate.GetCertHashString(), StringComparison.OrdinalIgnoreCase))
                {
                    return certificate;
                }
            }
        }
        return null;
    }

    public int Port { get; }
    public bool Tls { get; }
    public CancellationToken CancellationToken { get; }

    private readonly Socket _listen = new(SocketType.Stream, ProtocolType.Tcp);
    public DemoServer(int port, bool tls, CancellationToken cancellationToken)
    {
        Port = port;
        if (tls && s_cert is null)
        {
            Console.Error.WriteLine("Certificate not available");
            tls = false;
        }
        Tls = tls;
        CancellationToken = cancellationToken;
        quwiCallback = HandleAccepted;
    }

    [Conditional("DEBUG")]
    static void DebugLog(string message) => Console.WriteLine(message);
    public async Task ListenAsync()
    {
        try
        {
            await Task.Yield();
            var ep = new IPEndPoint(IPAddress.Parse("172.21.111.145"), Port);
            // var ep = new IPEndPoint(IPAddress.Any, Port);
            DKSocket.Initialize();
            using var socket = DKSocket.Create(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Bind(ep);
            socket.Listen(32);
            Console.WriteLine($"Listening on {ep} (tls: {Tls})...");
            while (!CancellationToken.IsCancellationRequested)
            {
                Console.WriteLine($"Accepting...");
                AcceptResult client = socket.Accept();
                Console.WriteLine($"Accepted {client}");
                ThreadPool.QueueUserWorkItem(quwiCallback, client, false);
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
        }
    }

    private readonly Action<AcceptResult> quwiCallback;
    static int count;
    async void HandleAccepted(AcceptResult accept)
    {
        var socket = accept.AsSocket();
        try
        {
            DebugLog($"Accepted from {accept.ToString()} as {socket}");
            Stream stream = new DKStream(socket, false);
            if (Tls)
            {
                Console.WriteLine("Negotiating TLS...");
                var ssl = new SslStream(stream, true);

                try
                {
                    await ssl.AuthenticateAsServerAsync(s_cert!, false,
#pragma warning disable CS0618
                        SslProtocols.Ssl2 | SslProtocols.Ssl3 |
#pragma warning restore CS0618
                        SslProtocols.Tls | SslProtocols.Tls11 | SslProtocols.Tls12 | SslProtocols.Tls13, false);
                    stream = ssl;
                }
                catch // (Exception? ex)
                {
                    //while (ex is not null)
                    //{
                    //    Console.Error.WriteLine(ex.Message);
                    //    ex = ex.InnerException;
                    //}
                    try
                    {
                        socket.Dispose();
                    }
                    catch { } // boom
                    return;
                }
            }
            byte[] blob = ArrayPool<byte>.Shared.Rent(2048);

            int offset = 0, read;
            while ((read = await stream.ReadAsync(blob, offset, blob.Length - offset, CancellationToken)) > 0)
            {
                offset += read;
                if (IsComplete(new ReadOnlySpan<byte>(blob, 0, offset)))
                {
                    DebugLog($"Request took {offset} bytes, TLS: {Tls}");
                    break;
                }
            }

            var len = WritePayload(blob);
            await stream.WriteAsync(blob, 0, len, CancellationToken);
            ArrayPool<byte>.Shared.Return(blob);
            await stream.FlushAsync(CancellationToken);
            // socket.Shutdown(SocketShutdown.Both);
            socket.Close();
        }
        catch when (CancellationToken.IsCancellationRequested) { }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
        }
        finally
        {
            Console.WriteLine("Closing client...");
            socket.Dispose();
        }
    }

    private static bool IsComplete(ReadOnlySpan<byte> request)
#if NET7_0_OR_GREATER
        => request.LastIndexOf("\r\n\r\n"u8) > 0;
#else
        => request.LastIndexOf(EOF) > 0;

    static ReadOnlySpan<byte> EOF => new byte[] { (byte)'\r', (byte)'\n', (byte)'\r', (byte)'\n' };
#endif

    static int WritePayload(byte[] blob)
    {
        var payload = $"Hello raw {Interlocked.Increment(ref count)}";
        var len = Encoding.ASCII.GetByteCount(payload);
        prefix.CopyTo(blob, 0);
        Span<byte> span = blob;
        span = span.Slice(prefix.Length);
        Utf8Formatter.TryFormat(len, span, out var written);
        span = span.Slice(written);
        suffix.CopyTo(span);
        span = span.Slice(suffix.Length);
        return (blob.Length - span.Length) + Encoding.ASCII.GetBytes(payload, span);
    }
    static readonly byte[]
        prefix = Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nContent-type: text/plain\r\nContent-length: "),
        suffix = Encoding.ASCII.GetBytes("\r\nConnection: close\r\n\r\n");
}