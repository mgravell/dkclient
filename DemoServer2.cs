
using Demikernel;
using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;

sealed class DemoServer2
{
    public static void Execute()
    {
        Task.Run(() => RunClient());
        using var ct = new CancellationTokenSource();
        _ = new DemoServer2(5245, false, ct.Token).ListenAsync();
        // _ = new DemoServer(5255, true, ct.Token).ListenAsync();
        Console.WriteLine("executing, press any key...");
        Console.ReadLine();
        Console.WriteLine("closing...");
        ct.Cancel();
    }
    static async Task RunClient()
    {
        Console.WriteLine("Starting client...");
        await Task.Delay(1000);
        var ep = new IPEndPoint(IPAddress.Parse("172.21.111.145"), 5245);
        Console.WriteLine($"Connecting to {ep}");
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket.Connect(ep);
        Console.WriteLine("Connected");

        Console.WriteLine("Pausing...");
        Console.ReadLine();
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
    public DemoServer2(int port, bool tls, CancellationToken cancellationToken)
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
                AcceptResult client = await socket.AcceptAsync();
                Console.WriteLine($"Accepted {client}");
                // HandleAccepted(client);
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
    //async
        void HandleAccepted(AcceptResult accept)
    {
        var socket = accept.AsSocket();
        try
        {
            DebugLog($"Accepted from {accept.ToString()} as {socket}");
            using var pop = socket.Receive();
            Console.WriteLine($"Received: {pop.TotalBytes} bytes");
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