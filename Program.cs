// export LIBOS=Catnap
// export CONFIG_PATH=$HOME/config.yaml
// export RUST_LOG=debug (or info, warn, error)
using Demikernel;
using System.Net;
using System.Net.Sockets;
using System.Text;

DemoServer.Execute();
// DemoServer2.Execute();
// await SimpleProgram.RunAsync();
static class SimpleProgram
{
    internal static async Task RunAsync()
    {
        Console.WriteLine("[server] init...");
        DKSocket.Initialize("--catnap");

        Console.WriteLine("[server] create socket...");
        using var socket = DKSocket.Create(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        Console.WriteLine($"Socket: {socket}");

        var ep = new IPEndPoint(IPAddress.Parse("172.21.111.145"), 5245);
        Console.WriteLine($"[server] bind to {ep}...");
        socket.Bind(ep);

        Console.WriteLine("[server] listen...");
        socket.Listen(32);

        Console.WriteLine("[server] accept...");
        using var client = socket.Accept().AsSocket();
        Console.WriteLine("[server] connected (yay!)");

        //Console.WriteLine("[server] accept (async)...");
        //var pending = socket.AcceptAsync();
        //Console.WriteLine(pending);

        //_ = Task.Run(RunClient);

        //Console.WriteLine("[server] wait...");
        ////var result = await pending; //.Wait();

        //Console.WriteLine("[server] accept (sync)...");
        //var result = socket.Accept();

        //Console.WriteLine($"[server] accepted from {result}");
        //using var other = result.AsSocket();
        //Console.WriteLine($"[server] client socket: {other}");

        //while (true)
        //{
        //    using var sga = await other.ReceiveAsync();
        //    Console.WriteLine($"[server] received {sga}: {Encoding.ASCII.GetString(sga.FirstSpan)}");

        //    if (sga.IsEmpty) break; // client disconnected

        //    using var resp = ScatterGatherArray.Create(sga.TotalBytes);
        //    sga.FirstSpan.CopyTo(resp.FirstSpan);
        //    resp.FirstSpan.Reverse();
        //    Console.WriteLine($"[server] sending: {resp}: {Encoding.ASCII.GetString(resp.FirstSpan)}");
        //    await other.SendAsync(resp);
        //}

        //Console.WriteLine("[server] closing");

        //void RunClient()
        //{
        //    Console.WriteLine("[client] create...");
        //    using var client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        //    Console.WriteLine("[client] connect...");
        //    client.Connect(ep);

        //    Console.WriteLine("[client] connected");
        //    int offset, read;
        //    byte[] buffer;
        //    for (int i = 0; i < 5; i++)
        //    {
        //        buffer = Encoding.ASCII.GetBytes($"Hello, world! ({i})");
        //        var wrote = client.Send(buffer);
        //        Console.WriteLine($"[client] sent {wrote} bytes: {Encoding.ASCII.GetString(buffer, 0, wrote)}");
        //        Array.Clear(buffer);
        //        offset = 0;
        //        while (offset < buffer.Length && (read = client.Receive(buffer, offset, buffer.Length - offset, SocketFlags.None)) > 0)
        //        {
        //            offset += read;
        //        }
        //        Console.WriteLine($"[client] received {offset} bytes: {Encoding.ASCII.GetString(buffer, 0, offset)}");
        //    }
        //    client.Shutdown(SocketShutdown.Send);
        //    Console.WriteLine($"[client] draining...");
        //    buffer = new byte[64];
        //    while ((read = client.Receive(buffer, 0, buffer.Length, SocketFlags.None)) > 0)
        //    {
        //        Console.WriteLine($"[client] received {read} bytes: {Encoding.ASCII.GetString(buffer, 0, read)}");
        //    }
        //    Console.WriteLine("[client] closing");
        //}
    }
}