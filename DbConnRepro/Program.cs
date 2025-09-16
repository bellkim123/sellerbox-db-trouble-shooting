using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using MySqlConnector;

namespace DbConnRepro;

public static class Program
{
    // ⚠️ 운영 DB에서 절대 실행 금지!
    // 환경변수 DB_CONN 또는 --conn 옵션으로 연결문자열 주입.

    public static async Task<int> Main(string[] args)
    {
        Arguments arguments = Arguments.Parse(args);

        if (arguments.ShowHelp)
        {
            PrintHelp();
            return 0;
        }

        Console.WriteLine("=== Db Aborted-Connection Repro ===");
        Console.WriteLine($"Scenario           : {arguments.Scenario}");
        Console.WriteLine($"Concurrency        : {arguments.Concurrency}");
        Console.WriteLine($"Duration (sec)     : {arguments.DurationSec}");
        Console.WriteLine($"Interval (ms)      : {arguments.IntervalMs}");
        Console.WriteLine($"Cancel (ms)        : {arguments.CancelMs}");
        Console.WriteLine($"RetryDelay (ms)    : {arguments.RetryDelayMs}");
        Console.WriteLine($"Host:Port          : {arguments.Host}:{arguments.Port}");
        Console.WriteLine($"BadPwd             : {arguments.BadPwd}");
        Console.WriteLine($"ResolveOnce        : {arguments.ResolveOnce}");
        Console.WriteLine($"ConnString (from {(string.IsNullOrWhiteSpace(arguments.ConnectionString) ? "ENV DB_CONN" : "--conn")}):");
        string safeConn = HidePwd(arguments.ConnectionString);
        Console.WriteLine(safeConn);
        Console.WriteLine();

        if ((arguments.Scenario is Scenario.TcpProbeStorm
             || arguments.Scenario is Scenario.RstStorm
             || arguments.Scenario is Scenario.ProtocolGarbageStorm)
            && string.IsNullOrWhiteSpace(arguments.Host))
        {
            Console.WriteLine("❌ This scenario requires --host (and optionally --port).");
            return 1;
        }

        if (arguments.Scenario is not Scenario.TcpProbeStorm
            && arguments.Scenario is not Scenario.RstStorm
            && arguments.Scenario is not Scenario.ProtocolGarbageStorm
            && string.IsNullOrWhiteSpace(arguments.ConnectionString))
        {
            Console.WriteLine("❌ Connection string is required. Set ENV DB_CONN or pass --conn.");
            return 1;
        }

        using CancellationTokenSource cts =
            new CancellationTokenSource(TimeSpan.FromSeconds(arguments.DurationSec));
        CancellationToken cancellationToken = cts.Token;

        Metrics metrics = new Metrics();
        Task reporter = ReportMetricsAsync(metrics, cancellationToken);

        switch (arguments.Scenario)
        {
            case Scenario.TcpProbeStorm:
                await RunTcpProbeStormAsync(arguments, metrics, cancellationToken);
                break;

            case Scenario.RstStorm:
                await RunRstStormAsync(arguments, metrics, cancellationToken);
                break;

            case Scenario.ProtocolGarbageStorm:
                await RunProtocolGarbageStormAsync(arguments, metrics, cancellationToken);
                break;

            case Scenario.HandshakeCancelStorm:
                await RunHandshakeCancelStormAsync(arguments, metrics, cancellationToken);
                break;

            case Scenario.ShortTimeoutRetryBurst:
                await RunShortTimeoutRetryBurstAsync(arguments, metrics, cancellationToken);
                break;

            case Scenario.PoolThrashByLifetime:
                await RunPoolThrashByLifetimeAsync(arguments, metrics, cancellationToken);
                break;

            case Scenario.HealthCheckHammer:
                await RunHealthCheckHammerAsync(arguments, metrics, cancellationToken);
                break;

            case Scenario.WrongCredentialsBurst:
                await RunWrongCredentialsBurstAsync(arguments, metrics, cancellationToken);
                break;

            case Scenario.LongQueryTimeout:
                await RunLongQueryTimeoutAsync(arguments, metrics, cancellationToken);
                break;

            case Scenario.CheckHostBlocked:
                await RunCheckHostBlockedAsync(arguments, metrics, cancellationToken);
                break;

            default:
                Console.WriteLine("❌ Unknown scenario.");
                break;
        }

        // 잠깐 대기 후 종료
        await Task.Delay(500);
        cts.Cancel();

        Console.WriteLine();
        Console.WriteLine("=== Final Metrics ===");
        Console.WriteLine(metrics.ToString());
        return 0;
    }

    // === Scenarios ===

    // 1) TCP 포트 프로브 폭탄 (강화판: 가비지 + RST로 pre-auth 오류를 강제)
    private static async Task RunTcpProbeStormAsync(Arguments args, Metrics metrics, CancellationToken ct)
    {
        Console.WriteLine("Running TcpProbeStorm… (garbage + RST to force pre-auth errors)");

        // 호스트를 한 번만 resolve해서 특정 IP로 고정(인스턴스 단일화)
        string targetHost = await PinHostAsync(args.Host!, args.ResolveOnce);

        // 핸드셰이크 파싱 오류 유발용 임의 바이트
        byte[] garbage = new byte[] { 0x00, 0x13, 0x37, 0x42, 0xFF, 0xEE };

        List<Task> workers = new();
        for (int i = 0; i < args.Concurrency; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    using TcpClient tcpClient = new();
                    try
                    {
                        // RST 종료를 위해 Linger=0 (Close 즉시 RST)
                        tcpClient.LingerState = new LingerOption(true, 0);

                        using var innerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        int ms = Math.Max(50, args.CancelMs > 0 ? args.CancelMs : 100);
                        innerCts.CancelAfter(ms);

                        await tcpClient.ConnectAsync(targetHost, args.Port, innerCts.Token);

                        // MySQL 핸드셰이크 대신 쓰레기 바이트 전송 → 서버 입장 pre-auth 프로토콜 오류
                        try
                        {
                            using var ns = tcpClient.GetStream();
                            await ns.WriteAsync(garbage, 0, garbage.Length, innerCts.Token);
                            await ns.FlushAsync(innerCts.Token);
                        }
                        catch { /* 무시 */ }

                        // 즉시 Close → RST
                        tcpClient.Close();

                        // 의도적으로 '실패' 집계(오류 유도 목적)
                        metrics.IncrementFailure(new Exception("TcpProbe: garbage+RST"));
                    }
                    catch (OperationCanceledException oce)
                    {
                        metrics.IncrementCancelled(oce);
                        LogException("TcpProbeStorm/Connect", oce);
                    }
                    catch (Exception ex)
                    {
                        metrics.IncrementFailure(ex);
                        LogException("TcpProbeStorm/Connect", ex);
                    }
                    finally
                    {
                        try { tcpClient.Dispose(); } catch { }
                        metrics.IncrementClosed();
                    }

                    await SafeDelay(args.IntervalMs, ct);
                }
            }, ct));
        }

        try { await Task.WhenAll(workers); }
        catch (OperationCanceledException) { /* 정상 종료 */ }
    }

    // 1-2) RST만 연속(보다 단순하지만 강한 pre-auth 오류 유발)
    private static async Task RunRstStormAsync(Arguments args, Metrics metrics, CancellationToken ct)
    {
        Console.WriteLine("Running RstStorm… (connect then immediate RST close)");
        string targetHost = await PinHostAsync(args.Host!, args.ResolveOnce);

        List<Task> workers = new();
        for (int i = 0; i < args.Concurrency; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    using TcpClient c = new();
                    try
                    {
                        c.LingerState = new LingerOption(true, 0); // Close 시 RST
                        await c.ConnectAsync(targetHost, args.Port, ct);
                        c.Close(); // 즉시 RST
                        metrics.IncrementFailure(new Exception("RST close"));
                    }
                    catch (OperationCanceledException oce)
                    {
                        metrics.IncrementCancelled(oce);
                        LogException("RstStorm/Connect", oce);
                    }
                    catch (Exception ex)
                    {
                        metrics.IncrementFailure(ex);
                        LogException("RstStorm/Connect", ex);
                    }
                    finally
                    {
                        try { c.Dispose(); } catch { }
                        metrics.IncrementClosed();
                    }
                    await SafeDelay(args.IntervalMs, ct);
                }
            }, ct));
        }
        try { await Task.WhenAll(workers); } catch (OperationCanceledException) { }
    }

    // 1-3) 가비지 바이트 + RST 연속
    private static async Task RunProtocolGarbageStormAsync(Arguments args, Metrics metrics, CancellationToken ct)
    {
        Console.WriteLine("Running ProtocolGarbageStorm… (send invalid pre-auth bytes + RST)");
        string targetHost = await PinHostAsync(args.Host!, args.ResolveOnce);
        byte[] garbage = new byte[] { 0x4D, 0x59, 0x53, 0x51, 0x00, 0xFF }; // "MYSql?" 느낌의 잡바이트

        List<Task> workers = new();
        for (int i = 0; i < args.Concurrency; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    using TcpClient c = new();
                    try
                    {
                        c.LingerState = new LingerOption(true, 0);
                        await c.ConnectAsync(targetHost, args.Port, ct);
                        using var s = c.GetStream();
                        await s.WriteAsync(garbage, 0, garbage.Length, ct);
                        c.Close(); // RST
                        metrics.IncrementFailure(new Exception("Garbage+RST"));
                    }
                    catch (OperationCanceledException oce)
                    {
                        metrics.IncrementCancelled(oce);
                        LogException("ProtocolGarbageStorm/Connect", oce);
                    }
                    catch (Exception ex)
                    {
                        metrics.IncrementFailure(ex);
                        LogException("ProtocolGarbageStorm/Connect", ex);
                    }
                    finally
                    {
                        try { c.Dispose(); } catch { }
                        metrics.IncrementClosed();
                    }
                    await SafeDelay(args.IntervalMs, ct);
                }
            }, ct));
        }
        try { await Task.WhenAll(workers); } catch (OperationCanceledException) { }
    }

    // 2) 핸드셰이크 직전 취소 폭탄
    private static async Task RunHandshakeCancelStormAsync(Arguments args, Metrics metrics, CancellationToken ct)
    {
        Console.WriteLine("Running HandshakeCancelStorm… (OpenAsync cancelled near handshake)");

        List<Task> workers = new();
        for (int i = 0; i < args.Concurrency; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    using MySqlConnection connection = new(args.ConnectionString);
                    using CancellationTokenSource cancelOpen = new();
                    if (args.CancelMs > 0) cancelOpen.CancelAfter(args.CancelMs); // 100~300ms

                    try
                    {
                        await connection.OpenAsync(cancelOpen.Token); // 핸드셰이크 중 취소 → pre-auth 오류
                        metrics.IncrementSuccess();
                    }
                    catch (OperationCanceledException oce)
                    {
                        metrics.IncrementCancelled(oce);
                        LogException("HandshakeCancelStorm/Open", oce);
                    }
                    catch (Exception ex)
                    {
                        metrics.IncrementFailure(ex);
                        LogException("HandshakeCancelStorm/Open", ex);
                    }
                    finally
                    {
                        try { await connection.CloseAsync(); } catch { }
                        metrics.IncrementClosed();
                    }

                    await SafeDelay(args.IntervalMs, ct);
                }
            }, ct));
        }

        try { await Task.WhenAll(workers); }
        catch (OperationCanceledException) { }
    }

    // 3) 짧은 ConnectTimeout + 무백오프 재시도
    private static async Task RunShortTimeoutRetryBurstAsync(Arguments args, Metrics metrics, CancellationToken ct)
    {
        Console.WriteLine("Running ShortTimeoutRetryBurst… (no backoff retries on connect)");

        List<Task> workers = new();
        for (int i = 0; i < args.Concurrency; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    for (int attempt = 0; attempt < 3 && !ct.IsCancellationRequested; attempt++)
                    {
                        using MySqlConnection connection = new(args.ConnectionString);
                        try
                        {
                            await connection.OpenAsync(ct);
                            metrics.IncrementSuccess();
                            break;
                        }
                        catch (OperationCanceledException oce)
                        {
                            metrics.IncrementCancelled(oce);
                            LogException("ShortTimeoutRetryBurst/Open", oce);
                        }
                        catch (Exception ex)
                        {
                            metrics.IncrementFailure(ex);
                            LogException("ShortTimeoutRetryBurst/Open", ex);
                        }
                        finally
                        {
                            try { await connection.CloseAsync(); } catch { }
                            metrics.IncrementClosed();
                        }

                        await SafeDelay(args.RetryDelayMs, ct); // 0이면 무백오프
                    }

                    await SafeDelay(args.IntervalMs, ct);
                }
            }, ct));
        }

        try { await Task.WhenAll(workers); }
        catch (OperationCanceledException) { }
    }

    // 4) Pool Thrash 유도 (ConnectionLifeTime 짧게 + 빈번한 Open/Close)
    private static async Task RunPoolThrashByLifetimeAsync(Arguments args, Metrics metrics, CancellationToken ct)
    {
        Console.WriteLine("Running PoolThrashByLifetime… (forced new connections)");

        List<Task> workers = new();
        for (int i = 0; i < args.Concurrency; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    using MySqlConnection connection = new(args.ConnectionString);
                    try
                    {
                        await connection.OpenAsync(ct);
                        metrics.IncrementSuccess();

                        using MySqlCommand cmd = connection.CreateCommand();
                        cmd.CommandText = "SELECT 1";
                        cmd.CommandTimeout = 5;
                        _ = await cmd.ExecuteScalarAsync(ct);
                    }
                    catch (OperationCanceledException oce)
                    {
                        metrics.IncrementCancelled(oce);
                        LogException("PoolThrashByLifetime/Open/Query", oce);
                    }
                    catch (Exception ex)
                    {
                        metrics.IncrementFailure(ex);
                        LogException("PoolThrashByLifetime/Open/Query", ex);
                    }
                    finally
                    {
                        try { await connection.CloseAsync(); } catch { }
                        metrics.IncrementClosed();
                    }

                    await SafeDelay(args.IntervalMs, ct);
                }
            }, ct));
        }

        try { await Task.WhenAll(workers); }
        catch (OperationCanceledException) { }
    }

    // 5) 헬스체크 남발
    private static async Task RunHealthCheckHammerAsync(Arguments args, Metrics metrics, CancellationToken ct)
    {
        Console.WriteLine("Running HealthCheckHammer… (frequent SELECT 1)");

        List<Task> workers = new();
        for (int i = 0; i < args.Concurrency; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    using MySqlConnection connection = new(args.ConnectionString);
                    try
                    {
                        await connection.OpenAsync(ct);
                        using MySqlCommand cmd = connection.CreateCommand();
                        cmd.CommandText = "SELECT 1";
                        cmd.CommandTimeout = 5;
                        _ = await cmd.ExecuteScalarAsync(ct);
                        metrics.IncrementSuccess();
                    }
                    catch (OperationCanceledException oce)
                    {
                        metrics.IncrementCancelled(oce);
                        LogException("HealthCheckHammer/Open/Query", oce);
                    }
                    catch (Exception ex)
                    {
                        metrics.IncrementFailure(ex);
                        LogException("HealthCheckHammer/Open/Query", ex);
                    }
                    finally
                    {
                        try { await connection.CloseAsync(); } catch { }
                        metrics.IncrementClosed();
                    }

                    await SafeDelay(args.IntervalMs, ct);
                }
            }, ct));
        }

        try { await Task.WhenAll(workers); }
        catch (OperationCanceledException) { }
    }

    // 6) 잘못된 비밀번호로 인증 실패 반복 (주의: 계정 정책)
    private static async Task RunWrongCredentialsBurstAsync(Arguments args, Metrics metrics, CancellationToken ct)
    {
        Console.WriteLine("Running WrongCredentialsBurst… (auth failures; beware of account lock/policy)");

        string badConn = ToggleBadPassword(args.ConnectionString!, args.BadPwd);
        List<Task> workers = new();
        for (int i = 0; i < args.Concurrency; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    using MySqlConnection connection = new(badConn);
                    try
                    {
                        await connection.OpenAsync(ct); // 보통 ER_ACCESS_DENIED_ERROR
                        metrics.IncrementSuccess();
                    }
                    catch (OperationCanceledException oce)
                    {
                        metrics.IncrementCancelled(oce);
                        LogException("WrongCredentialsBurst/Open", oce);
                    }
                    catch (Exception ex)
                    {
                        metrics.IncrementFailure(ex);
                        LogException("WrongCredentialsBurst/Open", ex);
                    }
                    finally
                    {
                        try { await connection.CloseAsync(); } catch { }
                        metrics.IncrementClosed();
                    }

                    await SafeDelay(args.IntervalMs, ct);
                }
            }, ct));
        }

        try { await Task.WhenAll(workers); }
        catch (OperationCanceledException) { }
    }

    // 7) 쿼리 타임아웃 재현 (SELECT SLEEP으로 Command Timeout 유발)
    private static async Task RunLongQueryTimeoutAsync(Arguments args, Metrics metrics, CancellationToken ct)
    {
        Console.WriteLine("Running LongQueryTimeout… (SELECT SLEEP to trigger command timeout)");

        List<Task> workers = new();
        for (int i = 0; i < args.Concurrency; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    string connStr = args.ConnectionString!;
                    if (!connStr.Contains("DefaultCommandTimeout=", StringComparison.OrdinalIgnoreCase))
                        connStr += ";DefaultCommandTimeout=1"; // 없으면 1초로 강제

                    using MySqlConnection conn = new(connStr);
                    try
                    {
                        await conn.OpenAsync(ct);
                        using MySqlCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "SELECT SLEEP(5)"; // 5초 소요
                        cmd.CommandTimeout = 1;               // 1초 제한 → 타임아웃 발생
                        _ = await cmd.ExecuteScalarAsync(ct);
                        metrics.IncrementSuccess();
                    }
                    catch (OperationCanceledException oce)
                    {
                        metrics.IncrementCancelled(oce);
                        LogException("LongQueryTimeout/Open/Query", oce);
                    }
                    catch (Exception ex)
                    {
                        metrics.IncrementFailure(ex);
                        LogException("LongQueryTimeout/Open/Query", ex);
                    }
                    finally
                    {
                        try { await conn.CloseAsync(); } catch { }
                        metrics.IncrementClosed();
                    }

                    await SafeDelay(args.IntervalMs, ct);
                }
            }, ct));
        }

        try { await Task.WhenAll(workers); }
        catch (OperationCanceledException) { }
    }

    // 8) 단발 확인: 지금 호스트가 막혔는지(ER 1129) 즉시 판별
    private static async Task RunCheckHostBlockedAsync(Arguments args, Metrics metrics, CancellationToken ct)
    {
        Console.WriteLine("Running CheckHostBlocked… (single open to classify 1129)");
        using var conn = new MySqlConnection(args.ConnectionString!);
        try
        {
            await conn.OpenAsync(ct);
            Console.WriteLine("[OK] Open succeeded ⇒ NOT BLOCKED");
            metrics.IncrementSuccess();
        }
        catch (MySqlException mx)
        {
            metrics.IncrementFailure(mx);
            LogException("CheckHostBlocked/Open", mx);
        }
        catch (OperationCanceledException oce)
        {
            metrics.IncrementCancelled(oce);
            LogException("CheckHostBlocked/Open", oce);
        }
        catch (Exception ex)
        {
            metrics.IncrementFailure(ex);
            LogException("CheckHostBlocked/Open", ex);
        }
        finally
        {
            try { await conn.CloseAsync(); } catch { }
            metrics.IncrementClosed();
        }
    }

    // === Helpers ===

    private static async Task<string> PinHostAsync(string host, bool resolveOnce)
    {
        if (!resolveOnce) return host;

        try
        {
            var addrs = await Dns.GetHostAddressesAsync(host);
            if (addrs is { Length: > 0 })
            {
                string ip = addrs[0].ToString();
                string all = string.Join(", ", addrs.Select(a => a.ToString()));
                Console.WriteLine($"[Resolve] {host} -> {all}");
                Console.WriteLine($"[Resolve] Using pinned IP: {ip}");
                return ip;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Resolve] DNS resolve failed: {ex.Message}. Fallback to host string.");
        }
        return host;
    }

    private static Task ReportMetricsAsync(Metrics metrics, CancellationToken ct)
    {
        return Task.Run(async () =>
        {
            Stopwatch sw = Stopwatch.StartNew();
            while (!ct.IsCancellationRequested)
            {
                await SafeDelay(1000, ct);
                Console.WriteLine(metrics.ToString());
            }
        }, ct);
    }

    private static void PrintHelp()
    {
        Console.WriteLine(@"
Usage:
  dotnet run -- [--scenario <name>] [--conn <connectionString>|ENV DB_CONN] [--host] [--port]
                [--concurrency N] [--durationSec S] [--intervalMs MS] [--cancelMs MS]
                [--retryDelayMs MS] [--badPwd true|false] [--resolveOnce true|false]

Scenarios:
  TcpProbeStorm            : (강화) 가비지 바이트 + RST로 pre-auth 오류 대량 유발
  RstStorm                 : 연결 즉시 RST 종료 반복
  ProtocolGarbageStorm     : 잘못된 pre-auth 바이트 + RST 반복
  HandshakeCancelStorm     : OpenAsync 도중 짧은 취소로 핸드셰이크 직전 끊기
  ShortTimeoutRetryBurst   : 짧은 ConnectTimeout + 무백오프 재시도 폭주
  PoolThrashByLifetime     : ConnectionLifeTime 활용한 빈번한 새 연결 강제
  HealthCheckHammer        : SELECT 1을 높은 병렬/주기로 수행
  WrongCredentialsBurst    : 잘못된 자격증명으로 인증 실패 반복 (계정 정책 주의)
  LongQueryTimeout         : SELECT SLEEP으로 Command Timeout 재현
  CheckHostBlocked         : 단발 Open으로 ER 1129(호스트 차단) 여부 판별

Examples:
  dotnet run -- --scenario TcpProbeStorm --host <instance-endpoint> --port 3306 \
    --concurrency 400 --durationSec 90 --intervalMs 0 --cancelMs 100 --resolveOnce true

  dotnet run -- --scenario ProtocolGarbageStorm --host <instance-endpoint> --port 3306 \
    --concurrency 300 --durationSec 60 --intervalMs 0 --resolveOnce true

  dotnet run -- --scenario CheckHostBlocked
");
    }

    private static async Task SafeDelay(int ms, CancellationToken ct)
    {
        if (ms <= 0) return;
        try { await Task.Delay(ms, ct); }
        catch (OperationCanceledException) { /* 정상 종료 → 무시 */ }
    }

    private static string HidePwd(string conn)
    {
        if (string.IsNullOrWhiteSpace(conn)) return "(empty)";
        return System.Text.RegularExpressions.Regex.Replace(
            conn,
            @"(Pwd|Password)\s*=\s*[^;]+",
            "$1=*****",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase
        );
    }

    private static string ToggleBadPassword(string conn, bool badPwd)
    {
        if (!badPwd) return conn;
        return System.Text.RegularExpressions.Regex.Replace(
            conn,
            @"(Pwd|Password)\s*=\s*[^;]+",
            m => m.Groups[1].Value + "=DefinitelyWrongPassword!",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase
        );
    }

    private static bool IsHostBlocked(MySqlException ex) =>
        ex.Number == 1129 ||
        (ex.Message?.IndexOf("is blocked because of many connection errors", StringComparison.OrdinalIgnoreCase) >= 0);

    private static void LogException(string where, Exception ex)
    {
        if (ex is MySqlException mx)
        {
            Console.WriteLine($"[ERR|{where}] MySql #{mx.Number} {mx.ErrorCode} :: {mx.Message}");
            if (IsHostBlocked(mx))
            {
                Console.WriteLine("🚫 DETECTED: Host is BLOCKED (ER 1129). Run: FLUSH HOSTS;");
            }
        }
        else if (ex is SocketException sx)
        {
            Console.WriteLine($"[SOCK|{where}] {sx.SocketErrorCode} :: {sx.Message}");
        }
        else if (ex is OperationCanceledException)
        {
            Console.WriteLine($"[CANCEL|{where}] cancelled");
        }
        else
        {
            Console.WriteLine($"[EX|{where}] {ex.GetType().Name} :: {ex.Message}");
        }
    }

    private enum Scenario
    {
        TcpProbeStorm,
        RstStorm,
        ProtocolGarbageStorm,
        HandshakeCancelStorm,
        ShortTimeoutRetryBurst,
        PoolThrashByLifetime,
        HealthCheckHammer,
        WrongCredentialsBurst,
        LongQueryTimeout,
        CheckHostBlocked
    }

    private sealed class Arguments
    {
        public Scenario Scenario { get; init; } = Scenario.TcpProbeStorm;
        public string? ConnectionString { get; init; }
        public string? Host { get; init; }
        public int Port { get; init; } = 3306;
        public int Concurrency { get; init; } = 100;
        public int DurationSec { get; init; } = 60;
        public int IntervalMs { get; init; } = 10;
        public int CancelMs { get; init; } = 200;
        public int RetryDelayMs { get; init; } = 0;
        public bool BadPwd { get; init; } = false;
        public bool ResolveOnce { get; init; } = true;
        public bool ShowHelp { get; init; } = false;

        public static Arguments Parse(string[] args)
        {
            Dictionary<string, string?> map = new(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < args.Length; i++)
            {
                string key = args[i];
                if (!key.StartsWith("--")) continue;
                string norm = key[2..];
                string? val = (i + 1 < args.Length && !args[i + 1].StartsWith("--")) ? args[i + 1] : "true";
                map[norm] = val;
            }

            Scenario scenario = Scenario.TcpProbeStorm;
            if (map.TryGetValue("scenario", out string? scenStr) && !string.IsNullOrWhiteSpace(scenStr))
            {
                if (!Enum.TryParse<Scenario>(scenStr, true, out scenario))
                    scenario = Scenario.TcpProbeStorm;
            }

            string? conn = null;
            if (map.TryGetValue("conn", out string? c) && !string.IsNullOrWhiteSpace(c))
                conn = c;
            else
                conn = Environment.GetEnvironmentVariable("DB_CONN");

            bool showHelp = map.ContainsKey("help") || map.ContainsKey("h") || map.ContainsKey("?");

            return new Arguments
            {
                Scenario = scenario,
                ConnectionString = conn,
                Host = map.TryGetValue("host", out string? h) ? h : null,
                Port = map.TryGetValue("port", out string? p) && int.TryParse(p, out int port) ? port : 3306,
                Concurrency = map.TryGetValue("concurrency", out string? con) && int.TryParse(con, out int cc) ? Math.Max(1, cc) : 100,
                DurationSec = map.TryGetValue("durationSec", out string? ds) && int.TryParse(ds, out int dd) ? Math.Max(1, dd) : 60,
                IntervalMs = map.TryGetValue("intervalMs", out string? im) && int.TryParse(im, out int ii) ? Math.Max(0, ii) : 10,
                CancelMs = map.TryGetValue("cancelMs", out string? cm) && int.TryParse(cm, out int ci) ? Math.Max(1, ci) : 200,
                RetryDelayMs = map.TryGetValue("retryDelayMs", out string? rd) && int.TryParse(rd, out int ri) ? Math.Max(0, ri) : 0,
                BadPwd = map.TryGetValue("badPwd", out string? bp) && bool.TryParse(bp, out bool b) ? b : false,
                ResolveOnce = map.TryGetValue("resolveOnce", out var ro) && bool.TryParse(ro, out var bro) ? bro : true,
                ShowHelp = showHelp
            };
        }
    }

    private sealed class Metrics
    {
        private long _success;
        private long _failure;
        private long _cancelled;
        private long _closed;

        public void IncrementSuccess() => Interlocked.Increment(ref _success);
        public void IncrementFailure(Exception _) => Interlocked.Increment(ref _failure);
        public void IncrementCancelled(Exception _) => Interlocked.Increment(ref _cancelled);
        public void IncrementClosed() => Interlocked.Increment(ref _closed);

        public override string ToString()
        {
            long s = Interlocked.Read(ref _success);
            long f = Interlocked.Read(ref _failure);
            long c = Interlocked.Read(ref _cancelled);
            long cl = Interlocked.Read(ref _closed);
            return $"[Metrics] success={s} failure={f} cancelled={c} closed={cl}";
        }
    }
}
