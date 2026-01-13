# Hướng Dẫn Tích Hợp Walrus với .NET 8

Hướng dẫn này sẽ giúp bạn tích hợp Walrus distributed log streaming engine với ứng dụng .NET 8.

## Tổng Quan

Thư viện .NET 8 client cho Walrus cung cấp:
- Kết nối TCP với Walrus cluster
- Đăng ký và quản lý topics
- Ghi và đọc dữ liệu (string và binary)
- Lấy metadata và metrics
- Async/await support đầy đủ
- Error handling với custom exceptions

## Cài Đặt

### Từ Source Code

1. Clone repository:
```bash
git clone https://github.com/nubskr/walrus.git
cd walrus/walrus-dotnet
```

2. Build thư viện:
```bash
cd Walrus.Client
dotnet build
```

3. Thêm reference vào project của bạn:
```xml
<ItemGroup>
  <ProjectReference Include="path/to/Walrus.Client/Walrus.Client.csproj" />
</ItemGroup>
```

### Từ NuGet (Khi đã publish)

```bash
dotnet add package Walrus.Client
```

## Sử Dụng Cơ Bản

### 1. Tạo Client

```csharp
using Walrus.Client;

// Kết nối đến một node trong cluster
var client = new WalrusClient("localhost", 9091);

// Hoặc với timeout tùy chỉnh
var client = new WalrusClient("192.168.1.100", 9091, 
    connectionTimeout: TimeSpan.FromSeconds(10));
```

### 2. Đăng Ký Topic

```csharp
try
{
    await client.RegisterTopicAsync("my-topic");
    Console.WriteLine("Topic đã được đăng ký");
}
catch (WalrusCommandException ex)
{
    Console.WriteLine($"Lỗi: {ex.Message}");
}
```

### 3. Ghi Dữ Liệu

```csharp
// Ghi string
await client.PutAsync("my-topic", "Hello, Walrus!");

// Ghi binary data
var data = Encoding.UTF8.GetBytes("Binary data");
await client.PutAsync("my-topic", data);

// Ghi với cancellation token
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
await client.PutAsync("my-topic", "Data", cts.Token);
```

### 4. Đọc Dữ Liệu

```csharp
// Đọc entry tiếp theo
var entry = await client.GetAsync("my-topic");
if (entry != null)
{
    Console.WriteLine($"Đọc được: {entry}");
}
else
{
    Console.WriteLine("Topic đang trống");
}

// Đọc nhiều entries
while (true)
{
    var entry = await client.GetAsync("my-topic");
    if (entry == null)
    {
        break; // Hết dữ liệu
    }
    Console.WriteLine(entry);
    await Task.Delay(100);
}
```

### 5. Lấy Trạng Thái Topic

```csharp
var state = await client.GetTopicStateAsync("my-topic");

Console.WriteLine($"Topic: {state.Topic}");
Console.WriteLine($"Tổng entries: {state.TotalEntries}");
Console.WriteLine($"Số segments: {state.Segments.Count}");

foreach (var segment in state.Segments)
{
    Console.WriteLine($"Segment {segment.SegmentId}:");
    Console.WriteLine($"  Leader: Node {segment.LeaderNode}");
    Console.WriteLine($"  Entries: {segment.EntryCount}");
    Console.WriteLine($"  Sealed: {segment.Sealed}");
}
```

### 6. Lấy Cluster Metrics

```csharp
var metrics = await client.GetMetricsAsync();

Console.WriteLine($"Node ID: {metrics.NodeId}");
Console.WriteLine($"Current Term: {metrics.CurrentTerm}");
Console.WriteLine($"State: {metrics.State}");
Console.WriteLine($"Leader: {metrics.Leader}");
Console.WriteLine($"Last Log Index: {metrics.LastLogIndex}");
```

## Xử Lý Lỗi

```csharp
try
{
    await client.PutAsync("my-topic", "data");
}
catch (WalrusConnectionException ex)
{
    // Lỗi kết nối (timeout, network error, etc.)
    Console.WriteLine($"Lỗi kết nối: {ex.Message}");
    // Có thể retry hoặc reconnect
}
catch (WalrusCommandException ex)
{
    // Lỗi từ server (invalid command, topic not found, etc.)
    Console.WriteLine($"Lỗi command: {ex.Message}");
}
catch (Exception ex)
{
    // Lỗi không mong đợi
    Console.WriteLine($"Lỗi: {ex.Message}");
}
```

## Best Practices

### 1. Sử dụng using/await using

```csharp
// Synchronous disposal
using var client = new WalrusClient("localhost", 9091);
// ... sử dụng client

// Asynchronous disposal (khuyến nghị)
await using var client = new WalrusClient("localhost", 9091);
// ... sử dụng client
```

### 2. Connection Pooling

Nếu cần nhiều connections, tạo một pool:

```csharp
public class WalrusClientPool
{
    private readonly ConcurrentQueue<WalrusClient> _pool = new();
    private readonly string _host;
    private readonly int _port;

    public WalrusClientPool(string host, int port, int poolSize = 10)
    {
        _host = host;
        _port = port;
        
        for (int i = 0; i < poolSize; i++)
        {
            _pool.Enqueue(new WalrusClient(host, port));
        }
    }

    public WalrusClient Rent()
    {
        if (_pool.TryDequeue(out var client))
        {
            return client;
        }
        return new WalrusClient(_host, _port);
    }

    public void Return(WalrusClient client)
    {
        _pool.Enqueue(client);
    }
}
```

### 3. Retry Logic

```csharp
public static class WalrusClientExtensions
{
    public static async Task<T> RetryAsync<T>(
        this Func<CancellationToken, Task<T>> operation,
        int maxRetries = 3,
        TimeSpan? delay = null,
        CancellationToken cancellationToken = default)
    {
        delay ??= TimeSpan.FromSeconds(1);
        
        for (int i = 0; i < maxRetries; i++)
        {
            try
            {
                return await operation(cancellationToken);
            }
            catch (WalrusConnectionException) when (i < maxRetries - 1)
            {
                await Task.Delay(delay.Value, cancellationToken);
            }
        }
        
        return await operation(cancellationToken);
    }
}

// Sử dụng
await ((ct) => client.PutAsync("topic", "data", ct))
    .RetryAsync(maxRetries: 3);
```

### 4. Load Balancing

Kết nối đến nhiều nodes và phân tải:

```csharp
public class WalrusClusterClient
{
    private readonly List<WalrusClient> _clients;
    private int _currentIndex = 0;

    public WalrusClusterClient(params (string host, int port)[] nodes)
    {
        _clients = nodes.Select(n => new WalrusClient(n.host, n.port)).ToList();
    }

    private WalrusClient GetNextClient()
    {
        var client = _clients[_currentIndex];
        _currentIndex = (_currentIndex + 1) % _clients.Count;
        return client;
    }

    public async Task PutAsync(string topic, string data, CancellationToken ct = default)
    {
        var client = GetNextClient();
        await client.PutAsync(topic, data, ct);
    }

    // Implement các methods khác tương tự
}
```

## Ví Dụ Ứng Dụng Thực Tế

### Logging Service

```csharp
public class WalrusLogger
{
    private readonly WalrusClient _client;
    private readonly string _topic;

    public WalrusLogger(string host, int port, string topic)
    {
        _client = new WalrusClient(host, port);
        _topic = topic;
    }

    public async Task LogAsync(string message, LogLevel level = LogLevel.Info)
    {
        var logEntry = new
        {
            Timestamp = DateTime.UtcNow,
            Level = level.ToString(),
            Message = message
        };

        var json = JsonSerializer.Serialize(logEntry);
        await _client.PutAsync(_topic, json);
    }

    public async Task<IEnumerable<string>> ReadLogsAsync(int count)
    {
        var logs = new List<string>();
        for (int i = 0; i < count; i++)
        {
            var entry = await _client.GetAsync(_topic);
            if (entry == null) break;
            logs.Add(entry);
        }
        return logs;
    }
}
```

### Message Queue Consumer

```csharp
public class WalrusConsumer
{
    private readonly WalrusClient _client;
    private readonly string _topic;
    private readonly CancellationTokenSource _cts = new();

    public WalrusConsumer(string host, int port, string topic)
    {
        _client = new WalrusClient(host, port);
        _topic = topic;
    }

    public async Task StartConsumingAsync(Func<string, Task> handler)
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                var entry = await _client.GetAsync(_topic, _cts.Token);
                if (entry != null)
                {
                    await handler(entry);
                }
                else
                {
                    // Không có dữ liệu, đợi một chút
                    await Task.Delay(100, _cts.Token);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Lỗi khi consume: {ex.Message}");
                await Task.Delay(1000, _cts.Token);
            }
        }
    }

    public void Stop()
    {
        _cts.Cancel();
    }
}
```

## Testing

### Unit Tests với Mock

```csharp
[Fact]
public async Task PutAsync_ShouldSendCommand()
{
    // Arrange
    var mockTcpClient = new MockTcpClient();
    var client = new WalrusClient("localhost", 9091);
    
    // Act
    await client.PutAsync("test-topic", "data");
    
    // Assert
    // Verify command was sent
}
```

### Integration Tests

```csharp
[Fact]
public async Task IntegrationTest_WriteAndRead()
{
    // Arrange
    var client = new WalrusClient("localhost", 9091);
    
    // Act
    await client.RegisterTopicAsync("test-topic");
    await client.PutAsync("test-topic", "test-data");
    var result = await client.GetAsync("test-topic");
    
    // Assert
    Assert.Equal("test-data", result);
}
```

## Performance Tips

1. **Reuse Clients**: Tạo một client instance và reuse thay vì tạo mới mỗi lần
2. **Batch Operations**: Nếu có thể, batch nhiều operations lại
3. **Connection Timeout**: Điều chỉnh timeout phù hợp với network latency
4. **Async Everywhere**: Luôn sử dụng async/await, không block threads

## Troubleshooting

### Connection Timeout

- Kiểm tra firewall và network connectivity
- Tăng connection timeout nếu network chậm
- Kiểm tra Walrus server đang chạy

### Command Errors

- Kiểm tra topic đã được đăng ký chưa
- Kiểm tra format của command
- Xem logs của Walrus server

### Performance Issues

- Sử dụng connection pooling
- Kiểm tra network latency
- Monitor Walrus cluster metrics

## Tài Liệu Tham Khảo

- [Walrus Client README](../walrus-dotnet/Walrus.Client/README.md)
- [Ubuntu Setup Guide](./ubuntu-setup-guide.md)
- [Walrus Architecture](../distributed-walrus/docs/architecture.md)

