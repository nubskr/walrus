# Hướng Dẫn Setup Walrus Cluster trên Ubuntu Server

Hướng dẫn này sẽ giúp bạn thiết lập một cụm Walrus 3-node trên các máy chủ Ubuntu.

## Yêu Cầu Hệ Thống

- **OS**: Ubuntu 20.04 LTS trở lên
- **RAM**: Tối thiểu 2GB mỗi node (khuyến nghị 4GB+)
- **Disk**: Tối thiểu 10GB dung lượng trống
- **Network**: Các node phải có thể giao tiếp với nhau qua TCP
- **Rust**: Version nightly (sẽ được cài đặt trong hướng dẫn)

## Bước 1: Chuẩn Bị Môi Trường

### Cài Đặt Rust và Dependencies

Trên mỗi node Ubuntu, chạy các lệnh sau:

```bash
# Cập nhật hệ thống
sudo apt update && sudo apt upgrade -y

# Cài đặt dependencies cần thiết
sudo apt install -y \
    build-essential \
    pkg-config \
    cmake \
    clang \
    curl \
    git \
    ca-certificates

# Cài đặt Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# Cài đặt Rust nightly toolchain
rustup toolchain install nightly
rustup default nightly
rustup component add rustfmt clippy
```

### Clone Repository

```bash
cd ~
git clone https://github.com/nubskr/walrus.git
cd walrus
```

## Bước 2: Build Binary

Trên mỗi node, build binary:

```bash
cd ~/walrus/distributed-walrus
cargo +nightly -Zunstable-options build --release
```

Binary sẽ được tạo tại: `~/walrus/distributed-walrus/target/release/distributed-walrus`

**Lưu ý**: Bạn có thể build trên một máy và copy binary sang các node khác để tiết kiệm thời gian.

## Bước 3: Cấu Hình Network

Đảm bảo các port sau được mở trên firewall:

- **Raft Port** (mặc định 6000): Giao tiếp giữa các node
- **Client Port** (mặc định 8080): Kết nối từ client

```bash
# Nếu sử dụng UFW
sudo ufw allow 6000/tcp
sudo ufw allow 8080/tcp
sudo ufw reload

# Hoặc nếu sử dụng firewalld
sudo firewall-cmd --permanent --add-port=6000/tcp
sudo firewall-cmd --permanent --add-port=8080/tcp
sudo firewall-cmd --reload
```

## Bước 4: Tạo Thư Mục Dữ Liệu

Trên mỗi node:

```bash
# Tạo thư mục lưu trữ dữ liệu
mkdir -p ~/walrus-data/node_1/user_data
mkdir -p ~/walrus-data/node_1/raft_meta
```

## Bước 5: Khởi Động Cluster

### Node 1 (Bootstrap Leader)

Trên node đầu tiên, khởi động với vai trò leader:

```bash
cd ~/walrus/distributed-walrus

./target/release/distributed-walrus \
    --node-id 1 \
    --data-dir ~/walrus-data \
    --raft-host 0.0.0.0 \
    --raft-port 6001 \
    --client-host 0.0.0.0 \
    --client-port 9091
```

**Lưu ý**: Thay `0.0.0.0` bằng IP thực tế của node nếu cần.

### Node 2 (Join Cluster)

Trên node thứ hai, join vào cluster:

```bash
cd ~/walrus/distributed-walrus

./target/release/distributed-walrus \
    --node-id 2 \
    --data-dir ~/walrus-data \
    --raft-host 0.0.0.0 \
    --raft-port 6002 \
    --raft-advertise-host <IP_NODE_2> \
    --client-host 0.0.0.0 \
    --client-port 9092 \
    --join <IP_NODE_1>:6001
```

Thay `<IP_NODE_1>` và `<IP_NODE_2>` bằng IP thực tế của các node.

### Node 3 (Join Cluster)

Trên node thứ ba:

```bash
cd ~/walrus/distributed-walrus

./target/release/distributed-walrus \
    --node-id 3 \
    --data-dir ~/walrus-data \
    --raft-host 0.0.0.0 \
    --raft-port 6003 \
    --raft-advertise-host <IP_NODE_3> \
    --client-host 0.0.0.0 \
    --client-port 9093 \
    --join <IP_NODE_1>:6001
```

## Bước 6: Kiểm Tra Cluster

Sau khi khởi động, kiểm tra logs để đảm bảo các node đã join thành công:

```bash
# Trên mỗi node, kiểm tra logs
# Bạn sẽ thấy thông báo về việc join cluster và Raft leader election
```

### Test với CLI Client

```bash
# Kết nối đến node bất kỳ
cd ~/walrus/distributed-walrus
cargo run --bin walrus-cli -- --addr <IP_NODE_1>:9091

# Trong CLI:
> REGISTER logs
> PUT logs "Hello Walrus"
> GET logs
> STATE logs
> METRICS
```

## Bước 7: Tạo Systemd Service (Tùy Chọn)

Để chạy Walrus như một service tự động khởi động, tạo file systemd:

### Tạo Service File cho Node 1

```bash
sudo nano /etc/systemd/system/walrus-node1.service
```

Nội dung:

```ini
[Unit]
Description=Walrus Node 1
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=/home/$USER/walrus/distributed-walrus
ExecStart=/home/$USER/walrus/distributed-walrus/target/release/distributed-walrus \
    --node-id 1 \
    --data-dir /home/$USER/walrus-data \
    --raft-host 0.0.0.0 \
    --raft-port 6001 \
    --client-host 0.0.0.0 \
    --client-port 9091
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
Environment="RUST_LOG=info"

[Install]
WantedBy=multi-user.target
```

### Kích Hoạt Service

```bash
sudo systemctl daemon-reload
sudo systemctl enable walrus-node1
sudo systemctl start walrus-node1
sudo systemctl status walrus-node1
```

Lặp lại cho Node 2 và Node 3 với các tham số tương ứng.

## Cấu Hình Nâng Cao

### Environment Variables

Bạn có thể cấu hình thêm qua biến môi trường:

```bash
export WALRUS_MAX_SEGMENT_ENTRIES=1000000  # Số entry trước khi rollover
export WALRUS_MONITOR_CHECK_MS=10000       # Interval kiểm tra rollover
export WALRUS_DISABLE_IO_URING=1           # Tắt io_uring (nếu cần)
export RUST_LOG=info                       # Log level
```

### Load Balancer (Nginx)

Để phân tải client requests, cấu hình Nginx:

```nginx
upstream walrus_cluster {
    least_conn;
    server <IP_NODE_1>:9091;
    server <IP_NODE_2>:9092;
    server <IP_NODE_3>:9093;
}

server {
    listen 80;
    
    location / {
        proxy_pass http://walrus_cluster;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## Troubleshooting

### Node không join được cluster

1. Kiểm tra firewall đã mở port chưa
2. Kiểm tra IP address có đúng không
3. Kiểm tra network connectivity: `ping` và `telnet` giữa các node
4. Xem logs để tìm lỗi cụ thể

### Raft leader không được bầu

1. Đảm bảo có ít nhất 3 node (hoặc cấu hình cho 2 node)
2. Kiểm tra Raft port có thể truy cập được không
3. Xem logs của từng node

### Performance Issues

1. Kiểm tra disk I/O: `iostat -x 1`
2. Kiểm tra network latency giữa các node
3. Điều chỉnh `WALRUS_MAX_SEGMENT_ENTRIES` nếu cần
4. Xem xét sử dụng io_uring (mặc định bật trên Linux)

## Monitoring

### Kiểm Tra Metrics

Sử dụng CLI client để lấy metrics:

```bash
cargo run --bin walrus-cli -- --addr <IP>:9091
> METRICS
```

### Health Check Script

Tạo script kiểm tra health:

```bash
#!/bin/bash
NODES=("node1:9091" "node2:9092" "node3:9093")

for node in "${NODES[@]}"; do
    echo "Checking $node..."
    timeout 2 bash -c "echo > /dev/tcp/${node/:/ /}" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "$node is UP"
    else
        echo "$node is DOWN"
    fi
done
```

## Backup và Recovery

### Backup Data

```bash
# Backup dữ liệu trên mỗi node
tar -czf walrus-backup-$(date +%Y%m%d).tar.gz ~/walrus-data
```

### Recovery

1. Dừng tất cả nodes
2. Restore dữ liệu từ backup
3. Khởi động lại nodes theo thứ tự (node 1 trước, sau đó node 2, 3)

## Tài Liệu Tham Khảo

- [Architecture Documentation](../distributed-walrus/docs/architecture.md)
- [CLI Guide](../distributed-walrus/docs/cli.md)
- [GitHub Repository](https://github.com/nubskr/walrus)

