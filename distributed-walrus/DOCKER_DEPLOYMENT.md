# Hướng Dẫn Deploy Walrus với Docker

## Tổng Quan

Tài liệu này hướng dẫn deploy Walrus (distributed-walrus) sử dụng Docker với các tối ưu về performance, đặc biệt cho môi trường production.

## Yêu Cầu Hệ Thống

### Kernel và io_uring

Walrus sử dụng `io_uring` trên Linux để đạt performance tối đa. Yêu cầu:

- **Kernel Linux >= 5.1** (khuyến nghị >= 5.10)
- **Docker >= 20.10** với hỗ trợ io_uring

Kiểm tra kernel version:
```bash
uname -r
```

Kiểm tra io_uring support:
```bash
# Trong container
cat /proc/sys/kernel/io_uring_max_entries
# Nếu có output (số > 0) thì io_uring được hỗ trợ
```

### Nếu io_uring không khả dụng

Nếu môi trường không hỗ trợ io_uring, Walrus sẽ tự động fallback về mmap backend. Bạn cũng có thể force disable:

```bash
docker run -e WALRUS_DISABLE_IO_URING=1 ...
```

## Build Docker Image

### Build từ source

```bash
cd distributed-walrus
docker build -t walrus:latest .
```

### Pull từ registry (sau khi có CI)

```bash
docker pull ghcr.io/nubskr/walrus/walrus:latest
```

## Chạy Container

### Basic Run

```bash
docker run -d \
  --name walrus \
  -p 9091:9091 \
  -v walrus-data:/data \
  walrus:latest
```

### Production Configuration

#### 1. Volume Mount - QUAN TRỌNG cho Performance

**✅ NÊN DÙNG: Docker Volume (local driver)**
```bash
docker run -d \
  --name walrus \
  -v walrus-data:/data \
  walrus:latest
```

**✅ NÊN DÙNG: Bind mount với local SSD**
```bash
docker run -d \
  --name walrus \
  -v /mnt/ssd/walrus-data:/data \
  walrus:latest
```

**❌ KHÔNG NÊN: Network filesystem (NFS, CIFS)**
```bash
# Tránh mount qua NFS/CIFS vì sẽ giảm performance đáng kể
# -v /nfs/walrus-data:/data  # KHÔNG KHUYẾN NGHỊ
```

#### 2. CPU và Memory Limits

```bash
docker run -d \
  --name walrus \
  --cpus="4.0" \
  --memory="8g" \
  --memory-swap="8g" \
  -v walrus-data:/data \
  walrus:latest
```

**Lưu ý:**
- `--memory-swap` nên bằng `--memory` để disable swap (tốt cho performance)
- CPU pinning: `--cpuset-cpus="0-3"` nếu cần

#### 3. I/O Limits (nếu cần)

```bash
docker run -d \
  --name walrus \
  --device-read-bps /dev/sda:1000mb \
  --device-write-bps /dev/sda:1000mb \
  -v walrus-data:/data \
  walrus:latest
```

#### 4. Network Mode

**Host mode** (tốt nhất cho performance, nhưng ít isolation):
```bash
docker run -d \
  --name walrus \
  --network host \
  -v walrus-data:/data \
  walrus:latest
```

**Bridge mode** (mặc định, tốt cho multi-container):
```bash
docker run -d \
  --name walrus \
  -p 9091:9091 \
  -v walrus-data:/data \
  walrus:latest
```

#### 5. Security và Capabilities

```bash
docker run -d \
  --name walrus \
  --cap-drop ALL \
  --cap-add SYS_ADMIN \
  --security-opt no-new-privileges \
  -v walrus-data:/data \
  walrus:latest
```

**Lưu ý:** `SYS_ADMIN` có thể cần cho một số filesystem operations. Nếu không cần, có thể bỏ.

#### 6. ulimits

```bash
docker run -d \
  --name walrus \
  --ulimit nofile=65536:65536 \
  --ulimit memlock=-1 \
  -v walrus-data:/data \
  walrus:latest
```

## Docker Compose - Multi-Node Cluster

```yaml
version: '3.8'

services:
  walrus-1:
    image: walrus:latest
    container_name: walrus-node-1
    ports:
      - "9091:9091"
    volumes:
      - walrus-data-1:/data
    environment:
      - WALRUS_DATA_DIR=/data
      - NODE_ID=1
      - CLUSTER_NODES=walrus-1:9091,walrus-2:9091,walrus-3:9091
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G
        reservations:
          cpus: '2'
          memory: 4G
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
    networks:
      - walrus-cluster

  walrus-2:
    image: walrus:latest
    container_name: walrus-node-2
    ports:
      - "9092:9091"
    volumes:
      - walrus-data-2:/data
    environment:
      - WALRUS_DATA_DIR=/data
      - NODE_ID=2
      - CLUSTER_NODES=walrus-1:9091,walrus-2:9091,walrus-3:9091
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
    networks:
      - walrus-cluster

  walrus-3:
    image: walrus:latest
    container_name: walrus-node-3
    ports:
      - "9093:9091"
    volumes:
      - walrus-data-3:/data
    environment:
      - WALRUS_DATA_DIR=/data
      - NODE_ID=3
      - CLUSTER_NODES=walrus-1:9091,walrus-2:9091,walrus-3:9091
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
    networks:
      - walrus-cluster

volumes:
  walrus-data-1:
    driver: local
  walrus-data-2:
    driver: local
  walrus-data-3:
    driver: local

networks:
  walrus-cluster:
    driver: bridge
```

## Kubernetes Deployment

### StorageClass cho High Performance

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: walrus-fast-ssd
provisioner: kubernetes.io/local-volume
volumeBindingMode: WaitForFirstConsumer
parameters:
  fsType: ext4
```

### StatefulSet Example

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: walrus
spec:
  serviceName: walrus
  replicas: 3
  selector:
    matchLabels:
      app: walrus
  template:
    metadata:
      labels:
        app: walrus
    spec:
      containers:
      - name: walrus
        image: walrus:latest
        ports:
        - containerPort: 9091
        env:
        - name: WALRUS_DATA_DIR
          value: /data
        - name: NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        volumeMounts:
        - name: data
          mountPath: /data
        resources:
          requests:
            cpu: "2"
            memory: "4Gi"
          limits:
            cpu: "4"
            memory: "8Gi"
        livenessProbe:
          httpGet:
            path: /health
            port: 9091
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 9091
          initialDelaySeconds: 10
          periodSeconds: 5
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: walrus-fast-ssd
      resources:
        requests:
          storage: 100Gi
```

## Performance Tuning

### 1. Filesystem Options

Nếu dùng bind mount, mount với options tối ưu:

```bash
# Trên host
mount -o noatime,nodiratime /dev/sdb1 /mnt/walrus-data

# Hoặc trong /etc/fstab
/dev/sdb1 /mnt/walrus-data ext4 noatime,nodiratime 0 2
```

### 2. I/O Scheduler

Cho SSD, dùng `none` hoặc `mq-deadline`:

```bash
echo none > /sys/block/sdb/queue/scheduler
# hoặc
echo mq-deadline > /sys/block/sdb/queue/scheduler
```

### 3. Docker Storage Driver

Khuyến nghị dùng `overlay2` (mặc định từ Docker 18.09+):

```json
{
  "storage-driver": "overlay2",
  "storage-opts": [
    "overlay2.override_kernel_check=true"
  ]
}
```

### 4. Monitoring

Monitor các metrics quan trọng:

```bash
# I/O stats
docker stats walrus

# Disk I/O
iostat -x 1

# io_uring stats (trong container)
cat /proc/sys/kernel/io_uring_max_entries
```

## Troubleshooting

### io_uring không hoạt động

1. Kiểm tra kernel version: `uname -r` (cần >= 5.1)
2. Kiểm tra trong container: `cat /proc/sys/kernel/io_uring_max_entries`
3. Nếu không có, disable: `-e WALRUS_DISABLE_IO_URING=1`

### Performance thấp

1. Kiểm tra volume mount (tránh NFS/CIFS)
2. Kiểm tra I/O scheduler
3. Kiểm tra CPU/memory limits
4. Kiểm tra network mode (thử host mode)

### Permission errors

```bash
# Fix permissions
docker exec walrus chown -R walrus:walrus /data
```

## Best Practices

1. **Luôn dùng named volumes** hoặc bind mount với local SSD
2. **Set memory limits** và disable swap
3. **Monitor I/O** và adjust limits nếu cần
4. **Dùng health checks** để auto-restart
5. **Backup data** thường xuyên (volume snapshots)
6. **Test io_uring** trước khi deploy production
7. **Dùng resource limits** để tránh noisy neighbor

## So Sánh Performance

### Docker vs Native

- **CPU overhead:** ~1-3% (minimal)
- **Memory overhead:** ~100-200MB
- **I/O overhead:** ~2-5% (với local volumes)
- **Network overhead:** ~1-2ms (bridge mode)

### io_uring vs mmap (trong Docker)

- **io_uring:** ~10-30% nhanh hơn cho batch operations
- **mmap:** Tốt hơn cho sequential reads

Khuyến nghị: Dùng io_uring nếu kernel hỗ trợ, fallback về mmap nếu không.

