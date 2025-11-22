import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class KafkaBenchmark {

    static final int NUM_THREADS = 10;
    static final long MONITOR_INTERVAL_MS = 500; 
    static final long BATCH_SLEEP_MS = 500;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java KafkaBenchmark <topic_prefix> <duration_seconds>");
            System.exit(1);
        }

        final String topicPrefix = args[0];
        final long durationSeconds = Long.parseLong(args[1]);
        final long startTime = System.currentTimeMillis();
        final long endTime = startTime + durationSeconds * 1000;

        // Kafka Producer Configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("linger.ms", "0");
        props.put("batch.size", "16384");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());

        final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        AtomicLong totalMessages = new AtomicLong(0);
        AtomicLong totalBytes = new AtomicLong(0);

        // Prepare CSV output file
        String csvPath = "kafka.csv";
        boolean writeHeader = !Files.exists(Paths.get(csvPath));
        PrintWriter csvWriter = new PrintWriter(new FileOutputStream(csvPath, true), true);
        if (writeHeader) {
            csvWriter.println(
                    "timestamp,elapsed_seconds,writes_per_second,bytes_per_second,total_writes,total_bytes,dirty_pages_kb,dirty_ratio_percent"
            );
        }

        ExecutorService exec = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);

        for (int t = 0; t < NUM_THREADS; t++) {
            final int threadId = t;
            exec.submit(() -> {
                try {
                    Random rng = new Random();
                    long counter = 0;
                    int batchIndex = 0;

                    while (System.currentTimeMillis() < endTime) {
                        int batchSize = Math.min(500_000, 50_000 + batchIndex * 50_000);
                        String topic = topicPrefix + "_" + threadId;

                        for (int i = 0; i < batchSize; i++) {
                            if (System.currentTimeMillis() >= endTime) break;

                            int size = 500 + rng.nextInt(525); // [500, 1024]
                            byte[] payload = new byte[size];
                            Arrays.fill(payload, (byte) (counter % 256));

                            String key = topic + ":" + counter;

                            producer.send(new ProducerRecord<>(topic, key, payload));
                            totalMessages.incrementAndGet();
                            totalBytes.addAndGet(size);
                            counter++;
                        }

                        batchIndex = (batchIndex + 1) % 10;
                        if (System.currentTimeMillis() < endTime) {
                            Thread.sleep(BATCH_SLEEP_MS);
                        }
                    }
                } catch (Exception ignored) {
                } finally {
                    latch.countDown();
                }
            });
        }

        Thread monitor = new Thread(() -> {
            long lastMessages = 0;
            long lastBytes = 0;
            long lastTime = startTime;

            while (System.currentTimeMillis() < endTime + 2000) {
                try {
                    Thread.sleep(MONITOR_INTERVAL_MS);
                } catch (InterruptedException ignored) {}

                long now = System.currentTimeMillis();
                double intervalSec = (now - lastTime) / 1000.0;
                long curMessages = totalMessages.get();
                long curBytes = totalBytes.get();

                double wps = (curMessages - lastMessages) / intervalSec;
                double bps = (curBytes - lastBytes) / intervalSec;
                double elapsed = (now - startTime) / 1000.0;

                long dirtyKB = getDirtyKB();
                long totalMem = getTotalMemKB();
                double dirtyRatio = totalMem > 0 ? (dirtyKB * 100.0 / totalMem) : 0.0;
                long timestampSec = now / 1000;

                csvWriter.printf(Locale.US,
                        "%d,%.2f,%.0f,%.0f,%d,%d,%d,%.2f%n",
                        timestampSec, elapsed, wps, bps, curMessages, curBytes, dirtyKB, dirtyRatio);
                csvWriter.flush();

                System.out.printf(
                        "[Monitor] %.1fs: %.0f writes/sec, %.2f MB/sec, total: %d, dirty: %.2f%% (%d KB)%n",
                        elapsed, wps, (bps / (1024.0 * 1024.0)), curMessages, dirtyRatio, dirtyKB
                );
                System.out.flush();

                lastMessages = curMessages;
                lastBytes = curBytes;
                lastTime = now;
            }
        });

        monitor.start();
        latch.await();
        producer.flush();
        producer.close();
        exec.shutdownNow();
        monitor.join();
        csvWriter.close();

        long finalMessages = totalMessages.get();
        long finalBytes = totalBytes.get();
        double totalSec = (System.currentTimeMillis() - startTime) / 1000.0;

        System.out.printf(
                "=== Final ===%nTotal Messages: %d%nTotal MB: %.2f%nThroughput: %.0f msg/s, %.2f MB/s%n",
                finalMessages,
                finalBytes / (1024.0 * 1024.0),
                finalMessages / totalSec,
                (finalBytes / (1024.0 * 1024.0)) / totalSec
        );
    }

    static long getDirtyKB() {
        try {
            for (String line : Files.readAllLines(Paths.get("/proc/meminfo"))) {
                if (line.startsWith("Dirty:")) {
                    return Long.parseLong(line.split("\\s+")[1]);
                }
            }
        } catch (IOException ignored) {}
        return 0;
    }

    static long getTotalMemKB() {
        try {
            for (String line : Files.readAllLines(Paths.get("/proc/meminfo"))) {
                if (line.startsWith("MemTotal:")) {
                    return Long.parseLong(line.split("\\s+")[1]);
                }
            }
        } catch (IOException ignored) {}
        return 0;
    }
}
