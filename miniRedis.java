
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.AtomicMoveNotSupportedException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class miniRedis implements AutoCloseable {

    private final ConcurrentMap<String, String> database =
            new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Long> expiryTimes =
            new ConcurrentHashMap<>();

    private final Path snapshotPath = Path.of("dump.txt");
    private final Path tempPath = Path.of("dump.tmp");

    // One lock protects consistent database and TTL snapshots.
    private final Object dataLock = new Object();

    private final ScheduledExecutorService snapshotScheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "mini-redis-snapshot");
                thread.setDaemon(true);
                return thread;
            });

    private volatile boolean autoSaveStarted = false;

    public miniRedis() {
        loadSnapshot();
    }

    public void startAutoSave() {
        synchronized (dataLock) {
            if (autoSaveStarted) {
                return;
            }

            autoSaveStarted = true;
        }

        snapshotScheduler.scheduleAtFixedRate(() -> {
            try {
                saveSnapshot();
                System.out.println("Automatic snapshot saved.");
            } catch (IOException e) {
                System.err.println(
                        "Automatic snapshot failed: " + e.getMessage()
                );
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    public String execute(String command) {

        if (command == null || command.isBlank()) {
            return "ERROR: Empty command";
        }

        String[] parts = command.trim().split("\\s+");
        String operation = parts[0].toUpperCase();

        synchronized (dataLock) {
            return switch (operation) {

                case "SET" -> {
                    if (parts.length < 3) {
                        yield "ERROR: Usage SET key value [EX seconds]";
                    }

                    String key = parts[1];
                    String value = parts[2];

                    // SET key value
                    if (parts.length == 3) {
                        database.put(key, value);
                        expiryTimes.remove(key);
                        yield "OK";
                    }

                    // SET key value EX seconds
                    if (parts.length == 5
                            && parts[3].equalsIgnoreCase("EX")) {
                        try {
                            long seconds = Long.parseLong(parts[4]);

                            if (seconds <= 0) {
                                yield "ERROR: Expiry must be positive";
                            }

                            long expiry = Math.addExact(
                                    System.currentTimeMillis(),
                                    TimeUnit.SECONDS.toMillis(seconds)
                            );

                            database.put(key, value);
                            expiryTimes.put(key, expiry);

                            yield "OK";

                        } catch (NumberFormatException e) {
                            yield "ERROR: Invalid expiry seconds";
                        } catch (ArithmeticException e) {
                            yield "ERROR: Expiry is too large";
                        }
                    }

                    yield "ERROR: Usage SET key value [EX seconds]";
                }

                case "GET" -> {
                    if (parts.length != 2) {
                        yield "ERROR: Usage GET key";
                    }

                    String key = parts[1];
                    removeIfExpired(key);

                    yield database.getOrDefault(key, "(nil)");
                }

                case "DEL" -> {
                    if (parts.length != 2) {
                        yield "ERROR: Usage DEL key";
                    }

                    String key = parts[1];
                    removeIfExpired(key);
                    expiryTimes.remove(key);

                    yield database.remove(key) != null ? "1" : "0";
                }

                case "EXISTS" -> {
                    if (parts.length != 2) {
                        yield "ERROR: Usage EXISTS key";
                    }

                    String key = parts[1];
                    removeIfExpired(key);

                    yield database.containsKey(key) ? "1" : "0";
                }

                case "TTL" -> {
                    if (parts.length != 2) {
                        yield "ERROR: Usage TTL key";
                    }

                    String key = parts[1];
                    removeIfExpired(key);

                    if (!database.containsKey(key)) {
                        yield "-2";
                    }

                    Long expiry = expiryTimes.get(key);

                    if (expiry == null) {
                        yield "-1";
                    }

                    long remainingMillis =
                            expiry - System.currentTimeMillis();

                    long remainingSeconds =
                            (remainingMillis + 999) / 1000;

                    yield String.valueOf(
                            Math.max(0, remainingSeconds)
                    );
                }

                case "SAVE" -> {
                    if (parts.length != 1) {
                        yield "ERROR: Usage SAVE";
                    }

                    try {
                        saveSnapshot();
                        yield "OK";
                    } catch (IOException e) {
                        System.err.println(
                                "Snapshot save failed: " + e.getMessage()
                        );
                        yield "ERROR: Could not save snapshot";
                    }
                }

                default -> "ERROR: Unknown command";
            };
        }
    }

    private void removeIfExpired(String key) {
        Long expiry = expiryTimes.get(key);

        if (expiry != null
                && System.currentTimeMillis() >= expiry) {
            expiryTimes.remove(key);
            database.remove(key);
        }
    }

    private void saveSnapshot() throws IOException {
        synchronized (dataLock) {
            List<String> lines = new ArrayList<>();

            for (var entry : database.entrySet()) {
                String key = entry.getKey();
                removeIfExpired(key);

                String value = database.get(key);

                if (value == null) {
                    continue;
                }

                long expiry = expiryTimes.getOrDefault(key, -1L);

                lines.add(
                        encode(key) + "\t"
                                + encode(value) + "\t"
                                + expiry
                );
            }

            Files.write(
                    tempPath,
                    lines,
                    StandardCharsets.UTF_8
            );

            try {
                Files.move(
                        tempPath,
                        snapshotPath,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE
                );
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(
                        tempPath,
                        snapshotPath,
                        StandardCopyOption.REPLACE_EXISTING
                );
            }
        }
    }

    private void loadSnapshot() {
        if (!Files.exists(snapshotPath)) {
            System.out.println("No snapshot found. Starting empty.");
            return;
        }

        try {
            List<String> lines = Files.readAllLines(
                    snapshotPath,
                    StandardCharsets.UTF_8
            );

            int loaded = 0;

            for (String line : lines) {
                try {
                    String[] fields = line.split("\t", -1);

                    if (fields.length != 3) {
                        continue;
                    }

                    String key = decode(fields[0]);
                    String value = decode(fields[1]);
                    long expiry = Long.parseLong(fields[2]);

                    if (expiry != -1
                            && System.currentTimeMillis() >= expiry) {
                        continue;
                    }

                    database.put(key, value);

                    if (expiry != -1) {
                        expiryTimes.put(key, expiry);
                    }

                    loaded++;

                } catch (IllegalArgumentException e) {
                    System.err.println("Skipping invalid snapshot entry.");
                }
            }

            System.out.println(
                    "Snapshot loaded. Keys restored: " + loaded
            );

        } catch (IOException e) {
            System.err.println(
                    "Could not load snapshot: " + e.getMessage()
            );
        }
    }

    private String encode(String value) {
        return Base64.getEncoder().encodeToString(
                value.getBytes(StandardCharsets.UTF_8)
        );
    }

    private String decode(String value) {
        return new String(
                Base64.getDecoder().decode(value),
                StandardCharsets.UTF_8
        );
    }

    @Override
    public void close() {
        snapshotScheduler.shutdownNow();

        try {
            saveSnapshot();
            System.out.println("Final snapshot saved.");
        } catch (IOException e) {
            System.err.println(
                    "Final snapshot failed: " + e.getMessage()
            );
        }
    }
}
