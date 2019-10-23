package io.playground;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class Reader {

    private static boolean shouldRun = true;

    private static final int ENOUGH_ELEMENTS = 15;
    private static final ArrayBlockingQueue<String> availablePaths = new ArrayBlockingQueue<>(
            ENOUGH_ELEMENTS);


    public static boolean isGoOnReading() {
        return goOnReading;
    }

    public static void setGoOnReading(boolean goOnReading) {
        Reader.goOnReading = goOnReading;
    }

    private static boolean goOnReading = true;


    public static void signalForRead(String dbPath) {
        availablePaths.offer(dbPath);
    }

    public static void run() {

        String dbPath = null;
        String newDbPath = null;
        try {
            while (dbPath == null) {
                dbPath = availablePaths.poll(10, TimeUnit.MINUTES);
            }

            System.out.println("Reader got available db=" + dbPath);

            while (shouldRun) {
                try (
                        RocksDB db = initDbReadOnly(dbPath)
                ) {
                    Future<?> x = submitBackgroundReads(db);

                    while (newDbPath == null) {
                        newDbPath = availablePaths.poll(10, TimeUnit.MINUTES);
                    }
                    System.out.println(
                            "Reader finished processing db=" + dbPath + " received newDb="
                                    + newDbPath);
                    waitForReader(x);
                    System.out.println("Reader background finished. Closing DB and notifying.");

                } catch (RocksDBException e) {
                    e.printStackTrace();
                } finally {
                    System.out.println("NOT PURGING DB on end on READER!!!!!!!!!");
//                    System.out.println("Purging dbPath=" + dbPath);
//                    rmRfDb(dbPath);
//                    System.out.println("Purged dbPath=" + dbPath + " notifying to reader.");
//                    signalDone(dbPath);
//                    dbPath = newDbPath;
//                    newDbPath = null;
//                    System.out.println("||||||||||||||||||||||||||| Reader done dbPath=" + dbPath);
//                    System.out.println();
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void rmRfDb(String dbPath) {
        try {
            Process process = Runtime.getRuntime().exec("rm -rf " + dbPath);
            process.onExit().get();
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void waitForReader(Future<?> x)
            throws InterruptedException, ExecutionException {
        setGoOnReading(false);
        x.get();
    }

    private static Future<?> submitBackgroundReads(RocksDB db) {
        setGoOnReading(true);
        return Executors.newSingleThreadExecutor()
                .submit(() -> {
                    long initTime = System.nanoTime();
                    long totalReads = 0;
                    int hits = 0;
                    int misses = 0;
//                    while (isGoOnReading()) {
                    while (totalReads < 1_000_000) {
                        try {
                            byte[] x = RocksLoad.doRandomRead(db);
                            if (totalReads % 10_000 == 0) {
                                System.out.println("total-reads=" + totalReads);
                                if (x != null) {
                                    System.out.println("value len=" + x.length);
                                } else {
                                    System.out.println("value is a miss");
                                }
                            }
                            if (x == null || x.length == 0) {
                                misses++;
                            } else {
                                hits++;
                            }
                            totalReads++;
//                            if (totalReads % 10_000 == 0) {
//                                System.out.println("value_len="+x.length);
//                            }
                        } catch (RocksDBException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println(String.format(
                            "Stop reading totalReads=%d (%d hits / %d misses) in ms=%d",
                            totalReads, hits, misses, (System.nanoTime() - initTime) / 1_000_000));
                });
    }

    private static void signalDone(String dbPath) {
        System.out.println("Reader releasing to next stage. db=" + dbPath);
        Writer.signalForWrite(dbPath);
    }

    private static RocksDB initDbReadOnly(String nextDbPath) throws RocksDBException {
        final Options options = new Options()
                .setCreateIfMissing(true)
                .setDisableAutoCompactions(true)
                .setAllowConcurrentMemtableWrite(true)
                .setRandomAccessMaxBufferSize(1_000_000);
        final RocksDB db = RocksDB.openReadOnly(options, nextDbPath);
        return db;
    }

}
