package io.playground;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class Compacter {

    private static boolean shouldRun = true;

    private static final int ENOUGH_ELEMENTS = 15;
    private static final ArrayBlockingQueue<String> availablePaths = new ArrayBlockingQueue<>(
            ENOUGH_ELEMENTS);

    public static void signalForCompaction(String dbPath) {
        availablePaths.offer(dbPath);
    }

    public static void run() {
        while (shouldRun) {

            String dbPath = null;
            try {
                while (dbPath == null) {
                    dbPath = availablePaths.poll(50, TimeUnit.MINUTES);
                }

                System.out.println("Compacter got available db=" + dbPath);

                try (
                        RocksDB db = initDb(dbPath)
                ) {

                    RocksLoad.doFullCompactionOnDefaultColumnFamily(db, 0);
                    System.out.println("Compacter finished processing db=" + dbPath);

                } catch (RocksDBException e) {
                    e.printStackTrace();
                } finally {
                    signalDone(dbPath);
                    System.out.println("]]]]]]]]]]]]]]]]]]]]]]]] Compacter done dbPath=" + dbPath);
                    System.out.println();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void signalDone(String dbPath) {
        System.out.println("Compacter releasing to next stage. db=" + dbPath);
        Reader.signalForRead(dbPath);
    }

    private static RocksDB initDb(String nextDbPath) throws RocksDBException {
        final Options options = new Options()
                .setCreateIfMissing(true)
                .setDisableAutoCompactions(true)
                .setAllowConcurrentMemtableWrite(true);
        final RocksDB db = RocksDB.open(options, nextDbPath);
        return db;
    }
}
