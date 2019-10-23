package io.playground;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class Writer {

    public static final String DB_BASE_PATH = "/tmp/rocks-db-";
    private static boolean shouldRun = true;

    private static final int TOTAL_DBS = 2;
    private static final ArrayBlockingQueue<String> availablePaths = new ArrayBlockingQueue<>(
            TOTAL_DBS);

    static {
        initAvailablePaths();
    }

    public static void signalForWrite(String dbPath) {
        availablePaths.offer(dbPath);
    }

    private static void initAvailablePaths() {
        for (int i = 0; i < TOTAL_DBS; i++) {
            String nextDbPath = DB_BASE_PATH + i;
            signalForWrite(nextDbPath);
        }
    }

    public static void run() {
        while (shouldRun) {

            String dbPath = null;
            try {
                while (dbPath == null) {
                    dbPath = availablePaths.poll(50, TimeUnit.MINUTES);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("Writer got available db=" + dbPath);

            try (
                    RocksDB db = initDb(dbPath)
            ) {
                RocksLoad.fillDb(db, 20, 3);

                System.out.println("Writer finished processing db=" + dbPath);

            } catch (RocksDBException e) {
                e.printStackTrace();
            } finally {
                signalDone(dbPath);
                System.out.println("########################## Writer is done dbPath=" + dbPath);
                System.out.println();
            }

        }
    }

    private static void signalDone(String dbPath) {
        System.out.println("Writer releasing to next stage. db=" + dbPath);
        Compacter.signalForCompaction(dbPath);
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
