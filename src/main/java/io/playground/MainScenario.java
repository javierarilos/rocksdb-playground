package io.playground;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainScenario {

    private static ExecutorService executor = Executors.newFixedThreadPool(3);

    public static void main(String[] args) {
        executor.submit(() -> Reader.run());
        executor.submit(() -> Reader.run());
        executor.submit(() -> Reader.run());
//        executor.submit(() -> Compacter.run());
//        executor.submit(() -> Writer.run());

//        Reader.signalForRead("/tmp/rocks-db-0");
//        Reader.signalForRead("/tmp/rocks-db-0");
//        Reader.signalForRead("/tmp/rocks-db-0");

        Reader.signalForRead("/tmp/rocks-db-1");
        Reader.signalForRead("/tmp/rocks-db-1");
        Reader.signalForRead("/tmp/rocks-db-1");
    }

}
