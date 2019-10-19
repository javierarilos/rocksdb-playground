package io.playground;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainScenario {

    private static ExecutorService executor = Executors.newFixedThreadPool(3);

    public static void main(String[] args) {
        executor.submit(() -> Reader.run());
        executor.submit(() -> Compacter.run());
        executor.submit(() -> Writer.run());
    }

}
