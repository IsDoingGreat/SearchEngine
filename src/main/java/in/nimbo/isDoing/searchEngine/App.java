package in.nimbo.isDoing.searchEngine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class App implements Runnable {
    UrlQueue<String> queue;

    public static void main(String[] args) throws IOException {
        new App().run();
    }

    @Override
    public void run() {
        initSystem();
    }


    public void initSystem() {
        try {
            queue = new ArrayListUrlQueue();
            Path seedLock = Paths.get("./seed.lock");
            Path seedFile = Paths.get("./seeds.txt");
            if (!Files.exists(seedLock) && Files.exists(seedFile)) {
                Files.createFile(seedLock);
                List<String> lines = Files.readAllLines(seedFile);
                for (String line : lines) {
                    queue.push(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
