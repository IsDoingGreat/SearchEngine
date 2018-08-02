package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.interfaces.CrawlScheduler;
import in.nimbo.isDoing.searchEngine.crawler.interfaces.PageCrawlerController;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import in.nimbo.isDoing.searchEngine.engine.Status;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class CrawlerService implements Service {
    PageCrawlerController controller;
    CrawlScheduler scheduler;

    @Override
    public void start() {

    }

    private void initSeeds(){
        try {
            Path seedLock = Paths.get("./seed.lock");
            Path seedFile = Paths.get("./seeds.txt");
            if (/*!Files.exists(seedLock) &&*/ Files.exists(seedFile)) {
//                Files.createFile(seedLock);
                List<String> lines = Files.readAllLines(seedFile);
                for (String line : lines) {
                    queue.push(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Status status() {
        return null;
    }
}
