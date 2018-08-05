package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.interfaces.CrawlScheduler;
import in.nimbo.isDoing.searchEngine.crawler.interfaces.URLQueue;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class CrawlerService implements Service {
    private final static Logger logger = LoggerFactory.getLogger(CrawlerService.class);
    private CrawlScheduler scheduler;
    private Thread schedulerThread;
    private URLQueue urlQueue;

    public CrawlerService() {
        logger.info("Creating Crawler Service...");
        urlQueue = new KafkaUrlQueue();
        scheduler = new CrawlSchedulerImpl(urlQueue);
        logger.info("Crawler Service Created");
    }

    @Override
    public void start() {
        logger.info("Starting Crawler Service...");

        boolean initSeeds = Boolean.parseBoolean(Engine.getConfigs().get("crawler.initSeeds",
                String.valueOf(true)));


        if (initSeeds)
            initSeeds();


        schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();
    }

    @Override
    public void stop() {
        scheduler.stop();
        schedulerThread.interrupt();
    }

    private void initSeeds() {
        logger.info("loading Seeds...");
        try {
            Path seedLock = Paths.get("./seed.lock");
            Path seedFile = Paths.get("./seeds.txt");
            if (!Files.exists(seedLock) && Files.exists(seedFile)) {
                Files.createFile(seedLock);
                List<String> lines = Files.readAllLines(seedFile);
                for (String line : lines) {
                    urlQueue.push(line);
                }
            }
            logger.info("Seeds loaded");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Status status() {
        return null;
    }

    @Override
    public String getName() {
        return "crawler";
    }
}
