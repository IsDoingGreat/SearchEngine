package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.scheduler.CrawlScheduler;
import in.nimbo.isDoing.searchEngine.crawler.scheduler.CrawlSchedulerImpl;
import in.nimbo.isDoing.searchEngine.crawler.server.LocalServer;
import in.nimbo.isDoing.searchEngine.crawler.urlqueue.KafkaUrlQueue;
import in.nimbo.isDoing.searchEngine.crawler.urlqueue.URLQueue;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.SystemConfigs;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Stateful;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CrawlerService implements Service {
    private final static Logger logger = LoggerFactory.getLogger(CrawlerService.class);
    private CrawlScheduler scheduler;
    private Thread schedulerThread;
    private URLQueue urlQueue;
    private LocalServer localServer = new LocalServer();

    public CrawlerService() throws IOException {
        logger.info("Creating Crawler Service...");
        Engine.getOutput().show("Creating Crawler Service...");
        urlQueue = new KafkaUrlQueue();
//        urlQueue = new ArrayListURLQueueImpl();
        scheduler = new CrawlSchedulerImpl(urlQueue);
        logger.info("Crawler Service Created");
        Engine.getOutput().show("Crawler Service Created");
    }

    public static void main(String[] args) throws Exception {
        Engine.start(new ConsoleOutput(), new SystemConfigs("crawler"));
        Engine.getInstance().startService(new CrawlerService());
    }

    @Override
    public void start() {
        try {
            logger.info("Starting Crawler Service...");
            Engine.getOutput().show("Starting Crawler Service...");

            localServer.start();
            Engine.getOutput().show("LocalServer Started...");

            boolean initSeeds = Boolean.parseBoolean(Engine.getConfigs().get("crawler.initSeeds",
                    String.valueOf(true)));


            if (initSeeds)
                initSeeds();

            schedulerThread = new Thread(scheduler);
            schedulerThread.setDaemon(true);
            schedulerThread.start();
        } catch (Exception e) {
            logger.error("Error ");
        }
    }

    @Override
    public void stop() {
        Engine.getOutput().show("Stopping CrawlerService... ");
        scheduler.stop();

        Engine.getOutput().show("Stopping URLQueue... ");
        urlQueue.stop();

        Engine.getOutput().show("Interrupting Scheduler Thread... ");
        schedulerThread.interrupt();
    }

    public void reload() {
        scheduler.reload();
    }

    private void initSeeds() {
        try {
            Path seedLock = Paths.get("./seed.lock");
            Path seedFile = Paths.get("./seeds.txt");
            if (!Files.exists(seedLock) && Files.exists(seedFile)) {
                logger.info("loading Seeds...");
                Engine.getOutput().show("loading Seeds...");
                Files.createFile(seedLock);
                List<String> lines = Files.readAllLines(seedFile);
                for (String line : lines) {
                    urlQueue.push(line);
                }
                logger.info("Seeds loaded");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> status() {
        Map<String, Object> map = new HashMap<>();

        if (scheduler instanceof Stateful)
            map.putAll(((Stateful) scheduler).status());

        return map;
    }

    @Override
    public String getName() {
        return "crawler";
    }
}
