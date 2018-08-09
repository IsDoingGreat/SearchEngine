package in.nimbo.isDoing.searchEngine.crawler.scheduler;

import in.nimbo.isDoing.searchEngine.crawler.controller.PageCrawlerController;
import in.nimbo.isDoing.searchEngine.crawler.controller.PageCrawlerControllerImpl;
import in.nimbo.isDoing.searchEngine.crawler.page.LanguageDetector;
import in.nimbo.isDoing.searchEngine.crawler.page_crawler.PageCrawlerImpl;
import in.nimbo.isDoing.searchEngine.crawler.urlqueue.URLQueue;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CrawlSchedulerImpl implements CrawlScheduler {
    private final static Logger logger = LoggerFactory.getLogger(CrawlSchedulerImpl.class);
    private static final int DEFAULT_MAX_ACTIVE_CRAWLERS = 100;
    private static final int DEFAULT_QUEUE_SIZE = 100;
    private static final int DEFAULT_QUEUE_POP_SIZE = 100;


    private int maxActiveCrawlers;
    private int queueSize;
    private int queuePopSize;
    private PageCrawlerController controller;
    private ExecutorService executor;
    private Date startDate = null;
    private volatile boolean exitRequested = false;
    private BlockingQueue<String> queue;
    private URLQueue urlQueue;
    private Thread counterThread;

    public CrawlSchedulerImpl(URLQueue urlQueue) throws IOException {
        logger.info("Creating scheduler");
        Engine.getOutput().show("Creating scheduler...");

        maxActiveCrawlers = Integer.parseInt(Engine.getConfigs().get("crawler.scheduler.activeCrawlers",
                String.valueOf(DEFAULT_MAX_ACTIVE_CRAWLERS)));

        logger.info("Using maxActiveCrawlers={}", maxActiveCrawlers);

        queueSize = Integer.parseInt(Engine.getConfigs().get("crawler.scheduler.queueSize",
                String.valueOf(DEFAULT_QUEUE_SIZE)));
        logger.info("Using queueSize={}", queueSize);

        queuePopSize = Integer.parseInt(Engine.getConfigs().get("crawler.scheduler.queuePopSize",
                String.valueOf(DEFAULT_QUEUE_POP_SIZE)));

        logger.info("Using queuePopSize={}", queueSize);

        queue = new LinkedBlockingQueue<>(queueSize);
        this.controller = new PageCrawlerControllerImpl(queue, urlQueue);
        this.urlQueue = urlQueue;

        LanguageDetector.getInstance();

        executor = new ThreadPoolExecutor(maxActiveCrawlers, maxActiveCrawlers,
                0L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactory());

        logger.info("scheduler Created");
    }

    public void start() {
        if (startDate != null)
            throw new IllegalStateException("Scheduler Already Started");

        startDate = new Date();

        logger.info("Starting scheduler at {}", startDate);

        int numberOfThreads = 0;
        while (numberOfThreads < maxActiveCrawlers) {
            executor.submit(new PageCrawlerImpl(controller));
            numberOfThreads++;
        }

        counterThread = new Thread(controller.getCounter());
        counterThread.setName("CounterThread");
        counterThread.setDaemon(true);

        try {
            Thread.sleep(1500);
            counterThread.start();
            while (!exitRequested && !Thread.interrupted()) {
                List<String> urlList = urlQueue.pop(queuePopSize);
                logger.trace("{} urls poped from URLQueue", urlList.size());
                logger.trace("{} urls in Blocking Queue", queue.size());
                for (String url : urlList) {
                    queue.put(url);
                }

            }
        } catch (InterruptedException e) {
            logger.warn("Scheduler Thread Interrupted {}", e);
        }
    }

    @Override
    public void run() {
        start();
    }

    @Override
    public void stop() {
        try {
            exitRequested = true;
            Engine.getOutput().show("Waiting for Fetcher Threads To Stop (At Most 10 Seconds)... ");
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            Engine.getOutput().show("Stopping Controller... ");
            controller.stop();

            Engine.getOutput().show("Stopping URLQueue... ");
            urlQueue.stop();

            Engine.getOutput().show("Intercepting Counter Thread... ");
            counterThread.interrupt();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class ThreadFactory implements java.util.concurrent.ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        ThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = "fetcher-" +
                    poolNumber.getAndIncrement() +
                    "-thread-";
        }

        @Override
        public Thread newThread(@NotNull Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);

            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
}
