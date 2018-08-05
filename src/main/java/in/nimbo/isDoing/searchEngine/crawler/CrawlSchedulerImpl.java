package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.interfaces.CrawlScheduler;
import in.nimbo.isDoing.searchEngine.crawler.interfaces.PageCrawlerController;
import in.nimbo.isDoing.searchEngine.crawler.interfaces.URLQueue;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CrawlSchedulerImpl implements CrawlScheduler {
    private final static Logger logger = LoggerFactory.getLogger(CrawlSchedulerImpl.class);
    private static final int DEFAULT_MAX_ACTIVE_CRAWLERS = 100;
    private static final int DEFAULT_QUEUE_SIZE = 100;


    private int maxActiveCrawlers;
    private int queueSize;
    private PageCrawlerController controller;
    private ExecutorService executor;
    private Date startDate = null;
    private volatile boolean exitRequested = false;
    private BlockingQueue<String> queue;
    private URLQueue urlQueue;
    private Thread counterThread;

    public CrawlSchedulerImpl(URLQueue urlQueue) {
        logger.info("Creating CrawlScheduler");

        maxActiveCrawlers = Integer.parseInt(Engine.getConfigs().get("crawler.scheduler.activeCrawlers",
                String.valueOf(DEFAULT_MAX_ACTIVE_CRAWLERS)));

        logger.info("Using maxActiveCrawlers={}", maxActiveCrawlers);

        queueSize = Integer.parseInt(Engine.getConfigs().get("crawler.scheduler.queueSize",
                String.valueOf(DEFAULT_QUEUE_SIZE)));
        logger.info("Using queueSize={}", queueSize);

        queue = new LinkedBlockingQueue<>(queueSize);
        this.controller = new PageCrawlerControllerImpl(queue, urlQueue);
        this.urlQueue = urlQueue;

        executor = new ThreadPoolExecutor(maxActiveCrawlers, maxActiveCrawlers,
                0L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactory());

        logger.info("CrawlScheduler Created");
    }

    public void startCrawling() {
        if (startDate != null)
            throw new IllegalStateException("Scheduler Already Started");

        startDate = new Date();

        logger.info("Starting CrawlScheduler at {}", startDate);

        int numberOfThreads = 0;
        while (numberOfThreads < maxActiveCrawlers) {
            executor.submit(new PageCrawlerImpl(controller));
            numberOfThreads++;
        }


        counterThread = new Thread(() -> {
            try {
                int lastCount = 0;
                while (!Thread.interrupted()) {
                    Thread.sleep(1000);
                    int totalCrawls = controller.getTotalCrawls();
                    logger.info("Crawled Per Second:" + (totalCrawls - lastCount));
                    lastCount = totalCrawls;
                }

            } catch (InterruptedException e) {
                logger.info("counterThread stopped");
            }
        });
        counterThread.setDaemon(true);
        counterThread.start();


        try {
            while (!exitRequested && !Thread.interrupted()) {
                List<String> urlList = urlQueue.pop(queueSize);

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
        startCrawling();
    }

    @Override
    public void stop() {
        exitRequested = true;
        counterThread.interrupt();
        executor.shutdown();
        controller.stop();
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
            namePrefix = "pool-" +
                    poolNumber.getAndIncrement() +
                    "-thread-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);

            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
}
