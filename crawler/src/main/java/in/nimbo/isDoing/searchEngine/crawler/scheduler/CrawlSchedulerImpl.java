package in.nimbo.isDoing.searchEngine.crawler.scheduler;

import in.nimbo.isDoing.searchEngine.crawler.controller.Counter;
import in.nimbo.isDoing.searchEngine.crawler.controller.PageCrawlerController;
import in.nimbo.isDoing.searchEngine.crawler.controller.PageCrawlerControllerImpl;
import in.nimbo.isDoing.searchEngine.crawler.page.LanguageDetector;
import in.nimbo.isDoing.searchEngine.crawler.page_crawler.PageCrawlerImpl;
import in.nimbo.isDoing.searchEngine.crawler.urlqueue.URLQueue;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Stateful;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CrawlSchedulerImpl implements CrawlScheduler, Stateful {
    private final static Logger logger = LoggerFactory.getLogger(CrawlSchedulerImpl.class);
    private static final int DEFAULT_MAX_ACTIVE_CRAWLERS = 100;
    private static final int DEFAULT_QUEUE_SIZE = 100;
    private static final int DEFAULT_QUEUE_POP_SIZE = 100;


    private int maxActiveCrawlers;
    private int queueSize;
    private int queuePopSize;
    private PageCrawlerController controller;
    private ThreadPoolExecutor executor;
    private Date startDate = null;
    private volatile boolean exitRequested = false;
    private BlockingQueue<String> queue;
    private URLQueue urlQueue;
    private Thread counterThread;

    public CrawlSchedulerImpl(URLQueue urlQueue) throws IOException {
        logger.info("Creating scheduler");
        Engine.getOutput().show("Creating scheduler...");

        reload();
        queue = new LinkedBlockingQueue<>(queueSize);
        this.controller = new PageCrawlerControllerImpl(queue, urlQueue);
        this.urlQueue = urlQueue;

        LanguageDetector.getInstance();

        executor = new ThreadPoolExecutor(maxActiveCrawlers, maxActiveCrawlers,
                0L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactory());

        logger.info("scheduler Created");
    }

    @Override
    public void reload() {

        int oldActiveCrawlers = maxActiveCrawlers;
        maxActiveCrawlers = Integer.parseInt(Engine.getConfigs().get("crawler.scheduler.activeCrawlers",
                String.valueOf(DEFAULT_MAX_ACTIVE_CRAWLERS)));
        logger.info("Using maxActiveCrawlers={}", maxActiveCrawlers);
        queueSize = Integer.parseInt(Engine.getConfigs().get("crawler.scheduler.queueSize",
                String.valueOf(DEFAULT_QUEUE_SIZE)));
        logger.info("Using queueSize={} QueueSize Cant Be Reloaded.", queueSize);
        queuePopSize = Integer.parseInt(Engine.getConfigs().get("crawler.scheduler.queuePopSize",
                String.valueOf(DEFAULT_QUEUE_POP_SIZE)));
        logger.info("Using queuePopSize={}", queueSize);


        //todo
//        executor.setCorePoolSize(maxActiveCrawlers);
//        executor.setMaximumPoolSize(maxActiveCrawlers);
//        if (maxActiveCrawlers > oldActiveCrawlers) {
//            for (int i = 0; i < maxActiveCrawlers - oldActiveCrawlers; i++) {
//                executor.execute(new PageCrawlerImpl(controller));
//            }
//        }

        logger.info("scheduler Reloaded");
    }

    public void start() {
        if (startDate != null)
            throw new IllegalStateException("Scheduler Already Started");

        startDate = new Date();

        logger.info("Starting scheduler at {}", startDate);

        controller.getPersister().start();

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
//                logger.trace("{} urls poped from URLQueue", urlList.size());
//                logger.trace("{} urls in Blocking Queue", queue.size());
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

            int tries = 0;
            Engine.getOutput().show("Waiting for Fetcher Threads To Stop (At Most 5 Tries and each 5 Seconds)... ");
            executor.shutdownNow();

            while (tries < 5 && !executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                tries++;
                Engine.getOutput().show(Output.Type.WARN,
                        "Fetcher Threads Termination is taking too long. Waiting for one more time (Try" + tries + " )...");
                Engine.getOutput().show(String.valueOf(executor.getActiveCount()));
            }

            if (executor.getActiveCount() == 0)
                Engine.getOutput().show(Output.Type.SUCCESS,
                        "Fetcher Threads Terminated");
            else
                Engine.getOutput().show(Output.Type.ERROR,
                        "Fetcher Threads Termination Not Completed in " + tries + " Tries. Ignoring...");

            Engine.getOutput().show("Stopping Controller... ");
            controller.stop();

            Engine.getOutput().show("Interrupting Counter Thread... ");
            if (counterThread != null) {
                counterThread.interrupt();
            }

        } catch (InterruptedException e) {
            Engine.getOutput().show(Output.Type.ERROR, "Error During Stop Process of Scheduler. Check Logs");
            logger.warn("Error in Stopping", e);
        }
    }

    @Override
    public Map<String, Object> status() {
        Map<String, Object> map = new HashMap<>();
        map.put("start Time", startDate);
        map.put("URL Blocking Queue Size Now", queue.size());
        long seconds = (new Date().getTime() - startDate.getTime()) / 1000;
        map.put("runningSeconds :", seconds);
        map.put("avg_total", (controller.getCounter().get(Counter.States.TOTAL) + 0.0) / seconds);
        map.put("avg_duplicate", (controller.getCounter().get(Counter.States.DUPLICATE) + 0.0) / seconds);
        map.put("avg_fetcher_error", (controller.getCounter().get(Counter.States.FETCHER_ERROR) + 0.0) / seconds);
        map.put("avg_lru_rejected", (controller.getCounter().get(Counter.States.LRU_REJECTED) + 0.0) / seconds);
        map.put("avg_invalid_lang", (controller.getCounter().get(Counter.States.INVALID_LANG) + 0.0) / seconds);
        map.put("avg_successful", (controller.getCounter().get(Counter.States.SUCCESSFUL) + 0.0) / seconds);
        map.put("avg_persisted", (controller.getCounter().get(Counter.States.PERSISTED) + 0.0) / seconds);

        if (controller instanceof Stateful)
            map.put("controller", ((Stateful) controller).status());

        return map;
    }

    private static class ThreadFactory implements java.util.concurrent.ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        ThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = "fetcher-" +
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
