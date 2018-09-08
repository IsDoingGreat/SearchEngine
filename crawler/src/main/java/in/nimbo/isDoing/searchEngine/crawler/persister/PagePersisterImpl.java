package in.nimbo.isDoing.searchEngine.crawler.persister;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.isDoing.searchEngine.crawler.controller.Counter;
import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.crawler.persister.db.ElasticDBPersister;
import in.nimbo.isDoing.searchEngine.crawler.persister.db.HBaseDBPersister;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Stateful;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PagePersisterImpl implements PagePersister, Stateful {
    private final static Logger logger = LoggerFactory.getLogger(PagePersisterImpl.class);

    private static final int DEFAULT_THREAD_NUMBER = 2;
    private static final int DEFAULT_QUEUE_SIZE = 300;
    private static final String DEFAULT_ELASTIC_FLUSH_LIMIT = "150";
    private static final String DEFAULT_HBASE_FLUSH_LIMIT = "150";
    private static final String DEFAULT_ELASTIC_FLUSH_SIZE = "2";

    private BlockingQueue<Page> pageQueue;
    private ThreadPoolExecutor persisterExecutor;
    private int persisterThreadNumber;
    private int pageQueueSize;
    private Counter counter;
    private LinkedList<PersisterThread> pagePersisterDeque = new LinkedList<>();

    private int elasticFlushSizeLimit;
    private int elasticFlushNumberLimit;
    private int hbaseFlushNumberLimit;

    public PagePersisterImpl(Counter counter) {
        Engine.getOutput().show("Creating PagePersister...");
        logger.info("Creating PagePersister...");

        this.counter = counter;

        persisterThreadNumber = Integer.parseInt(Engine.getConfigs().get("crawler.persister.persisterThreadNumber",
                String.valueOf(DEFAULT_THREAD_NUMBER)));

        pageQueueSize = Integer.parseInt(Engine.getConfigs().get("crawler.persister.pageQueueSize",
                String.valueOf(DEFAULT_QUEUE_SIZE)));

        elasticFlushSizeLimit = Integer.parseInt(Engine.getConfigs().get(
                "crawler.persister.db.elastic.flushSizeLimit", DEFAULT_ELASTIC_FLUSH_SIZE));

        elasticFlushNumberLimit = Integer.parseInt(Engine.getConfigs().get(
                "crawler.persister.db.elastic.flushNumberLimit", DEFAULT_ELASTIC_FLUSH_LIMIT));

        hbaseFlushNumberLimit = Integer.parseInt(Engine.getConfigs().get(
                "crawler.persister.db.hbase.flushNumberLimit", DEFAULT_HBASE_FLUSH_LIMIT));

        persisterExecutor = new ThreadPoolExecutor(
                0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS, new SynchronousQueue<>(), new ThreadFactory());

        SharedMetricRegistries.getDefault().register("persister-queue-size", (Gauge<Integer>) () -> pageQueue.size());
        SharedMetricRegistries.getDefault().register("persister-liveTreads", (Gauge<Integer>) () -> persisterExecutor.getActiveCount());

        pageQueue = new LinkedBlockingQueue<>(pageQueueSize);


        //Initializing Runnables To See If There is Any Error!!
        for (int i = 0; i < persisterThreadNumber; i++) {
            pagePersisterDeque.addLast(new PersisterThread(this, new ElasticDBPersister(this), new HBaseDBPersister(this)));
        }
        logger.info("PagePersister Created...");
    }

    public int getElasticFlushSizeLimit() {
        return elasticFlushSizeLimit;
    }

    public int getElasticFlushNumberLimit() {
        return elasticFlushNumberLimit;
    }

    public int getHbaseFlushNumberLimit() {
        return hbaseFlushNumberLimit;
    }

    @Override
    public void stop() {
        Engine.getOutput().show("Waiting For Persister Threads To Stop... (At Most 10 Seconds)");
        persisterExecutor.shutdownNow();
        try {
            persisterExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void start() {
        logger.info("Starting PagePersister...");

        for (PersisterThread persisterThread : pagePersisterDeque) {
            persisterExecutor.execute(persisterThread);
        }
    }

    private void addNewThread() {
        PersisterThread persisterThread = new PersisterThread(this, new ElasticDBPersister(this), new HBaseDBPersister(this));
        pagePersisterDeque.addLast(persisterThread);
        persisterExecutor.submit(persisterThread);
    }

    @Override
    public Counter getCounter() {
        return counter;
    }

    @Override
    public BlockingQueue<Page> getPageQueue() {
        return pageQueue;
    }

    @Override
    public void reload() {
        elasticFlushSizeLimit = Integer.parseInt(Engine.getConfigs().get(
                "crawler.persister.db.elastic.flushSizeLimit", DEFAULT_ELASTIC_FLUSH_SIZE));
        logger.info("reload: new elasticFlushSizeLimit : {}", elasticFlushSizeLimit);


        elasticFlushNumberLimit = Integer.parseInt(Engine.getConfigs().get(
                "crawler.persister.db.elastic.flushNumberLimit", DEFAULT_ELASTIC_FLUSH_LIMIT));
        logger.info("reload: new elasticFlushNumberLimit : {}", elasticFlushNumberLimit);


        hbaseFlushNumberLimit = Integer.parseInt(Engine.getConfigs().get(
                "crawler.persister.db.hbase.flushNumberLimit", DEFAULT_HBASE_FLUSH_LIMIT));
        logger.info("reload: new hbaseFlushNumberLimit : {}", hbaseFlushNumberLimit);


        int oldPersisterThreadNumber = persisterThreadNumber;
        persisterThreadNumber = Integer.parseInt(Engine.getConfigs().get("crawler.persister.persisterThreadNumber",
                String.valueOf(DEFAULT_THREAD_NUMBER)));
        logger.info("reload: new persisterThreadCount : {}", persisterThreadNumber);

        if (oldPersisterThreadNumber != persisterThreadNumber) {
            if (persisterThreadNumber > oldPersisterThreadNumber) {
                for (int i = 0; i < persisterThreadNumber - oldPersisterThreadNumber; i++)
                    addNewThread();
            } else {
                for (int i = 0; i < oldPersisterThreadNumber - persisterThreadNumber; i++) {
                    PersisterThread persisterThread = pagePersisterDeque.pollFirst();
                    if (persisterThread != null)
                        persisterThread.stop();
                }
            }
        }

    }

    @Override
    public Map<String, Object> status() {
        Map<String, Object> map = new HashMap<>();
        map.put("pageQueueSize", pageQueue.size());
        map.put("livePersisterThreads", persisterExecutor.getActiveCount());
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
            namePrefix = "persister-thread-";
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
