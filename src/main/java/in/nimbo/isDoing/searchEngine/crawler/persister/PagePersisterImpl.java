package in.nimbo.isDoing.searchEngine.crawler.persister;

import in.nimbo.isDoing.searchEngine.crawler.controller.Counter;
import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.crawler.persister.db.ElasticDBPersister;
import in.nimbo.isDoing.searchEngine.crawler.persister.db.HBaseDBPersister;
import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.engine.interfaces.HaveStatus;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PagePersisterImpl implements PagePersister, HaveStatus {
    private final static Logger logger = LoggerFactory.getLogger(PagePersisterImpl.class);

    private static final int DEFAULT_THREAD_NUMBER = 2;
    private static final int DEFAULT_QUEUE_SIZE = 300;

    private BlockingQueue<Page> pageQueue;
    private ExecutorService persisterExecutor;
    private int persisterThreadNumber;
    private int pageQueueSize;
    private Runnable[] persisterThreads;
    private Counter counter;

    public PagePersisterImpl(Counter counter) {
        Engine.getOutput().show("Creating PagePersister...");
        logger.info("Creating PagePersister...");

        this.counter = counter;

        persisterThreadNumber = Integer.parseInt(Engine.getConfigs().get("crawler.persister.persisterThreadNumber",
                String.valueOf(DEFAULT_THREAD_NUMBER)));

        pageQueueSize = Integer.parseInt(Engine.getConfigs().get("crawler.persister.pageQueueSize",
                String.valueOf(DEFAULT_QUEUE_SIZE)));

        persisterExecutor = new ThreadPoolExecutor(
                persisterThreadNumber, persisterThreadNumber,
                0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactory());

        pageQueue = new LinkedBlockingQueue<>(pageQueueSize);


        //Initializing Runnables To See If There is Any Error!!
        persisterThreads = new Runnable[persisterThreadNumber];
        for (int i = 0; i < persisterThreadNumber; i++) {
            persisterThreads[i] = new PersisterThread(this, new ElasticDBPersister(), new HBaseDBPersister());
        }
        logger.info("PagePersister Created...");
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

        for (int i = 0; i < persisterThreadNumber; i++) {
            persisterExecutor.submit(persisterThreads[i]);
        }
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
    public Status status() {
        Status status = new Status("Elastic DB", "");
        try {
            Response response = ElasticClient.getClient().getLowLevelClient().performRequest("GET", "/_cluster/health");
            ClusterHealthStatus healthStatus;
            try (InputStream is = response.getEntity().getContent()) {
                Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    status.addLine(entry.getKey() + ": " + entry.getValue());
                }
            }
        } catch (IOException e) {
            status().addLine(e.getMessage());
        }

        return status;
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
