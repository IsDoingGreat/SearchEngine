package in.nimbo.isDoing.searchEngine.crawler.persister;

import in.nimbo.isDoing.searchEngine.crawler.controller.Counter;
import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.crawler.persister.db.ElasticDBPersister;
import in.nimbo.isDoing.searchEngine.crawler.persister.db.HBaseDBPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class PersisterThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PersisterThread.class);
    private static final int TOTAL_DELAY = 10000;
    ElasticDBPersister elasticDBPersister;
    HBaseDBPersister hBaseDBPersister;
    private PagePersister persister;
    private volatile boolean isStopped = false;

    public PersisterThread(PagePersister persister, ElasticDBPersister elasticDBPersister,
                           HBaseDBPersister hBaseDBPersister) {
        this.persister = persister;
        this.elasticDBPersister = elasticDBPersister;
        this.hBaseDBPersister = hBaseDBPersister;
    }

    @Override
    public void run() {
        try {
            Thread.sleep((long) (Math.random() * TOTAL_DELAY));
            try {
                while (!Thread.currentThread().isInterrupted() && !isStopped) {
                    Page page = persister.getPageQueue().poll(10, TimeUnit.SECONDS);
                    if (page == null)
                        continue;
                    elasticDBPersister.persist(page);
                    hBaseDBPersister.persist(page);
                    persister.getCounter().increment(Counter.States.PERSISTED);
                }
            } catch (InterruptedException e) {
                logger.info(Thread.currentThread() + "Interrupted... ");
            }

            //If It is Stopped We Do not need to free the blocking Queue;
            if (isStopped)
                return;

            //Trying to free Blocking Queue...
            Page page = null;
            while ((page = persister.getPageQueue().poll()) != null) {
                elasticDBPersister.persist(page);
                hBaseDBPersister.persist(page);
                if (persister.getPageQueue().isEmpty()) {
                    elasticDBPersister.flush();
                    hBaseDBPersister.flush();
                }
            }

            elasticDBPersister.flush();
            hBaseDBPersister.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void stop() {
        isStopped = true;
    }
}
