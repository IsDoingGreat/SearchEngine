package in.nimbo.isDoing.searchEngine.crawler.persister;

import in.nimbo.isDoing.searchEngine.crawler.controller.Counter;
import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.crawler.persister.db.ElasticDBPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersisterThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PersisterThread.class);
    ElasticDBPersister elasticDBPersister;
    private PagePersister persister;

    public PersisterThread(PagePersister persister, ElasticDBPersister elasticDBPersister) {
        this.persister = persister;
        this.elasticDBPersister = elasticDBPersister;
    }

    @Override
    public void run() {
        try {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Page page = persister.getPageQueue().take();
                    elasticDBPersister.persist(page);
                    persister.getCounter().increment(Counter.States.PERSISTED);
                }

            } catch (InterruptedException e) {
                logger.info(Thread.currentThread() + "Interrupted... ");
            }

            //Trying to free Blocking Queue...
            Page page = null;
            while ((page = persister.getPageQueue().poll()) != null) {
                elasticDBPersister.persist(page);
                if (persister.getPageQueue().isEmpty())
                    elasticDBPersister.flush();
            }

            elasticDBPersister.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
