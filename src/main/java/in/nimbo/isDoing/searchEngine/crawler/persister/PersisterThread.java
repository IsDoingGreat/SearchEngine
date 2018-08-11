package in.nimbo.isDoing.searchEngine.crawler.persister;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.crawler.persister.db.ElasticDBPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class PersisterThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PersisterThread.class);
    ElasticDBPersister elasticDBPersister;
    private BlockingQueue<Page> pageQueue;

    public PersisterThread(BlockingQueue<Page> pageQueue, ElasticDBPersister elasticDBPersister) {

        this.pageQueue = pageQueue;
        this.elasticDBPersister = elasticDBPersister;
    }

    @Override
    public void run() {
        try {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Page page = pageQueue.take();
                    elasticDBPersister.persist(page);
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //Trying to free Blocking Queue...
            Page page = null;
            while ((page = pageQueue.poll()) != null) {
                elasticDBPersister.persist(page);
                if (pageQueue.isEmpty())
                    elasticDBPersister.flush();
            }

            elasticDBPersister.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
