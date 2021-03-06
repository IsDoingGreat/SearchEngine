package in.nimbo.isDoing.searchEngine.crawler.controller;

import in.nimbo.isDoing.searchEngine.crawler.duplicate_checker.CaffeinePartlyDuplicateChecker;
import in.nimbo.isDoing.searchEngine.crawler.duplicate_checker.DuplicateChecker;
import in.nimbo.isDoing.searchEngine.crawler.fetcher.PageFetcher;
import in.nimbo.isDoing.searchEngine.crawler.fetcher.PageFetcherImpl;
import in.nimbo.isDoing.searchEngine.crawler.lru.CaffeineLRU;
import in.nimbo.isDoing.searchEngine.crawler.lru.LRU;
import in.nimbo.isDoing.searchEngine.crawler.persister.PagePersister;
import in.nimbo.isDoing.searchEngine.crawler.persister.PagePersisterImpl;
import in.nimbo.isDoing.searchEngine.crawler.urlqueue.URLQueue;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Stateful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class PageCrawlerControllerImpl implements PageCrawlerController, Stateful {
    private final static Logger logger = LoggerFactory.getLogger(PageCrawlerControllerImpl.class);
    private Counter counter;
    private PageFetcher fetcher;
    private LRU lru;
    private BlockingQueue<String> queue;
    private PagePersister persister;
    private DuplicateChecker duplicateChecker;
    private URLQueue urlQueue;

    public PageCrawlerControllerImpl(BlockingQueue<String> queue, URLQueue urlQueue,
                                     PageFetcher fetcher, LRU lru,
                                     PagePersister persister, DuplicateChecker duplicateChecker, Counter counter) {
        this.fetcher = fetcher;
        this.urlQueue = urlQueue;
        this.lru = lru;
        this.queue = queue;
        this.counter = counter;
        this.persister = persister;
        this.duplicateChecker = duplicateChecker;
        logger.info("PageCrawlerController Created");
    }

    public PageCrawlerControllerImpl(BlockingQueue<String> queue, URLQueue urlQueue) throws IOException {
        this(
                queue,
                urlQueue,
                new Counter()
        );
        logger.info("PageCrawlerController Created");
    }

    public PageCrawlerControllerImpl(BlockingQueue<String> queue, URLQueue urlQueue, Counter counter) throws IOException {
        this(
                queue,
                urlQueue,
                new PageFetcherImpl(),
                new CaffeineLRU(),
                new PagePersisterImpl(counter),
                new CaffeinePartlyDuplicateChecker(),
                counter
        );
        logger.info("PageCrawlerController Created");
    }

    public PageFetcher getFetcher() {
        return fetcher;
    }

    public LRU getLRU() {
        return lru;
    }

    public BlockingQueue<String> getQueue() {
        return queue;
    }

    public PagePersister getPersister() {
        return persister;
    }

    public DuplicateChecker getDuplicateChecker() {
        return duplicateChecker;
    }

    public URLQueue getURLQueue() {
        return urlQueue;
    }

    @Override
    public Counter getCounter() {
        return counter;
    }


    @Override
    public void stop() {
        if (lru != null)
            lru.stop();

        if (fetcher != null)
            fetcher.stop();

        if (persister != null)
            persister.stop();

        if (duplicateChecker != null)
            duplicateChecker.stop();
    }

    @Override
    public void reload() {
        persister.reload();
    }

    @Override
    public Map<String, Object> status() {
        Map<String, Object> map = new HashMap<>();
        map.put("counter", counter.status());

        if (lru instanceof Stateful) {
            map.put("lru", ((Stateful) lru).status());
        }

        if (persister instanceof Stateful) {
            map.put("persister", ((Stateful) persister).status());
        }

        if (duplicateChecker instanceof Stateful) {
            map.put("duplicateChecker", ((Stateful) duplicateChecker).status());
        }

        if (urlQueue instanceof Stateful) {
            map.put("urlQueue", ((Stateful) urlQueue).status());
        }

        return map;
    }
}
