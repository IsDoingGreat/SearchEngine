package in.nimbo.isDoing.searchEngine.crawler.controller;

import in.nimbo.isDoing.searchEngine.crawler.duplicate_checker.DuplicateChecker;
import in.nimbo.isDoing.searchEngine.crawler.duplicate_checker.MockingDuplicateChecker;
import in.nimbo.isDoing.searchEngine.crawler.fetcher.PageFetcher;
import in.nimbo.isDoing.searchEngine.crawler.fetcher.PageFetcherImpl;
import in.nimbo.isDoing.searchEngine.crawler.lru.CaffeineLRU;
import in.nimbo.isDoing.searchEngine.crawler.lru.LRU;
import in.nimbo.isDoing.searchEngine.crawler.presister.PagePersister;
import in.nimbo.isDoing.searchEngine.crawler.presister.SeedPersistor;
import in.nimbo.isDoing.searchEngine.crawler.urlqueue.URLQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class PageCrawlerControllerImpl implements PageCrawlerController {
    private final static Logger logger = LoggerFactory.getLogger(PageCrawlerControllerImpl.class);
    private Counter counter;
    private PageFetcher fetcher;
    private LRU lru;
    private BlockingQueue<String> queue;
    private PagePersister persister;
    private DuplicateChecker duplicateChecker;
    private URLQueue urlQueue;

    public PageCrawlerControllerImpl(BlockingQueue<String> queue, URLQueue urlQueue, PageFetcher fetcher, LRU lru, PagePersister persister, DuplicateChecker duplicateChecker, Counter counter) {
        this.fetcher = fetcher;
        this.urlQueue = urlQueue;
        this.lru = lru;
        this.queue = queue;
        this.persister = persister;
        this.duplicateChecker = duplicateChecker;
        this.counter = counter;
    }

    public PageCrawlerControllerImpl(BlockingQueue<String> queue, URLQueue urlQueue) throws IOException {
        this(
                queue,
                urlQueue,
                new PageFetcherImpl(),
                new CaffeineLRU(),
                new SeedPersistor(),
                new MockingDuplicateChecker(),
                new Counter()
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
}
