package in.nimbo.isDoing.searchEngine.crawler.controller;

import in.nimbo.isDoing.searchEngine.crawler.duplicate_checker.DuplicateChecker;
import in.nimbo.isDoing.searchEngine.crawler.duplicate_checker.MockingDuplicateChecker;
import in.nimbo.isDoing.searchEngine.crawler.fetcher.PageFetcher;
import in.nimbo.isDoing.searchEngine.crawler.fetcher.PageFetcherImpl;
import in.nimbo.isDoing.searchEngine.crawler.lru.LRU;
import in.nimbo.isDoing.searchEngine.crawler.lru.LRULinkHashMapImpl;
import in.nimbo.isDoing.searchEngine.crawler.presister.MockingPagePersister;
import in.nimbo.isDoing.searchEngine.crawler.presister.PagePersister;
import in.nimbo.isDoing.searchEngine.crawler.urlqueue.URLQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class PageCrawlerControllerImpl implements PageCrawlerController {
    private final static Logger logger = LoggerFactory.getLogger(PageCrawlerControllerImpl.class);
    private PageFetcher fetcher;
    private LRU lru;
    private BlockingQueue<String> queue;
    private PagePersister persister;
    private DuplicateChecker duplicateChecker;
    private URLQueue urlQueue;
    private AtomicInteger totalCrawls = new AtomicInteger(0);
    private AtomicInteger aliveThreads = new AtomicInteger(0);

    public PageCrawlerControllerImpl(BlockingQueue<String> queue, URLQueue urlQueue, PageFetcher fetcher, LRU lru, PagePersister persister, DuplicateChecker duplicateChecker) {
        this.fetcher = fetcher;
        this.urlQueue = urlQueue;
        this.lru = lru;
        this.queue = queue;
        this.persister = persister;
        this.duplicateChecker = duplicateChecker;
    }

    public PageCrawlerControllerImpl(BlockingQueue<String> queue, URLQueue urlQueue) throws IOException {
        this(
                queue,
                urlQueue,
                new PageFetcherImpl(),
                new LRULinkHashMapImpl(),
                new MockingPagePersister(),
                new MockingDuplicateChecker()
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
    public void addNewAliveThread() {
        aliveThreads.incrementAndGet();
    }

    @Override
    public void oneThreadDied() {
        aliveThreads.decrementAndGet();
    }

    @Override
    public void newSiteCrawled() {
        totalCrawls.incrementAndGet();
    }

    @Override
    public int getAliveThreads() {
        return aliveThreads.get();
    }

    @Override
    public int getTotalCrawls() {
        return totalCrawls.get();
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

        if (urlQueue != null)
            urlQueue.stop();
    }
}
