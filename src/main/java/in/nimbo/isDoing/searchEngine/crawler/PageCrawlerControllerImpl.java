package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.interfaces.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public PageCrawlerControllerImpl(BlockingQueue<String> queue, URLQueue urlQueue) {
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
}
