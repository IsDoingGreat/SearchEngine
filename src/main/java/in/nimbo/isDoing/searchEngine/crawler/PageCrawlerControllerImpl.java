package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.interfaces.*;

public class PageCrawlerControllerImpl implements PageCrawlerController {
    private PageFetcher fetcher;
    private LRU lru;
    private URLQueue queue;
    private PagePersister persister;
    private DuplicateChecker duplicateChecker;

    public PageCrawlerControllerImpl(PageFetcher fetcher, LRU lru, URLQueue queue, PagePersister persister, DuplicateChecker duplicateChecker) {
        this.fetcher = fetcher;
        this.lru = lru;
        this.queue = queue;
        this.persister = persister;
        this.duplicateChecker = duplicateChecker;
    }

    public PageCrawlerControllerImpl() {
        this(,new LRULinkHashMapImpl(), );
    }

    public PageFetcher getFetcher() {
        return fetcher;
    }

    public LRU getLRU() {
        return lru;
    }

    public URLQueue getQueue() {
        return queue;
    }

    public PagePersister getPersister() {
        return persister;
    }

    public DuplicateChecker getDuplicateChecker() {
        return duplicateChecker;
    }
}
