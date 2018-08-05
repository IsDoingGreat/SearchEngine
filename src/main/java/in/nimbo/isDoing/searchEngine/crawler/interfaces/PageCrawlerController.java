package in.nimbo.isDoing.searchEngine.crawler.interfaces;

import java.util.concurrent.BlockingQueue;

public interface PageCrawlerController {
    PageFetcher getFetcher();

    LRU getLRU();

    BlockingQueue<String> getQueue();

    PagePersister getPersister();

    DuplicateChecker getDuplicateChecker();

    URLQueue getURLQueue();

    void addNewAliveThread();

    void oneThreadDied();

    void newSiteCrawled();

    int getAliveThreads();

    int getTotalCrawls();
}
