package in.nimbo.isDoing.searchEngine.crawler.controller;

import in.nimbo.isDoing.searchEngine.crawler.duplicate_checker.DuplicateChecker;
import in.nimbo.isDoing.searchEngine.crawler.fetcher.PageFetcher;
import in.nimbo.isDoing.searchEngine.crawler.lru.LRU;
import in.nimbo.isDoing.searchEngine.crawler.persister.PagePersister;
import in.nimbo.isDoing.searchEngine.crawler.urlqueue.URLQueue;

import java.util.concurrent.BlockingQueue;

public interface PageCrawlerController {
    PageFetcher getFetcher();

    LRU getLRU();

    BlockingQueue<String> getQueue();

    PagePersister getPersister();

    DuplicateChecker getDuplicateChecker();

    URLQueue getURLQueue();


    Counter getCounter();

    void stop();

    void reload();
}
