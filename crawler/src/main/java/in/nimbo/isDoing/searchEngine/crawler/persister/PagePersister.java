package in.nimbo.isDoing.searchEngine.crawler.persister;

import in.nimbo.isDoing.searchEngine.crawler.controller.Counter;
import in.nimbo.isDoing.searchEngine.crawler.page.Page;

import java.util.concurrent.BlockingQueue;

// Why not Persistor? : https://english.stackexchange.com/questions/206893/persister-or-persistor
public interface PagePersister {
    void stop();

    void start();

    Counter getCounter();

    BlockingQueue<Page> getPageQueue();

    void reload();
}
