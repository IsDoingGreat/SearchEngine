package in.nimbo.isDoing.searchEngine.crawler.presister;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;

// Why not Persistor? : https://english.stackexchange.com/questions/206893/persister-or-persistor
public interface PagePersister {
    void insert(Page page) throws Exception;

    void stop();
}
