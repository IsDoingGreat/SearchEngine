package in.nimbo.isDoing.searchEngine.crawler.interfaces;

// Why not Persistor? : https://english.stackexchange.com/questions/206893/persister-or-persistor
public interface PagePersister {
    void insert(Page page) throws Exception;

    void stop();
}
