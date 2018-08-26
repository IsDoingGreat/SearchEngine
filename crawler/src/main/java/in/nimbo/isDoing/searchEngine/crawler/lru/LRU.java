package in.nimbo.isDoing.searchEngine.crawler.lru;

public interface LRU {
    boolean isRecentlyUsed(String url);

    void setUsed(String url);

    void stop();
}
