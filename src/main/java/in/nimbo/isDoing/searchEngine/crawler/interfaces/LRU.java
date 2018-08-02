package in.nimbo.isDoing.searchEngine.crawler.interfaces;

public interface LRU {
    boolean isRecentlyUsed(String url);

    void setUsed(String url);
}
