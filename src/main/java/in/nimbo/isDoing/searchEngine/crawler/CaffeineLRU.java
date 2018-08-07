package in.nimbo.isDoing.searchEngine.crawler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.isDoing.searchEngine.crawler.interfaces.LRU;

import java.util.concurrent.TimeUnit;

public class CaffeineLRU implements LRU {
    private final static Object OBJECT = new Object();
    private Cache<String, Object> cache;

    public CaffeineLRU() {
        cache = Caffeine.newBuilder()
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .maximumSize(18000)
                .build();
    }

    @Override
    public boolean isRecentlyUsed(String url) {
        Object value = cache.getIfPresent(url);
        return value != null;
    }

    @Override
    public void setUsed(String url) {
        cache.put(url, OBJECT);
    }

    @Override
    public void stop() {

    }
}
