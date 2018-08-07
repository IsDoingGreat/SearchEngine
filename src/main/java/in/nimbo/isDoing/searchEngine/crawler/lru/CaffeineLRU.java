package in.nimbo.isDoing.searchEngine.crawler.lru;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.isDoing.searchEngine.engine.Engine;

import java.util.concurrent.TimeUnit;

public class CaffeineLRU implements LRU {
    private final static Object OBJECT = new Object();
    private Cache<String, Object> cache;

    public CaffeineLRU() {
        int expireSeconds = Integer.parseInt(Engine.getConfigs().get("crawler.lru.caffeine.expireSeconds", "30"));
        int maximumSize = Integer.parseInt(Engine.getConfigs().get("crawler.lru.caffeine.maximumSize", "18000"));

        cache = Caffeine.newBuilder()
                .expireAfterWrite(expireSeconds, TimeUnit.SECONDS)
                .maximumSize(maximumSize)
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
        cache.cleanUp();
    }
}