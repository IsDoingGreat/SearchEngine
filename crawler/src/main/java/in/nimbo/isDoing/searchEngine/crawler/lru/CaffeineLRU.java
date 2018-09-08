package in.nimbo.isDoing.searchEngine.crawler.lru;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.SharedMetricRegistries;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.isDoing.searchEngine.crawler.page.WebPage;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Stateful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CaffeineLRU implements LRU, Stateful {
    private final static Logger logger = LoggerFactory.getLogger(WebPage.class);
    private final static Object OBJECT = new Object();
    private Cache<String, Object> cache;

    public CaffeineLRU() {
        logger.info("Creating Caffeine LRU...");
        Engine.getOutput().show("Creating Caffeine LRU...");
        int expireSeconds = Integer.parseInt(Engine.getConfigs().get("crawler.lru.caffeine.expireSeconds", "30"));
        int maximumSize = Integer.parseInt(Engine.getConfigs().get("crawler.lru.caffeine.maximumSize", "18000"));

        cache = Caffeine.newBuilder()
                .expireAfterWrite(expireSeconds, TimeUnit.SECONDS)
                .maximumSize(maximumSize)
                .recordStats()
                .build();

        SharedMetricRegistries.getDefault().register("lru.size", (Gauge<Long>) () -> cache.estimatedSize());
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
        Engine.getOutput().show("Stopping Caffeine LRU... ");
        cache.cleanUp();
    }

    @Override
    public Map<String, Object> status() {
        Map<String, Object> map = new HashMap<>();
        map.put("size", cache.estimatedSize());
        map.put("stat",cache.stats().toString());
        return map;
    }
}
