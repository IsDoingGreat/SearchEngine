package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.interfaces.LRU;
import in.nimbo.isDoing.searchEngine.engine.Engine;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class LRULinkHashMapImpl implements LRU {
    private final String INITIAL_CAPACITY = "1500";
    private final String LOAD_FACTOR = "0.75";
    private DateLinkedHashMap map;

    public LRULinkHashMapImpl() {
        int initialCapacity = Integer.parseInt(Engine.getConfigs().get("crawler.lru.initialCapacity", INITIAL_CAPACITY));
        float loadFactor = Float.parseFloat(Engine.getConfigs().get("crawler.lru.initialCapacity", LOAD_FACTOR));
        map = new DateLinkedHashMap(initialCapacity, loadFactor);
    }

    @Override
    public boolean isRecentlyUsed(String url) {
        return map.containsKey(url);
    }

    @Override
    public void setUsed(String url) {
        map.put(url, new Date());
    }


    private static class DateLinkedHashMap extends LinkedHashMap<String, Date> {
        DateLinkedHashMap(int initialCapacity, float loadFactor) {
            super(initialCapacity, loadFactor, true);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Date> eldest) {
            return (new Date().getTime() - eldest.getValue().getTime()) / 1000 > 30;
        }
    }

}
