package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.interfaces.LRU;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class LRULinkHashMapImpl implements LRU {
    private final static Logger logger = LoggerFactory.getLogger(LRULinkHashMapImpl.class);
    private static final String INITIAL_CAPACITY = "1500";
    private static final String LOAD_FACTOR = "0.75";
    private Map<String, Date> map;

    public LRULinkHashMapImpl() {
        int initialCapacity = Integer.parseInt(Engine.getConfigs().get("crawler.lru.initialCapacity", INITIAL_CAPACITY));
        float loadFactor = Float.parseFloat(Engine.getConfigs().get("crawler.lru.initialCapacity", LOAD_FACTOR));
        map = Collections.synchronizedMap(new DateLinkedHashMap(initialCapacity, loadFactor));

        logger.info("PageFetcher Created With Settings:\n" +
                "\tinitialCapacity= " + initialCapacity + "\n" +
                "\tloadFactor= " + loadFactor + "\n");
    }

    @Override
    public boolean isRecentlyUsed(String url) {
        Date date = map.get(url);
        if (date == null) return false;

        if (!((new Date().getTime() - date.getTime()) / 1000 > 30))
            return true;
        else {
            map.remove(url);
            return false;
        }
    }

    @Override
    public void setUsed(String url) {
        map.put(url, new Date());
    }

    @Override
    public void stop() {

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
