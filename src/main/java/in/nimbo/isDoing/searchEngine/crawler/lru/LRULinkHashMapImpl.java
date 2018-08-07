package in.nimbo.isDoing.searchEngine.crawler.lru;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LRULinkHashMapImpl implements LRU {
    private final static Logger logger = LoggerFactory.getLogger(LRULinkHashMapImpl.class);
    private static final String INITIAL_CAPACITY = "1500";
    private Map<String, Date> map;

    public LRULinkHashMapImpl() {
        int initialCapacity = Integer.parseInt(Engine.getConfigs().get("crawler.lru.initialCapacity", INITIAL_CAPACITY));
        map = new ConcurrentHashMap<>();

        logger.info("LRU Created With Settings:\n" +
                "\tinitialCapacity= " + initialCapacity + "\n");
    }

    @Override
    public boolean isRecentlyUsed(String url) {
        Date date = map.get(url);
        if (date == null) return false;

        if (!((new Date().getTime() - date.getTime()) / 1000 > 30))
            return true;
        else {
            return false;
        }
    }

    @Override
    public void setUsed(String url) {
        if (map.get(url) == null)
            logger.info(url);
        map.put(url, new Date());
    }

    @Override
    public void stop() {

    }

}
