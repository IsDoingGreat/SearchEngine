package in.nimbo.isDoing.searchEngine;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LRU {
    private Map<String, Date> map = new ConcurrentHashMap<>();

    public void add(String url) {
        map.put(url, new Date());
    }

    public boolean get(String url) {
        Date date = map.get(url);
        if (date == null || (new Date().getTime() - date.getTime()) / 1000 > 30)
            return true;
        else
            return false;

    }
}
