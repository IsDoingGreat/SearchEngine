package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.interfaces.URLQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

public class ArrayListURLQueueImpl implements URLQueue {
    private final static Logger logger = LoggerFactory.getLogger(ArrayListURLQueueImpl.class);
    private List<String> linkedList = new Vector<>();

    @Override
    public void push(String url) {
        linkedList.add(url);
    }

    @Override
    public List<String> pop(int number) {
        List<String> list = new ArrayList<>(number);
        while (number > 0 && linkedList.size() > 0) {
            list.add(linkedList.get(0));
            linkedList.remove(0);
            number--;
        }
        return Collections.unmodifiableList(list);
    }
}
