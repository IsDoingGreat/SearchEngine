package in.nimbo.isDoing.searchEngine.crawler.urlqueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ArrayListURLQueueImpl implements URLQueue {
    private final static Logger logger = LoggerFactory.getLogger(ArrayListURLQueueImpl.class);
    private Queue<String> linkedList = new ConcurrentLinkedQueue<>();

    @Override
    public void push(String url) {
        linkedList.add(url);
    }

    @Override
    public List<String> pop(int number) {
        List<String> list = new ArrayList<>(number);
        while (number > 0 && linkedList.size() > 0) {
            list.add(linkedList.poll());
            number--;
        }
        return list;
    }

    @Override
    public void stop() {

    }
}
