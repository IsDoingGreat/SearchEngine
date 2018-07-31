package in.nimbo.isDoing.searchEngine;

import in.nimbo.isDoing.searchEngine.interfaces.UrlQueue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ArrayListUrlQueue implements UrlQueue<String> {
    List<String> linkedList = new CopyOnWriteArrayList<>();

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
