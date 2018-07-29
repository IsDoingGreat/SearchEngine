package in.nimbo.isDoing.searchEngine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ArrayListUrlQueue implements UrlQueue<String> {
    LinkedList<String> linkedList = new LinkedList<>();

    @Override
    public void push(String url) {
        linkedList.offerLast(url);
    }

    @Override
    public Iterator<String> pop(int number) {
        List<String> list = new ArrayList<>(number);
        while (number > 0 && linkedList.size() > 0) {
            list.add(linkedList.pop());
            number--;
        }
        return list.iterator();
    }
}
