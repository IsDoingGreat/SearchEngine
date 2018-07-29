package in.nimbo.isDoing.searchEngine;

import java.util.Iterator;

public interface UrlQueue<U> {
    public void push(U url);

    public Iterator<U> pop(int number);
}