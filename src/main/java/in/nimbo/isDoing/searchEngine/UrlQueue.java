package in.nimbo.isDoing.searchEngine;

import java.util.Iterator;

public interface UrlQueue<U> {
    public void add(U url);

    public Iterator<U> pull(int number);
}