package in.nimbo.isDoing.searchEngine.interfaces;

import java.util.Iterator;
import java.util.List;

public interface UrlQueue<U> {
    public void push(U url);

    public List<U> pop(int number);
}