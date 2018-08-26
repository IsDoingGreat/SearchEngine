package in.nimbo.isDoing.searchEngine.crawler.urlqueue;

import java.util.List;

public interface URLQueue {
    void push(String url);

    List<String> pop(int number);

    void stop();
}