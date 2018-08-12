package in.nimbo.isDoing.searchEngine.crawler.duplicate_checker;

import java.net.URL;

public interface DuplicateChecker {
    boolean checkDuplicateAndSet(URL url) throws Exception;
    void stop();
}