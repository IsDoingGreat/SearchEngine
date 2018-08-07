package in.nimbo.isDoing.searchEngine.crawler.duplicate_checker;

public interface DuplicateChecker {
    boolean isDuplicate(String page) throws Exception;

    void stop();
}
