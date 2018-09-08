package in.nimbo.isDoing.searchEngine.crawler.scheduler;

public interface CrawlScheduler extends Runnable {
    void stop();

    void start();

    void reload();
}
