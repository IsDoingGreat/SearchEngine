package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.interfaces.Page;
import in.nimbo.isDoing.searchEngine.crawler.interfaces.PageCrawler;
import in.nimbo.isDoing.searchEngine.crawler.interfaces.PageCrawlerController;
import in.nimbo.isDoing.searchEngine.crawler.interfaces.PageFetcher;
import org.jsoup.UncheckedIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

public class PageCrawlerImpl implements PageCrawler {
    private final static Logger logger = LoggerFactory.getLogger(PageCrawlerImpl.class);
    private PageCrawlerController controller;

    public PageCrawlerImpl(PageCrawlerController controller) {
        this.controller = controller;
    }

    @Override
    public void run() {
        try {
            controller.addNewAliveThread();
            while (!Thread.interrupted()) {
                String link = controller.getQueue().take();
                URL url = new URL(link);
                if (controller.getLRU().isRecentlyUsed(url.getHost()) ||
                        controller.getDuplicateChecker().isDuplicate(link)
                ) {
                    continue;
                }

                controller.getLRU().setUsed(url.getHost());

                try {
                    url = new URL(link);


                    Page page = controller.getFetcher().fetch(url);
                    page.parse();

                    for (String outgoingUrl : page.getOutgoingUrls()) {
                        controller.getURLQueue().push(outgoingUrl);
                    }

                    controller.getPersister().insert(page);
                    controller.newSiteCrawled();
                } catch (PageFetcher.BadStatusCodeException | IOException | UncheckedIOException ignored) {
                }

            }
        } catch (Exception e) {
            logger.error("PageCrawler Stopped With Error {}", e);
            controller.oneThreadDied();
        }
    }
}
