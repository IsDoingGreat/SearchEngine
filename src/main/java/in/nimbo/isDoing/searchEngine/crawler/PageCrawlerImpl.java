package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.interfaces.Page;
import in.nimbo.isDoing.searchEngine.crawler.interfaces.PageCrawler;
import in.nimbo.isDoing.searchEngine.crawler.interfaces.PageCrawlerController;
import in.nimbo.isDoing.searchEngine.crawler.interfaces.PageFetcher;
import org.jsoup.UncheckedIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
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
                URL url;

                try {
                    url = new URL(link);
                } catch (MalformedURLException e) {
                    continue;
                }

                if (controller.getDuplicateChecker().isDuplicate(link)) {
                    controller.getURLQueue().push(url.toExternalForm());
                }

                if (controller.getLRU().isRecentlyUsed(url.getHost())) {
                    continue;
                }

                controller.getLRU().setUsed(url.getHost());

                Page page;
                try {
                    page = controller.getFetcher().fetch(url);
                    page.parse();
                } catch (Exception e) {
                    continue;
                }

                if (!page.getLang().startsWith("en")) {
                    continue;
                }

                for (String outgoingUrl : page.getOutgoingUrls()) {
                    controller.getURLQueue().push(outgoingUrl);
                }

                controller.getPersister().insert(page);
                controller.newSiteCrawled();


            }
        } catch (Exception e) {
            logger.error("PageCrawler Stopped With Error {}", e);
            controller.oneThreadDied();
        }
    }
}
