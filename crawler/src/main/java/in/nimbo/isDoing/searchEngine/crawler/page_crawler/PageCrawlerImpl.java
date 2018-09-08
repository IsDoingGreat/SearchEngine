package in.nimbo.isDoing.searchEngine.crawler.page_crawler;

import in.nimbo.isDoing.searchEngine.crawler.controller.Counter;
import in.nimbo.isDoing.searchEngine.crawler.controller.PageCrawlerController;
import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PageCrawlerImpl implements PageCrawler {
    private final static Logger logger = LoggerFactory.getLogger(PageCrawlerImpl.class);
    private final static Logger pageInfoLogger = LoggerFactory.getLogger("PageInfo");
    private volatile boolean isStopped = false;
    private PageCrawlerController controller;

    public PageCrawlerImpl(PageCrawlerController controller) {
        this.controller = controller;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted() && !isStopped) {
                String link = controller.getQueue().poll(10, TimeUnit.SECONDS);
                if (link == null)
                    continue;

                URL url;
                String normalizedLink;
                try {
                    normalizedLink = NormalizeURL.normalize(link);
                    url = new URL(normalizedLink);
                } catch (MalformedURLException e) {
                    pageInfoLogger.info("link is not valid {}", link);
                    continue;
                }

                if (controller.getLRU().isRecentlyUsed(url.getHost())) {
                    controller.getURLQueue().push(url.toExternalForm());
                    controller.getCounter().increment(Counter.States.LRU_REJECTED);
                    //logger.trace("link is recently used {}", link);
                    continue;
                }

                if (controller.getDuplicateChecker().checkDuplicateAndSet(url)) {
                    controller.getCounter().increment(Counter.States.DUPLICATE);
                    continue;
                }

                Page page;
                try {
                    controller.getLRU().setUsed(url.getHost());

                    page = controller.getFetcher().fetch(url);
                    page.parse();

                    if (!page.getLang().equals("en")) {
                        //logger.trace("link is not english {}, is {}", link,page.getLang());
                        controller.getCounter().increment(Counter.States.INVALID_LANG);
                        continue;
                    }
                } catch (Exception e) {
                    pageInfoLogger.trace("page fetch exception  : " + link + "\n "+ e.getMessage());
                    controller.getCounter().increment(Counter.States.FETCHER_ERROR);
                    continue;
                }

                Map<String, String> outgoingUrls = page.getOutgoingUrls();
                //logger.trace("{} Urls Found in link {}", outgoingUrls.size(), link);
                for (String outgoingUrl : outgoingUrls.keySet()) {
                    controller.getURLQueue().push(outgoingUrl);
                }

                controller.getPersister().getPageQueue().put(page);

                controller.getCounter().increment(Counter.States.SUCCESSFUL);
            }
        } catch (InterruptedException ignored) {
        } catch (Exception e) {
            logger.error("PageCrawler Stopped With Error {}", e);
            return;
        }
        logger.info("normally Exiting Thread {}", Thread.currentThread().getName());
    }

    @Override
    public void stop() {
        isStopped = true;
    }
}
