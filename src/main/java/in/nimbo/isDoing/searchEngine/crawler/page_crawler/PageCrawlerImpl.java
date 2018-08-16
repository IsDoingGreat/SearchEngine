package in.nimbo.isDoing.searchEngine.crawler.page_crawler;

import in.nimbo.isDoing.searchEngine.crawler.controller.Counter;
import in.nimbo.isDoing.searchEngine.crawler.controller.PageCrawlerController;
import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Set;

public class PageCrawlerImpl implements PageCrawler {
    private final static Logger logger = LoggerFactory.getLogger(PageCrawlerImpl.class);
    private PageCrawlerController controller;

    public PageCrawlerImpl(PageCrawlerController controller) {
        this.controller = controller;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                String link = controller.getQueue().take();
                URL url;

                try {
                    url = new URL(link);
                } catch (MalformedURLException e) {
                    logger.trace("link is not valid {}", link);
                    controller.getCounter().increment(Counter.States.INVALID_LINK);
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
                    //logger.trace("page fetch exception  : " + link, e);
                    continue;
                }

                Set<String> outgoingUrls = page.getOutgoingUrls();
                //logger.trace("{} Urls Found in link {}", outgoingUrls.size(), link);
                for (String outgoingUrl : outgoingUrls) {
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
}
