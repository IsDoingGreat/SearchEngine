package in.nimbo.isDoing.searchEngine.crawler.page_crawler;

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
            controller.addNewAliveThread();
            while (!Thread.interrupted()) {
                String link = controller.getQueue().take();
                URL url;

                try {
                    url = new URL(link);
                } catch (MalformedURLException e) {
                    logger.trace("link is not valid {}", link);
                    continue;
                }

                if (controller.getDuplicateChecker().isDuplicate(link)) {
                    continue;
                }

                if (controller.getLRU().isRecentlyUsed(url.getHost())) {
                    controller.getURLQueue().push(url.toExternalForm());
//                    logger.trace("link is recently used {}", link);
                    continue;
                }

                Page page;
                try {
                    controller.getLRU().setUsed(url.getHost());
                    
                    page = controller.getFetcher().fetch(url);
                    page.parse();

                    if (!page.getLang().equals("en")) {
//                        logger.trace("link is not english {}, is {}", link,page.getLang());
                        continue;
                    }
                } catch (Exception e) {
//                    logger.trace("page fetch exception  : " + link, e);
                    continue;
                }

                Set<String> outgoingUrls = page.getOutgoingUrls();
//                logger.trace("{} Urls Found in link {}", outgoingUrls.size(), link);
                for (String outgoingUrl : outgoingUrls) {
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
