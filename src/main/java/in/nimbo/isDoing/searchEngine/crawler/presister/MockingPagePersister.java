package in.nimbo.isDoing.searchEngine.crawler.presister;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockingPagePersister implements PagePersister {
    private static final Logger logger = LoggerFactory.getLogger(MockingPagePersister.class);

    @Override
    public void insert(Page page) {
//        logger.info("Page Persisted {}",page);
    }

    @Override
    public void stop() {

    }
}
