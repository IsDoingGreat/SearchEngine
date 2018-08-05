package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.interfaces.DuplicateChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockingDuplicateChecker implements DuplicateChecker {
    private final static Logger logger = LoggerFactory.getLogger(MockingDuplicateChecker.class);

    @Override
    public boolean isDuplicate(String page) throws Exception {
        return false;
    }

    @Override
    public void stop() {

    }
}
