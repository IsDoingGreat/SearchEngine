package in.nimbo.isDoing.searchEngine.crawler.duplicate_checker;

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
