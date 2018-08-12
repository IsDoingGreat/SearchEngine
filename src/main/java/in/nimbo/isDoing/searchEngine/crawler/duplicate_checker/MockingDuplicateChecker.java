package in.nimbo.isDoing.searchEngine.crawler.duplicate_checker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

public class MockingDuplicateChecker implements DuplicateChecker {
    private final static Logger logger = LoggerFactory.getLogger(MockingDuplicateChecker.class);


    @Override
    public boolean checkDuplicateAndSet(URL url) throws Exception {
        return false;
    }

    @Override
    public void stop() {

    }
}