package in.nimbo.isDoing.searchEngine.twitter_reader;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Twitter4jReaderService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(Twitter4jReaderService.class);
    private Twitter4jReader twitterReader = new Twitter4jReader();

    @Override
    public void start() {
        logger.info("Starting twitter reader service...");
        Engine.getOutput().show("Starting twitter reader service...");
        twitterReader.getTwitterStream();
        logger.info("Twitter reader service started.");
        Engine.getOutput().show("Twitter reader service started.");
    }

    @Override
    public void stop() {
        twitterReader.stopGetTwitterStream();
    }

    @Override
    public Map<String, Object> status() {
        return null;
    }

    @Override
    public String getName() {
        return "twitterReader";
    }
}
