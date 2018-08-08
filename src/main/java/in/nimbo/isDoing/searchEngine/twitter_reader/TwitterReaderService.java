package in.nimbo.isDoing.searchEngine.twitter_reader;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TwitterReaderService implements Service {
    private final static Logger logger = LoggerFactory.getLogger(TwitterReaderService.class);
    TwitterReader twitterReader = new TwitterReader();

    public TwitterReaderService() throws IOException {
        logger.info("Creating TwitterReader Service...");
        //coding ...
        logger.info("TwitterReader Service Created");
    }

    @Override
    public void start() {
        logger.info("Starting TwitterReader Service...");
        Engine.getOutput().show("Starting TwitterReader Service...");
        try {
            twitterReader.getTweets();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        twitterReader.stopGetTweets();
        logger.info("TwitterReader Service Stopped");
        Engine.getOutput().show("TwitterReader Service Stopped");
    }

    @Override
    public Status status() {
        return null;
    }

    @Override
    public String getName() {
        return "twitterReader";
    }
}
