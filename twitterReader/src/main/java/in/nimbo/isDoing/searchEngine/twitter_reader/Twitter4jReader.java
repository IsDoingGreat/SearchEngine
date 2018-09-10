package in.nimbo.isDoing.searchEngine.twitter_reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class Twitter4jReader {
    private static final Logger logger = LoggerFactory.getLogger(Twitter4jReader.class);
    private static ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

    private Twitter4jReader() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 4 ){
            System.out.println("Invalid Input");
            return;
        }

        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(args[0])
                .setOAuthConsumerSecret(args[1])
                .setOAuthAccessToken(args[2])
                .setOAuthAccessTokenSecret(args[3]);
        Twitter4jReader.getTwitterStream();
    }


    public static void getTwitterStream() {
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                if (status.getLang().equals("en")) {
                    System.out.println(status.getText());
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {
                e.printStackTrace();
            }
        };

        TwitterStreamFactory twitterStreamFactory = new TwitterStreamFactory(configurationBuilder.build());
        TwitterStream twitterStream = twitterStreamFactory.getInstance();
        twitterStream.addListener(listener);
        twitterStream.sample();
    }
}
