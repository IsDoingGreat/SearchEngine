package in.nimbo.isDoing.searchEngine.twitter_reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterStreamReader {

//    private static final Logger logger = LoggerFactory.getLogger(TwitterStreamReader.class);
    private static ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
    private TwitterStreamReader() {

    }
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

        System.out.println("starting");
        TwitterStreamReader.getTwitterStream();
        System.out.println("started");
    }
    public static void getTwitterStream() {
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                System.out.println("notEnglish");
                if (status.getLang().equals("en")) {
                    System.out.println(status.getText());
                }
            }
            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("onDeletionNotice");
            }
            @Override
            public void onTrackLimitationNotice(int i) {
                System.out.println("onScrubGeo");
            }
            @Override
            public void onScrubGeo(long l, long l1) {
                System.out.println("onScrubGeo");
            }
            @Override
            public void onStallWarning(StallWarning stallWarning) {
                System.out.println("onStallWarning");
            }
            @Override
            public void onException(Exception e) {
                System.out.println("Exception");
                e.printStackTrace();
            }
        };
        TwitterStreamFactory twitterStreamFactory = new TwitterStreamFactory(configurationBuilder.build());
        TwitterStream twitterStream = twitterStreamFactory.getInstance();
        twitterStream.addListener(listener);
        twitterStream.sample();
    }
}
