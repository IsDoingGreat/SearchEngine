package in.nimbo.isDoing.searchEngine.twitter_reader;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.SystemConfigs;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import in.nimbo.isDoing.searchEngine.kafka.KafkaProducerController;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Twitter4jReader implements Service {
    private static final Logger logger = LoggerFactory.getLogger(Twitter4jReader.class);
    private ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
//    private ElasticTwitterPersister elasticTwitterPersister = new ElasticTwitterPersister();
    private TwitterStream twitterStream;
    private KafkaProducerController kafkaProducer = new KafkaProducerController(
            Engine.getConfigs().get("kafka.brokers"),
            Engine.getConfigs().get("twitterReader.kafka.producerClientId"),
            Engine.getConfigs().get("twitterReader.kafka.topicName"));

    private Twitter4jReader() {
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(Engine.getConfigs().get("twitter.api.key"))
                .setOAuthConsumerSecret(Engine.getConfigs().get("twitter.api.secret.key"))
                .setOAuthAccessToken(Engine.getConfigs().get("twitter.api.access.token"))
                .setOAuthAccessTokenSecret(Engine.getConfigs().get("twitter.api.access.token.secret"));
    }

    public static void main(String[] args) throws Exception {
        Engine.start(new ConsoleOutput(),new SystemConfigs("twitterreader"));
        Engine.getInstance().startService(new Twitter4jReader());
    }

    private void getTwitterStream() {
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                logger.info("one new tweet got.");
//                try {
                    System.out.println(status.getText());
                    //elasticTwitterPersister.persist(status);
                    //kafkaProducer.produce(status.getText());
//                } catch (ExecutionException e) {
//                    logger.error("kafka producer execution exception.", e);
//                } catch (InterruptedException e) {
//                    logger.error("kafka producer interrupted exception.", e);
//                } catch (ElasticTwitterPersister.ElasticTwitterPersisterException e) {
//                    logger.error("elasticSearch twitter persister was intrupted", e);
//                }

            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                logger.info("onDeletionNotice");

            }

            @Override
            public void onTrackLimitationNotice(int i) {
                logger.info("onTrackLimitationNotice");
            }

            @Override
            public void onScrubGeo(long l, long l1) {
                logger.info("onScrubGeo");
            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {
                logger.info("onStallWarning");
            }

            @Override
            public void onException(Exception e) {
                logger.error("twitter stream exception.", e);
            }
        };

        TwitterStreamFactory twitterStreamFactory = new TwitterStreamFactory(configurationBuilder.build());
        twitterStream = twitterStreamFactory.getInstance();
        twitterStream.addListener(listener);
        twitterStream.sample();
        //twitterStream.filter(new FilterQuery().language("en"));

    }

    @Override
    public void start() {
        logger.info("Starting twitter reader service...");
        Engine.getOutput().show("Starting twitter reader service...");
        this.getTwitterStream();
        logger.info("Twitter reader service started.");
        Engine.getOutput().show("Twitter reader service started.");
    }

    @Override
    public Map<String, Object> status() {
        return null;
    }

    @Override
    public void stop() {
//        try {
//            elasticTwitterPersister.flush();
//        } catch (IOException e) {
//            logger.error("Elastic twitter persister interrupted.", e);
//        }

        if (kafkaProducer != null) {
            kafkaProducer.stop();
            logger.info("Kafka producer stopped.");
        }
        else {
            logger.info("Kafka stop refused because kafka is not running.");
        }

        if (twitterStream != null) {
            twitterStream.shutdown();
            logger.info("Twitter stream stoped.");
        }
        else {
            logger.info("Twitter stop refused because stream is not running.");
        }

    }

    @Override
    public String getName() {
        return "twitter4jReader";
    }
}
