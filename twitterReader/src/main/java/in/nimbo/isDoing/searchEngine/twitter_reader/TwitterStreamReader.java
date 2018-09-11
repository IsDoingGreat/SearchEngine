package in.nimbo.isDoing.searchEngine.twitter_reader;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.SystemConfigs;
import in.nimbo.isDoing.searchEngine.kafka.KafkaProducerController;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.concurrent.ExecutionException;

public class TwitterStreamReader {

    private static final Logger logger = LoggerFactory.getLogger(TwitterStreamReader.class);

    private ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

    private ElasticTwitterPersister elasticTwitterPersister;
    private KafkaProducerController kafkaProducer;

    private TwitterStreamFactory twitterStreamFactory;
    private TwitterStream twitterStream;


    private TwitterStreamReader() {
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(Engine.getConfigs().get("twitter.api.key"))
                .setOAuthConsumerSecret(Engine.getConfigs().get("twitter.api.secret.key"))
                .setOAuthAccessToken(Engine.getConfigs().get("twitter.api.access.token"))
                .setOAuthAccessTokenSecret(Engine.getConfigs().get("twitter.api.access.token.secret"));

        kafkaProducer = new KafkaProducerController(
                Engine.getConfigs().get("kafka.brokers"),
                Engine.getConfigs().get("twitterReader.kafka.producerClientId"),
                Engine.getConfigs().get("twitterReader.kafka.topicName"));

        elasticTwitterPersister = new ElasticTwitterPersister();

        twitterStreamFactory = new TwitterStreamFactory(configurationBuilder.build());
        twitterStream = twitterStreamFactory.getInstance();
    }

    public static void main(String[] args) throws Exception {
        Engine.start(new ConsoleOutput(), new SystemConfigs("twitterreader"));
        System.out.println("starting");
        TwitterStreamReader twitterStreamReader = new TwitterStreamReader();
        twitterStreamReader.getTwitterStream();
        System.out.println("started");

    }

    public void getTwitterStream() {
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                if (status.getLang().equals("en")) {
                    try {
                        System.out.println(status.getText());
                        elasticTwitterPersister.persist(status);
                        kafkaProducer.produce(status.getText());
                    } catch (ExecutionException e) {
                        logger.error("kafka producer execution exception.", e);
                    } catch (InterruptedException e) {
                        logger.error("kafka producer interrupted exception.", e);
                    } catch (ElasticTwitterPersister.ElasticTwitterPersisterException e) {
                        logger.error("elasticSearch twitter persister was intrupted", e);
                    }
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                logger.info("onDeletionNotice");
            }

            @Override
            public void onTrackLimitationNotice(int i) {
                logger.info("onScrubGeo");
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
                logger.error("Exception : ", e);
            }
        };
        twitterStream.addListener(listener);
        twitterStream.sample();
    }
}
