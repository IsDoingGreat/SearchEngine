package in.nimbo.isDoing.searchEngine.twitter_reader;

import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.kafka.KafkaProducerController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.concurrent.ExecutionException;

public class Twitter4jReader {
    private static final Logger logger = LoggerFactory.getLogger(Twitter4jReader.class);
    private static ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
    private static ElasticTwitterPersister elasticTwitterPersister = new ElasticTwitterPersister();
    private static KafkaProducerController kafkaProducer = new KafkaProducerController(
            Engine.getConfigs().get("kafka.brokers"),
            Engine.getConfigs().get("twitterReader.kafka.producerClientId"),
            Engine.getConfigs().get("twitterReader.kafka.topicName"));


    public static void main(String[] args) {
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(Engine.getConfigs().get("twitter.api.key"))
                .setOAuthConsumerSecret(Engine.getConfigs().get("twitter.api.secret.key"))
                .setOAuthAccessToken(Engine.getConfigs().get("twitter.api.access.token"))
                .setOAuthAccessTokenSecret(Engine.getConfigs().get("twitter.api.access.token.secret"));

        Twitter4jReader.getTwitterStream();
    }


    private static void getTwitterStream() {
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                try {
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
                logger.error("twitter stream exception.", e);
            }
        };

        TwitterStreamFactory twitterStreamFactory = new TwitterStreamFactory(configurationBuilder.build());
        TwitterStream twitterStream = twitterStreamFactory.getInstance();
        twitterStream.addListener(listener);
        twitterStream.filter(new FilterQuery().language("en"));
    }
}
