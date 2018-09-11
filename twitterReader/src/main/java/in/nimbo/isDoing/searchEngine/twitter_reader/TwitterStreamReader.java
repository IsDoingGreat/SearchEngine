package in.nimbo.isDoing.searchEngine.twitter_reader;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.kafka.KafkaProducerController;
import org.apache.hadoop.hbase.metrics.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.concurrent.ExecutionException;

public class TwitterStreamReader {

    private static final Logger logger = LoggerFactory.getLogger(TwitterStreamReader.class);
    private  ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

    private ElasticTwitterPersister elasticTwitterPersister;
    private KafkaProducerController kafkaProducer;

    private Meter meter;

    private TwitterStreamReader(String[] args) {
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(args[0])
                .setOAuthConsumerSecret(args[1])
                .setOAuthAccessToken(args[2])
                .setOAuthAccessTokenSecret(args[3]);

        kafkaProducer = new KafkaProducerController(
                Engine.getConfigs().get("kafka.brokers"),
                Engine.getConfigs().get("twitterReader.kafka.producerClientId"),
                Engine.getConfigs().get("twitterReader.kafka.topicName"));

        elasticTwitterPersister = new ElasticTwitterPersister();
        meter = SharedMetricRegistries.getDefault().meter("englishTweetsMetric");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Invalid Input");
            return;
        }

        JmxReporter jmxReporter = JmxReporter.forRegistry(SharedMetricRegistries.getDefault()).build();
        jmxReporter.start();

        System.out.println("starting");
        TwitterStreamReader twitterStreamReader = new TwitterStreamReader(args);
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
                        meter.mark();
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
        TwitterStreamFactory twitterStreamFactory = new TwitterStreamFactory(configurationBuilder.build());
        TwitterStream twitterStream = twitterStreamFactory.getInstance();
        twitterStream.addListener(listener);
        twitterStream.sample();
    }
}
