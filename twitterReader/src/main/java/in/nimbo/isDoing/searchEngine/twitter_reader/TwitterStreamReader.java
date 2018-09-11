package in.nimbo.isDoing.searchEngine.twitter_reader;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.SystemConfigs;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import in.nimbo.isDoing.searchEngine.kafka.KafkaProducerController;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class TwitterStreamReader implements Service {

    private static final Logger logger = LoggerFactory.getLogger(TwitterStreamReader.class);

    private  ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

    private ElasticTwitterPersister elasticTwitterPersister;
    private KafkaProducerController kafkaProducer;

    private Meter meter;
    private JmxReporter jmxReporter;

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

        SharedMetricRegistries.setDefault("metricRegistry");
        meter = SharedMetricRegistries.getDefault().meter("englishTweetsMetric");
        jmxReporter = JmxReporter.forRegistry(SharedMetricRegistries.getDefault()).build();

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
        twitterStream.addListener(listener);
        twitterStream.sample();
    }

    @Override
    public void start() {
        jmxReporter.start();

    }

    @Override
    public void stop() {
        twitterStream.shutdown();
        jmxReporter.stop();
    }

    @Override
    public Map<String, Object> status() {
        return null;
    }

    @Override
    public String getName() {
        return "TwitterStreamReader";
    }
}
