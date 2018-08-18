package in.nimbo.isDoing.searchEngine.twitter_reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.satori.rtm.*;
import com.satori.rtm.model.AnyJson;
import com.satori.rtm.model.SubscriptionData;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.kafka.KafkaProducerController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TwitterReader {
    private static final Logger logger = LoggerFactory.getLogger(TwitterReader.class);
    private KafkaProducerController producerController;
    private String endpoint = Engine.getConfigs().get("twitterReader.endpoint");
    private String appkey = Engine.getConfigs().get("twitterReader.appkey");
    final RtmClient client = new RtmClientBuilder(endpoint, appkey)
            .setListener(new RtmClientAdapter() {
                @Override
                public void onEnterConnected(RtmClient client) {
                    //System.out.println("Connected to Satori RTM!");
                }
            })
            .build();
    private String channel = Engine.getConfigs().get("twitterReader.channel");
    private long received = 0;

    public TwitterReader() {
        String topicName = Engine.getConfigs().get("twitterReader.kafka.topicName");
        String brokers = Engine.getConfigs().get("twitterReader.kafka.brokers");
        String producerClientId = Engine.getConfigs().get("twitterReader.kafka.producerClientId");

        producerController = new KafkaProducerController(brokers, producerClientId, topicName);
    }

    public void getTweets() throws InterruptedException {
        SubscriptionAdapter listener = new SubscriptionAdapter() {
            @Override
            public void onSubscriptionData(SubscriptionData data) {
                for (AnyJson json : data.getMessages()) {

                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode jsonNode = null;

                    try {
                        jsonNode = objectMapper.readTree(json.toString());
                    } catch (IOException e) {
                        e.printStackTrace();
                        continue;
                    }

                    String id = jsonNode.get("id").asText();

                    if (!jsonNode.has("entities"))
                        continue;

                    JsonNode hashtags = jsonNode.get("entities").get("hashtags");

                    String lang = "";
                    if (jsonNode.has("lang")) {
                        lang = jsonNode.get("lang").asText();
                    }

                    if (hashtags.size() > 0 && lang.startsWith("en")) {
                        try {
                            StringBuilder hashtagString = new StringBuilder();
                            for (JsonNode element : hashtags) {
                                hashtagString.append(element.get("text").asText()).append(" ");
                            }
                            producerController.produce(id, hashtagString.toString());
                            received++;
                            logger.trace("Received {} , {}", received, jsonNode);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        };

        client.createSubscription(channel, SubscriptionMode.SIMPLE, listener);

        client.start();
    }

    public void stopGetTweets() {
        client.shutdown();
        producerController.stop();
    }
}