package in.nimbo.isDoing.searchEngine.twitter_reader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.satori.rtm.*;
import com.satori.rtm.model.*;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.kafka.KafkaConsumerController;
import in.nimbo.isDoing.searchEngine.kafka.KafkaProducerController;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TwitterReader {
    private KafkaProducerController producerController;
    private String endpoint = Engine.getConfigs().get("twitterReader.endpoint");
    private String appkey = Engine.getConfigs().get("twitterReader.appkey");
    private String channel= Engine.getConfigs().get("twitterReader.channel");

    final RtmClient client = new RtmClientBuilder(endpoint, appkey)
            .setListener(new RtmClientAdapter() {
                @Override
                public void onEnterConnected(RtmClient client) {
                    //System.out.println("Connected to Satori RTM!");
                }
            })
            .build();

    public TwitterReader() {
        String  topicName = Engine.getConfigs().get("twitterReader.kafka.topicName");
        String  brokers = Engine.getConfigs().get("twitterReader.kafka.brokers");
        String  producerClientId = Engine.getConfigs().get("twitterReader.kafka.producerClientId");

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
                    }

                    //String created_at = jsonNode.get("created_at").asText();
                    //String text = jsonNode.get("text").asText();
                    //String timestamp_ms = jsonNode.get("timestamp_ms").asText();
                    String lang = jsonNode.get("lang").asText();

                    if (lang.equals("en")) {
                        try {
                            producerController.produce(jsonNode.toString());
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        //System.out.println(jsonNode);
                    }
                    //System.out.println("Created At: " + created_at + " Text: " + text + " lang: " + lang);
                }
            }
        };

        client.createSubscription(channel, SubscriptionMode.SIMPLE, listener);

        client.start();
    }

    public void stopGetTweets() {
        client.stop();
    }
}