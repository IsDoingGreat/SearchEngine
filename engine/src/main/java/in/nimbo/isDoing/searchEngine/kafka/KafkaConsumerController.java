package in.nimbo.isDoing.searchEngine.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;


public class KafkaConsumerController {
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerController(String brokers, String groupID, int maxPoolRecords, String topicName) {
        final KafkaConsumer<String, String> consumer = getConsumer(brokers, groupID, maxPoolRecords);
        consumer.subscribe(Collections.singletonList(topicName));
        this.consumer = consumer;
    }

    public KafkaConsumerController(String brokers, String groupID, int maxPoolRecords, String topicName, int partition) {
        final KafkaConsumer<String, String> consumer = getConsumer(brokers, groupID, maxPoolRecords);
        consumer.assign(Collections.singletonList(new TopicPartition(topicName, partition)));
        this.consumer = consumer;
    }

    private KafkaConsumer<String, String> getConsumer(String brokers, String groupID, int maxPoolRecords) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoolRecords);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<>(props);
    }

    public Iterable<ConsumerRecord<String, String>> get() {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        consumer.commitAsync();
        return records;
    }

    public void stop() {
        if (consumer != null)
            consumer.close();
    }
    public KafkaConsumer<String,String> getConsumer(){
        return consumer;
    }
}
