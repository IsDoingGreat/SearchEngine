package in.nimbo.isDoing.searchEngine.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerController {
    private static Logger logger = LoggerFactory.getLogger(KafkaProducerController.class);
    private Producer<Long, String> producer;
    private String topicName;

    public KafkaProducerController(String brokers, String clientID, String topicName) {
        this.topicName = topicName;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner);
        this.producer = new KafkaProducer<>(props);
    }

    public void produce(long index, String text) throws ExecutionException, InterruptedException {
        ProducerRecord<Long, String> record = new ProducerRecord<>(topicName, index, text);

        producer.send(record).get();
    }

    public void produce(String text) throws ExecutionException, InterruptedException {
        ProducerRecord<Long, String> record = new ProducerRecord<>(topicName, text);

        producer.send(record);
    }

    public void stop() {
        if (producer!= null) {
            producer.flush();
            producer.close();
        }
    }
}
