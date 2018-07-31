package in.nimbo.isDoing.searchEngine.kafka;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class KafkaImpl {

    public static String KAFKA_BROKERS = "192.168.122.121:9092";

    public static Integer MESSAGE_COUNT=100;

    public static String CLIENT_ID="1";

    public static String TOPIC_NAME="urls";

    public static String GROUP_ID_CONFIG="1";

    public static Integer MAX_NO_MESSAGE_FOUND_COUNT=100;

    public static String OFFSET_RESET_LATEST="latest";

    public static String OFFSET_RESET_EARLIER="earliest";

    public static Integer MAX_POLL_RECORDS=1;


    public static void main(String[] args) {
        runProducer();
        System.out.println("\n-------------------- [FINISHED PRODUCING] --------------------\n");
        runConsumer();
    }

    public static void runProducer() {
        Producer<Long, String> producer = createProducer();

        for (int index = 0; index < MESSAGE_COUNT; index++) {
            final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC_NAME, (long)index,
                    "This is record " + index);

            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " by value \"" + record.value() + "\"" + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());

            } catch (ExecutionException | InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
    }

    public static Producer<Long, String> createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }


    public static void runConsumer() {
        Consumer<Long, String> consumer = createConsumer();

        int noMessageToFetch = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }
            consumerRecords.forEach(record -> {

                System.out.print("Record Key " + record.key() + " ");
                System.out.print("Record value \"" + record.value() + "\" ") ;
                System.out.print("Record partition " + record.partition() + " ");
                System.out.print("Record offset " + record.offset()+ "\n");

            });
            consumer.commitAsync();
        }
        consumer.close();
    }

    public static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_EARLIER);

        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        return consumer;
    }

}