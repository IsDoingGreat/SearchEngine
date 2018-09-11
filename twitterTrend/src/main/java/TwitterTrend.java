import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class TwitterTrend {
    public static final int FILTER_LIMIT = 200;
    public static final int STOP_WORD_LENGTH = 4;
    static final String SPACE = "\\W";
    private static final String GROUP_ID = "TTGP";
    private static final String TOPICS = "tweetsItems";
    private static final String AUTO_OFFSET_RESET_CONFIG = "earliest";
    private static final String HBASE_TABLE_NAME = "twitterTrendWords";
    private static final String HBASE_COLUMN_FAMILY = "WC";
    private static final String HBASE_QUALIFIER = "C";
    private static final boolean ENABLE_AUTO_COMMIT_CONFIG = true;
    //    private static SparkConf sparkConf = new SparkConf().setAppName(TwitterTrend.class.getSimpleName()).setMaster(SPARK_MASTER);
//    private static JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(DURATIONS_SECOND));
    private static JavaStreamingContext javaStreamingContext;
    private static Set<String> TOPICS_SET = new HashSet<>(Arrays.asList(TOPICS.split(",")));
    private static Map<String, Object> KAFKA_PARAMS = new HashMap<>();
    private static Configuration configuration;

    public static void start() throws Exception {

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(TOPICS_SET, KAFKA_PARAMS)
        );

        // Get the lines, split them into words, count the words and print
        // Removing stop words
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.stream(x.toLowerCase().split(SPACE)).filter(s -> s.length() > STOP_WORD_LENGTH).iterator());

        // Calculate count of each word
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        // Sort words to find trending words
        JavaPairDStream<Integer, String> swappedPair = wordCounts.mapToPair(Tuple2::swap);
        JavaPairDStream<Integer, String> sortedWords = swappedPair.transformToPair(
                (Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>) jPairRDD -> jPairRDD.sortByKey(false));
        JavaPairDStream<Integer, String> filter = sortedWords.filter(t -> (t._1 > FILTER_LIMIT));

        // Put trending words to HBase table
        filter.foreachRDD(
                rdd -> {
                    Job job = null;
                    try {
                        job = Job.getInstance(configuration);
                        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, HBASE_TABLE_NAME);
                        job.setOutputFormatClass(TableOutputFormat.class);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    JavaPairRDD<ImmutableBytesWritable, Put> hBaseBulkPut = rdd.mapToPair(
                            record -> {
                                int count = record._1;
                                String word = record._2;
                                Put put = new Put(Bytes.toBytes(word));
                                put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY), Bytes.toBytes(HBASE_QUALIFIER), Bytes.toBytes(count));
                                return new Tuple2<>(new ImmutableBytesWritable(), put);

                            });
                    hBaseBulkPut.saveAsNewAPIHadoopDataset(job.getConfiguration());
                }
        );

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

    public static void main(String[] args) {
        int durationsMin = 30;
        String brokers = "srv2:9092,srv3:9092";
//        if (args.length < 4) {
//            System.out.println("Invalid args");
//            return;
//        }
//
//        String master = args[0];
//        SparkConf sparkConf = new SparkConf().setAppName(TwitterTrend.class.getSimpleName()).setMaster(master).setJars(new String[]{args[1]});
//        if (Boolean.valueOf(args[2])) {
//            sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        }
//
//        if (args[3] != null) {
//            durationsMin = Integer.parseInt(args[3]);
//        }
//        if (args[4] != null) {
//            brokers = args[4];
//        }

        /**
         * for using in local
         */

        String master = "local[*]";
        SparkConf sparkConf = new SparkConf().setAppName(TwitterTrend.class.getSimpleName()).setMaster(master);
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.minutes(durationsMin));

        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        configuration.set("hbase.rootdir", "hdfs://srv2:9000/hbase");
//        configuration.set("hbase.cluster.distributed", "true");
//        configuration.set("hbase.zookeeper.quorum", "srv2,srv3");
//        configuration.set("fs.defaultFS", "hdfs://srv2:9000");

        KAFKA_PARAMS.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        KAFKA_PARAMS.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        KAFKA_PARAMS.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KAFKA_PARAMS.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KAFKA_PARAMS.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT_CONFIG);
        KAFKA_PARAMS.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_CONFIG);

        try {
            start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
