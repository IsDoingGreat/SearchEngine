import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AnchorKeyword {

    public static final int STOP_WORD_LENGTH = 3;
    public static final int KEYWORDS_LIMIT = 10;
    public static final int FILTER_LIMIT = 100;
    private static final String hBaseInputTableName = "backLinks";
    private static final String hBaseInputColumnFamily = "links";
    private static final String hBaseOutputTableName = "hostKeyWordsT";
    private static final String hBaseOutputColumnFamily = "K";
    private static JavaSparkContext javaSparkContext;
    private static Configuration configuration;

    public static void start() {
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseData =
                javaSparkContext.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);


        LongAccumulator number_of_loaded = javaSparkContext.sc().longAccumulator("number of loaded");
//        JavaPairRDD<Tuple2<String, String>, Integer> mapToAnchor = hBaseData.flatMap(r -> r._2.listCells().iterator())
//                .flatMapToPair(cell -> {
//                    List<Tuple2<Tuple2<String, String>, Integer>> records = new ArrayList<>();
//                    String link = Bytes.toString(CellUtil.cloneQualifier(cell));
//                    String anchorText = Bytes.toString(CellUtil.cloneValue(cell)).toLowerCase();
//                    List<String> anchors = Arrays.stream(anchorText.split("[^\\w']+")).filter(s -> s.length() > STOP_WORD_LENGTH).collect(Collectors.toList());
//                    String host;
//                    try {
//                        host = new URL(link).getHost().toLowerCase();
//                    } catch (Exception e) {
//                        throw new IllegalStateException("Invalid host");
//                    }
//
//                    for (String anchor : anchors) {
//                        if (host.length() > 0) {
//                            records.add(new Tuple2<>(new Tuple2<>(host, anchor), 1));
//                        }
//                    }
//                    number_of_loaded.add(1);
//                    return records.iterator();
//                });

        JavaPairRDD<Tuple2<String, String>, Integer> mapToAnchor = hBaseData.flatMap(r -> r._2.listCells().iterator())
                .mapToPair(cell -> {
                    String link = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String host;
                    try {
                        host = new URL(link).getHost().toLowerCase();
                    } catch (Exception e) {
                        return new Tuple2<>("e", "");
                    }
                    String anchorText = Bytes.toString(CellUtil.cloneValue(cell)).toLowerCase();
                    number_of_loaded.add(1);
                    return new Tuple2<>(host, anchorText);
                })
                .flatMapToPair(t -> Arrays.stream(t._2.split("[^\\w']+"))
                        .filter(s -> s.length() > STOP_WORD_LENGTH)
                        .map(s -> new Tuple2<>(new Tuple2<>(t._1, s), 1)).iterator());

        JavaPairRDD<Tuple2<String, String>, Integer> hostToAnchorCount = mapToAnchor.reduceByKey((v1, v2) -> v1 + v2);

        JavaPairRDD<Tuple2<String, String>, Integer> filter = hostToAnchorCount.filter(t -> t._2 > FILTER_LIMIT);

        JavaPairRDD<String, Tuple2<String, Integer>> hostToAnchors = filter.mapToPair(record -> new Tuple2<>(record._1._1, new Tuple2<>(record._1._2, record._2)));

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> hostToAnchorTuples = hostToAnchors.groupByKey();

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> hostToAnchorTuplesTops = hostToAnchorTuples.mapToPair(record -> {
                    int[] keywordsCount = new int[KEYWORDS_LIMIT];
                    String[] keywords = new String[KEYWORDS_LIMIT];
                    for (int i = 0; i < KEYWORDS_LIMIT; i++) {
                        keywordsCount[i] = -(i + 1);
                        keywords[i] = "";
                    }

                    int minValue = Integer.MAX_VALUE;
                    int index = -1;
                    for (Tuple2<String, Integer> t : record._2) {
                        for (int i = 0; i < KEYWORDS_LIMIT; i++) {
                            if (minValue > keywordsCount[i]) {
                                minValue = keywordsCount[i];
                                index = i;
                            }
                        }

                        if (index != -1 && t._2 > minValue) {
                            keywords[index] = t._1;
                            keywordsCount[index] = t._2;
                        }

                        minValue = Integer.MAX_VALUE;
                        index = -1;
                    }

                    List<Tuple2<String, Integer>> records = new ArrayList<>();
                    for (int i = 0; i < KEYWORDS_LIMIT; i++) {
                        if (keywords[i].length() > STOP_WORD_LENGTH) {
                            records.add(new Tuple2<>(keywords[i], keywordsCount[i]));
                        }
                    }

                    return new Tuple2<>(record._1, records);
                }
        );

        Job job = null;
        try {
            job = Job.getInstance(configuration);
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hBaseOutputTableName);
            job.setOutputFormatClass(TableOutputFormat.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        JavaPairRDD<ImmutableBytesWritable, Put> hBaseBulkPut = hostToAnchorTuplesTops.mapToPair(
                record -> {
                    String host = record._1;

                    Put put = new Put(Bytes.toBytes(host));
                    int counter = 0;
                    for (Tuple2<String, Integer> anchorCount : record._2) {
                        counter++;
                        put.addColumn(Bytes.toBytes(hBaseOutputColumnFamily), Bytes.toBytes(anchorCount._1), Bytes.toBytes(anchorCount._2));

                    }
                    if (counter <= 0) {
                        throw new IllegalStateException("Host doesn't have keywords");
                    }

                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                });

        hBaseBulkPut.saveAsNewAPIHadoopDataset(job.getConfiguration());

        javaSparkContext.stop();
    }

    public static void main(String[] args) {

        if (args.length < 3) {
            System.out.println("Invalid args");
            return;
        }

        String master = args[0];
        SparkConf sparkConf = new SparkConf().setAppName(AnchorKeyword.class.getSimpleName()).setMaster(master)
                .setJars(new String[]{args[1]});
        if (Boolean.valueOf(args[2])) {
            sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        }

        /**
         * for using in local
         */
//        String master = "local[*]";
//        SparkConf sparkConf = new SparkConf().setAppName(AnchorKeyword.class.getSimpleName()).setMaster(master);
//        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        javaSparkContext = new JavaSparkContext(sparkConf);

        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.rootdir", "hdfs://srv1:9000/hbase");
        configuration.set("hbase.cluster.distributed", "true");
        configuration.set("hbase.zookeeper.quorum", "srv1,srv2,srv3");
        configuration.set("fs.defaultFS", "hdfs://srv1:9000");

        configuration.set(TableInputFormat.INPUT_TABLE, hBaseInputTableName);
        configuration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hBaseInputColumnFamily);
        start();
    }
}