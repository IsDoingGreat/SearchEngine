import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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
import java.util.*;
import java.util.stream.Collectors;

public class AnchorKeyword {

    public static final int stopWordLength = 3;
    private static final String hBaseInputTableName = "backLinks";
    private static final String hBaseInputColumnFamily = "links";
    private static final String hBaseOutputTableName = "hostKeyWords";
    private static final String hBaseOutputColumnFamily = "keywords";
    private static int numberOfKeywords = 10;
    private static JavaSparkContext javaSparkContext;
    private static Configuration configuration;

    public static void start() {
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseData =
                javaSparkContext.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);


        LongAccumulator number_of_loaded = javaSparkContext.sc().longAccumulator("number of loaded");
        JavaPairRDD<String, List<String>> mapToHostAnchorWords = hBaseData.flatMapToPair(
                record -> {
                    List<Tuple2<String, List<String>>> records = new ArrayList<>();
                    List<Cell> linkCells = record._2.listCells();
                    linkCells.forEach(cell -> {
                        String link = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String anchorText = Bytes.toString(CellUtil.cloneValue(cell)).toLowerCase();
                        List<String> anchor = Arrays.stream(anchorText.split("[^\\w']+")).filter(s -> s.length() > stopWordLength).collect(Collectors.toList());
                        String host;
                        try {
                            host = new URL(link).getHost();
                        } catch (Exception e) {
                            return;
                        }

                        if (host != null && host.length() > 0 && anchor.size() > 0) {
                            records.add(new Tuple2<>(host.toLowerCase(), anchor));
                        }
                    });

                    number_of_loaded.add(1);
                    return records.iterator();
                }
        );

        JavaPairRDD<String, List<String>> mapToHostAllAnchorWords = mapToHostAnchorWords.reduceByKey(
                ((v1, v2) -> {
                    List<String> anchorsWords = new ArrayList<>();
                    anchorsWords.addAll(v1);
                    anchorsWords.addAll(v2);
                    return anchorsWords;
                })
        );

        JavaPairRDD<String, List<String>> mapToHostKeyWords = mapToHostAllAnchorWords.mapToPair(
                record -> {
                    String host = record._1;
                    List<String> keywords = new ArrayList<>();
                    Map<String, Long> counts =
                            record._2.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));

                    Set<Map.Entry<String, Long>> set = counts.entrySet();
                    List<Map.Entry<String, Long>> list = new ArrayList<>(set);

                    Collections.sort(list, (o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));

                    int counter = 0;
                    for (Map.Entry<String, Long> entry : list) {
                        if (counter < numberOfKeywords) {
                            keywords.add(entry.getKey());
                        }
                        counter++;
                    }

                    return new Tuple2<>(host, keywords);
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

        JavaPairRDD<ImmutableBytesWritable, Put> hBaseBulkPut = mapToHostKeyWords.mapToPair(
                record -> {
                    String link = record._1;
                    List<String> keyWords = record._2;

                    Put put = new Put(Bytes.toBytes(link));
                    int index = 0;
                    for (String keyWord : keyWords) {
                        put.addColumn(Bytes.toBytes(hBaseOutputColumnFamily), Bytes.toBytes(index), Bytes.toBytes(keyWord));
                        index++;
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
//        String master = "local[1]";
//        SparkConf sparkConf = new SparkConf().setAppName(AnchorKeyword.class.getSimpleName()).setMaster(master);

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