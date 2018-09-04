package in.nimbo.isDoing.searchEngine.keyword;


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
import scala.Tuple2;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class AnchorKeyword {

    private static final String hBaseInputTableName = "backLinks";
    private static final String hBaseInputColumnFamily = "links";
    private static final String hBaseOutputTableName = "linkKeyWords";
    private static final String hBaseOutputColumnFamily = "keywords";
    private static int numberOfKeywords = 10;
    private static JavaSparkContext javaSparkContext;
    private static Configuration configuration;

    public static void start() {
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseData =
                javaSparkContext.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<String, List<String>> mapToHostAnchorWords = hBaseData.flatMapToPair(
                record -> {
                    List<Tuple2<String, List<String>>> records = new ArrayList<>();
                    List<Cell> linkCells = record._2.listCells();
                    linkCells.forEach(cell -> {
                        String link = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String anchorText = Bytes.toString(CellUtil.cloneValue(cell));
                        List<String> anchor = Arrays.asList(anchorText.split("\\s+"));
                        String host;
                        try {
                            host = new URL(link).getHost();
                        } catch (Exception e) {
                            return;
                        }

                        records.add(new Tuple2<>(host, anchor));
                    });

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
//                        System.out.println(entry.getKey() + " ==== " + entry.getValue());
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

        String master = "spark://srv1:7077";
        SparkConf sparkConf = new SparkConf().setAppName(AnchorKeyword.class.getSimpleName()).setMaster(master)
                .setJars(new String[]{"/home/project/sparkJobs/job.jar"});

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