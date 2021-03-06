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
import java.util.ArrayList;
import java.util.List;

public class RelativeHosts {

    public static final int FILTER_LIMIT = 10;
    private static final String hBaseInputTableName = "backLinks";
    private static final String hBaseInputColumnFamily = "links";
    private static final String hBaseOutputTableName = "hostToHost";
    private static final String hBaseOutputColumnFamily = "I";
    private static JavaSparkContext javaSparkContext;
    private static Configuration configuration;

    public static void start() {
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseData =
                javaSparkContext.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        LongAccumulator number_of_loaded = javaSparkContext.sc().longAccumulator("number of loaded");

        JavaPairRDD<Tuple2<String, String>, Integer> mapToRelativeHost = hBaseData.flatMapToPair(
                record -> {
                    List<Tuple2<Tuple2<String, String>, Integer>> records = new ArrayList<>();
                    List<Cell> linkCells = record._2.listCells();
                    String sourceLink = Bytes.toString(record._1.get());

                    String sourceHost = sourceLink.split("/")[0];
                    if (sourceHost != null && sourceHost.length() > 0) {
                        String normalizedSourceHost = sourceHost.toLowerCase();
                        linkCells.forEach(cell -> {
                            String link = Bytes.toString(CellUtil.cloneQualifier(cell));
                            String host;
                            try {
                                host = new URL(link).getHost();
                            } catch (Exception e) {
                                return;
                            }
                            if (host.length() <= 0) {
                                return;
                            }
                            if (normalizedSourceHost.equals(host)) {
                                return;
                            }
                            records.add(new Tuple2<>(new Tuple2<>(normalizedSourceHost, host), 1));
                        });
                    }
                    number_of_loaded.add(1);
                    return records.iterator();
                }
        );

        JavaPairRDD<Tuple2<String, String>, Integer> mapToHostToHostCount = mapToRelativeHost.reduceByKey((v1, v2) -> v1 + v2);

        JavaPairRDD<Tuple2<String, String>, Integer> filter = mapToHostToHostCount.filter(t -> t._2 > FILTER_LIMIT);
        Job job = null;
        try {
            job = Job.getInstance(configuration);
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hBaseOutputTableName);
            job.setOutputFormatClass(TableOutputFormat.class);
        } catch (IOException e) {
            e.printStackTrace();
        }


        JavaPairRDD<ImmutableBytesWritable, Put> hBaseBulkPut = filter.mapToPair(
                record -> {
                    Tuple2<String, String> hostToHost = record._1;
                    int count = record._2;

                    Put put = new Put(Bytes.toBytes(hostToHost._1));
                    put.addColumn(Bytes.toBytes(hBaseOutputColumnFamily), Bytes.toBytes(hostToHost._2), Bytes.toBytes(count));

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
        SparkConf sparkConf = new SparkConf().setAppName(RelativeHosts.class.getSimpleName()).setMaster(master)
                .setJars(new String[]{args[1]});
        if (Boolean.valueOf(args[2])) {
            sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        }

        /**
         * for using in local
         */
//        String master = "local[1]";
//        SparkConf sparkConf = new SparkConf().setAppName(RelativeHosts.class.getSimpleName()).setMaster(master);
//        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        javaSparkContext = new JavaSparkContext(sparkConf);

        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.rootdir", "hdfs://srv2:9000/hbase");
        configuration.set("hbase.cluster.distributed", "true");
        configuration.set("hbase.zookeeper.quorum", "srv2,srv3");
        configuration.set("fs.defaultFS", "hdfs://srv2:9000");


        configuration.set(TableInputFormat.INPUT_TABLE, hBaseInputTableName);
        configuration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hBaseInputColumnFamily);
        start();
    }
}