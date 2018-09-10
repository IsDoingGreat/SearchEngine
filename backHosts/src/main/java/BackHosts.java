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

public class BackHosts {

    public static final int FILTER_LIMIT = 10;
    private static final String hBaseInputTableName = "backLinks";
    private static final String hBaseInputColumnFamily = "links";
    private static final String hBaseOutputTableName = "hostRefsT";
    private static final String hBaseOutputColumnFamily = "RC";
    private static JavaSparkContext javaSparkContext;
    private static Configuration configuration;

    public static void start() {
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseData =
                javaSparkContext.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        LongAccumulator number_of_loaded = javaSparkContext.sc().longAccumulator("number of loaded");
        JavaPairRDD<String, Integer> mapToOne = hBaseData.flatMap(r -> r._2.listCells().iterator())
                .map(r -> Bytes.toString(CellUtil.cloneQualifier(r)))
                .mapToPair(s -> {
                    String link = s;
                    String host;

                    try {
                        host = new URL(link).getHost();
                    } catch (Exception e) {
                        return new Tuple2<>("e", 0);
                    }

                    if (host == null || host.length() <= 0) {
                        return new Tuple2<>("e", 0);
                    }
                    number_of_loaded.add(1);
                    return new Tuple2<>(host.toLowerCase(), 1);
                });


        JavaPairRDD<String, Integer> mapToRefCount = mapToOne.reduceByKey((v1, v2) -> v1 + v2);
        JavaPairRDD<String, Integer> mapToRefCountFiltered = mapToRefCount.filter(t -> (t._2 > FILTER_LIMIT && !t._1.equals("e")));

        Job job = null;
        try {
            job = Job.getInstance(configuration);
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hBaseOutputTableName);
            job.setOutputFormatClass(TableOutputFormat.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        JavaPairRDD<ImmutableBytesWritable, Put> hBaseBulkPut = mapToRefCountFiltered.mapToPair(
                record -> {
                    String host = record._1;
                    int count = record._2;

                    Put put = new Put(Bytes.toBytes(host));
                    put.addColumn(Bytes.toBytes(hBaseOutputColumnFamily), null, Bytes.toBytes(count));

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
        SparkConf sparkConf = new SparkConf().setAppName(BackHosts.class.getSimpleName()).setMaster(master)
                .setJars(new String[]{args[1]});
        if (Boolean.valueOf(args[2])) {
            sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        }

        /**
         * for using in local
         */
//        String master = "local[1]";
//        SparkConf sparkConf = new SparkConf().setAppName(BackHosts.class.getSimpleName()).setMaster(master);
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