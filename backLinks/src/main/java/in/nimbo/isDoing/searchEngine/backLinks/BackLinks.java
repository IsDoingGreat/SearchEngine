package in.nimbo.isDoing.searchEngine.backLinks;

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
import java.util.ArrayList;
import java.util.List;

public class BackLinks {

    private static final String hBaseInputTableName = "backLinks";
    private static final String hBaseInputColumnFamily = "links";
    private static final String hBaseOutputTableName = "linkRefs";
    private static final String hBaseOutputColumnFamily = "refCount";
    private static final String hBaseOutputQuantifier = "count";
    private static JavaSparkContext javaSparkContext;
    private static Configuration configuration;

    public static void start() {
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseData =
                javaSparkContext.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<String, Integer> mapToOne = hBaseData.flatMapToPair(
                record -> {
                    List<Tuple2<String, Integer>> records = new ArrayList<>();
                    List<Cell> linkCells = record._2.listCells();
                    linkCells.forEach(cell -> {
                        String link = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String host;
                        try {
                            host = new URL(link).getHost();
                        } catch (Exception e) {
                            return;
                        }
                        if (host.length() <= 0){
                            return;
                        }

                        records.add(new Tuple2<>(host, 1));
                    });

                    return records.iterator();
                }
        );

        JavaPairRDD<String, Integer> mapToRefCount = mapToOne.reduceByKey((v1, v2) -> v1 + v2);
        Job job = null;
        try {
            job = Job.getInstance(configuration);
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hBaseOutputTableName);
            job.setOutputFormatClass(TableOutputFormat.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        JavaPairRDD<ImmutableBytesWritable, Put> hBaseBulkPut = mapToRefCount.mapToPair(
                record -> {
                    String host = record._1;
                    int count = record._2;

                    Put put = new Put(Bytes.toBytes(host));
                    put.addColumn(Bytes.toBytes(hBaseOutputColumnFamily), Bytes.toBytes(hBaseOutputQuantifier), Bytes.toBytes(count));

                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                });

        hBaseBulkPut.saveAsNewAPIHadoopDataset(job.getConfiguration());

        javaSparkContext.stop();
    }

    public static void main(String[] args) {

//        String master = "spark://localhost:7077";
//        SparkConf sparkConf = new SparkConf().setAppName(BackLinks.class.getSimpleName()).setMaster(master)
//                .setJars(new String[]{"/home/reza/sparkJobs/job.jar"});
//        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        /**
         * for using in local
         */
        String master = "local[1]";
        SparkConf sparkConf = new SparkConf().setAppName(BackLinks.class.getSimpleName()).setMaster(master);
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        javaSparkContext = new JavaSparkContext(sparkConf);

        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        configuration.set("hbase.rootdir", "hdfs://srv1:9000/hbase");
//        configuration.set("hbase.cluster.distributed", "true");
//        configuration.set("hbase.zookeeper.quorum", "srv1,srv2,srv3");
//        configuration.set("fs.defaultFS", "hdfs://srv1:9000");


        configuration.set(TableInputFormat.INPUT_TABLE, hBaseInputTableName);
        configuration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hBaseInputColumnFamily);
        start();
    }
}