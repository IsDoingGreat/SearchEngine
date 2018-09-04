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
import java.util.List;

public class BackLinks {

    private static JavaSparkContext javaSparkContext;
    private static Configuration configuration;

    private static final String hBaseInputTableName = "backLinksT";
    private static final String hBaseInputColumnFamily = "linksT";

    private static final String hBaseOutputTableName = "linkRefsT";
    private static final String hBaseOutputColumnFamily = "refCountT";
    private static final String hBaseOutputQuantifier = "countT";


    public static void start() {
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseData =
                javaSparkContext.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<String, String> pairRDD = hBaseData.flatMapToPair(record -> {
            String key = Bytes.toString(record._1.get());
            List<Cell> linkCells = record._2.listCells();
                return linkCells.stream().map(cell -> new Tuple2<>(Bytes.toString(CellUtil.cloneQualifier(cell)), key)).iterator();
        });

        JavaPairRDD<String, Integer> mapToOne = pairRDD.mapToPair(r -> new Tuple2<>(r._1, 1));
        JavaPairRDD<String, Integer> mapToRefCount = mapToOne.reduceByKey((v1, v2) -> v1 + v2);

        mapToRefCount.foreach(s ->
                System.out.println(s._1 + " " + s._2));

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
                    String link = record._1;
                    int count = record._2;

                    Put put = new Put(Bytes.toBytes(link));
                    put.addColumn(Bytes.toBytes(hBaseOutputColumnFamily), Bytes.toBytes(hBaseOutputQuantifier), Bytes.toBytes(count));

                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                });

        hBaseBulkPut.saveAsNewAPIHadoopDataset(job.getConfiguration());

        javaSparkContext.stop();
    }

    public static void main(String[] args) {
        String master = "spark://srv1:7077";
        SparkConf sparkConf = new SparkConf().setAppName(BackLinks.class.getSimpleName()).setMaster(master)
                .setJars(new String[]{"/home/project/sparkJobs/job.jar"});
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