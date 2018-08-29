package in.nimbo.isDoing.searchEngine.backLinks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

    private static final JavaSparkContext javaSparkContext;
    private static final Configuration configuration;
    private static final String path;

    private static final String hBaseInputTableName;
    private static final String hBaseInputColumnFamily;

    private static final String hBaseOutputTableName;
    private static final String hBaseOutputColumnFamily;
    private static final String hBaseOutputQuantifier;

    static {
        String master = "spark://localhost:7077";
        SparkConf sparkConf = new SparkConf().setAppName(BackLinks.class.getSimpleName()).setMaster(master);
        javaSparkContext = new JavaSparkContext(sparkConf);


        configuration = HBaseConfiguration.create();
        path = BackLinks.class.getClassLoader().getResource("hbase-site.xml").getPath();
        configuration.addResource(new Path(path));

        hBaseInputTableName = "backLinks";
        hBaseInputColumnFamily = "links";

        hBaseOutputTableName = "linkRefs";
        hBaseOutputColumnFamily = "refCount";
        hBaseOutputQuantifier = "count";

        configuration.set(TableInputFormat.INPUT_TABLE, hBaseInputTableName);
        configuration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hBaseInputColumnFamily);
    }


    public static void start() {
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseData =
                javaSparkContext.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<String, String> pairRDD = hBaseData.flatMapToPair(record -> {
            String key = Bytes.toString(record._1.get());
            List<Cell> linkCells = record._2.listCells();
            return linkCells.stream().map(cell -> new Tuple2<>(Bytes.toString(CellUtil.cloneValue(cell)), key)).iterator();
        });

        JavaPairRDD<String, Integer> mapToOne = pairRDD.mapToPair(r -> new Tuple2<>(r._1, 1));
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
        start();
    }
}
