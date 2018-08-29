package in.nimbo.isDoing.searchEngine.hbase;

import com.twmacinta.util.MD5;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.engine.interfaces.HaveStatus;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;

public class HBaseClient implements HaveStatus {
    private static final Logger logger = LoggerFactory.getLogger(HBaseClient.class);
    private static volatile HBaseClient instance = new HBaseClient();
    private Connection connection;
    private Configuration configuration;

    private HBaseClient() {
        String hbaseSite = Engine.getConfigs().get("hbase.site");
        java.nio.file.Path hbaseFile = Paths.get(hbaseSite);
        if (!hbaseFile.toFile().exists()) {
            throw new NullPointerException("HBase site not exists");
        }

        configuration = HBaseConfiguration.create();
        configuration.addResource(new Path(hbaseFile.toAbsolutePath().toString()));

        try {
            HBaseAdmin.available(configuration);
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            logger.warn("Error during create connection", e);
            Engine.getOutput().show(Output.Type.ERROR, e.getMessage());
            throw new IllegalStateException(e);
        }
    }

    public static HBaseClient getInstance() {
        return instance;
    }

    public static Connection getConnection() {
        return getInstance().connection;
    }

    public static void close() throws IOException {
        getConnection().close();
    }

    public String generateRowKey(URL url) {
        return url.getHost() + "/" + generateHash(url);
    }

    public String generateHash(URL url) {
        MD5 md5 = new MD5();
        try {
            md5.Update(url.getPath(), null);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return md5.asHex();
    }

    @Override
    public Status status() {
        Status status = new Status("HBase", "");
        try {
            Admin admin = getConnection().getAdmin();
            ClusterMetrics metrics = admin.getClusterMetrics();
            status.addLine("Cluster Id :" + metrics.getClusterId());
            status.addLine("Average Load :" + metrics.getAverageLoad());
            status.addLine("Region Count :" + metrics.getRegionCount());
            status.addLine("Request Count :" + metrics.getRequestCount());

            status.addLine("");
            status.addLine("Server Metrics :");
            for (Map.Entry<ServerName, ServerMetrics> entry : metrics.getLiveServerMetrics().entrySet()) {
                status.addLine("Server Name :" + entry.getKey().getServerName() + " " + entry.getKey().getHostname());
                status.addLine("\tReq/Sec :" + entry.getValue().getRequestCountPerSecond());
                status.addLine("\tMaxHeapSize :" + entry.getValue().getMaxHeapSize());
                status.addLine("\tUsedHeapSize :" + entry.getValue().getUsedHeapSize());
            }
        } catch (IOException e) {
            status.addLine(e.getMessage());
        }
        return status;
    }
}
