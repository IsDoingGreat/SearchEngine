package in.nimbo.isDoing.searchEngine.hbase;

import com.twmacinta.util.MD5;
import in.nimbo.isDoing.searchEngine.engine.Engine;
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
import java.util.*;

public class HBaseClient {
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

    public Map<?, ?> getJson() {
        Map<String, Object> clusterMap = new HashMap<>();
        try {
            Admin admin = getConnection().getAdmin();
            ClusterMetrics metrics = admin.getClusterMetrics();
            clusterMap.put("clusterId", metrics.getClusterId());
            clusterMap.put("regionCount", metrics.getRegionCount());
            clusterMap.put("requestCount", metrics.getRequestCount());
            List<Map<String, Object>> servers = new ArrayList<>();
            for (Map.Entry<ServerName, ServerMetrics> entry : metrics.getLiveServerMetrics().entrySet()) {
                Map<String, Object> serverMap = new HashMap<>();
                serverMap.put("serverName", entry.getKey().getServerName());
                serverMap.put("reqPerSec", entry.getValue().getRequestCountPerSecond());
                serverMap.put("maxHeapSize", entry.getValue().getMaxHeapSize());
                serverMap.put("usedHeapSize", entry.getValue().getUsedHeapSize());
                servers.add(serverMap);
            }
            clusterMap.put("servers", servers);
        } catch (IOException e) {
            clusterMap.put("errors", Collections.singletonList(e.getMessage()));
        }
        return clusterMap;
    }
}
