package in.nimbo.isDoing.searchEngine.hbase;

import com.twmacinta.util.MD5;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;

public class HBaseClient {
    private static final Logger logger = LoggerFactory.getLogger(HBaseClient.class);
    private static volatile HBaseClient instance = new HBaseClient();
    private Connection connection;
    private Configuration configuration;

    private HBaseClient() {
        String hbaseSite = Engine.getConfigs().get("hbase.site");
        URL resource = this.getClass().getClassLoader().getResource(hbaseSite);
        if (resource == null) {
            throw new NullPointerException("HBase site null");
        }

        configuration = HBaseConfiguration.create();
        configuration.addResource(new Path(resource.getPath()));

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

    public  String generateRowKey(URL url) {
        String rowKey = url.getHost();
        MD5 md5 = new MD5();
        try {
            md5.Update(url.getPath(), null);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String hash = md5.asHex();
        rowKey += "/" + hash;

        return rowKey;
    }

}
