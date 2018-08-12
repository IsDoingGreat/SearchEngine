package in.nimbo.isDoing.searchEngine.crawler.duplicate_checker;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.isDoing.searchEngine.crawler.page.WebPage;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

public class CaffeineDuplicateChecker implements DuplicateChecker {
    private final static Logger logger = LoggerFactory.getLogger(WebPage.class);
    private final static Object OBJECT = new Object();
    private Cache<String, Object> cache;
    private TableName crawledLinkTableName;
    private String crawledLinkColumnFamily;
    private String crawledLinkQuantifier;
    private Table table;

    private Connection connection;

    public CaffeineDuplicateChecker() {
        Engine.getOutput().show("Creating CaffeineDuplicateChecker...");
        logger.info("Creating CaffeineDuplicateChecker...");

        cache = Caffeine.newBuilder().build();

        connection = HBaseClient.getConnection();

        crawledLinkTableName = TableName.valueOf(Engine.getConfigs().get("crawler.persister.db.hbase.crawledLink.tableName"));
        crawledLinkColumnFamily = Engine.getConfigs().get("crawler.persister.db.hbase.crawledLink.columnFamily");
        crawledLinkQuantifier = Engine.getConfigs().get("crawler.persister.db.hbase.crawledLink.qualifier");

        try {
            table = connection.getTable(crawledLinkTableName);
        } catch (IOException e) {
            logger.error("Get table of HBase connection failed: ", e);
            throw new IllegalStateException(e);
        }

        loadDataFromHBase();

        logger.info("CaffeineDuplicateChecker Created With Settings");
    }

    private void loadDataFromHBase() {
        try {
            Scan scan = new Scan();
            scan.setCaching(1000);
            scan.addColumn(Bytes.toBytes(crawledLinkColumnFamily), Bytes.toBytes(crawledLinkQuantifier));
            int loaded = 0;
            for (Result res : table.getScanner(scan)) {
                cache.put(Bytes.toString(res.getRow()), OBJECT);
                loaded++;
                if (loaded % 1000 == 0)
                    Engine.getOutput().show(loaded + " Cache Entries loaded!");
            }
            Engine.getOutput().show(loaded + " Cache Entries loaded!");

        } catch (IOException e) {
            logger.error("ERROR DURING LOADING CACHE FROM HBASE", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean checkDuplicateAndSet(URL url) throws Exception {
        if (cache.getIfPresent(HBaseClient.getInstance().generateRowKey(url)) != null)
            return true;
        else {
            cache.put(HBaseClient.getInstance().generateRowKey(url), OBJECT);
            persist(url);
            return false;
        }
    }

    private void persist(URL url) {
        try {
            Put put = new Put(Bytes.toBytes(HBaseClient.getInstance().generateRowKey(url)));
            put.addColumn(Bytes.toBytes(crawledLinkColumnFamily), Bytes.toBytes(crawledLinkQuantifier), Bytes.toBytes(url.toExternalForm()));
            table.put(put);
        } catch (IOException e) {
            logger.error("Persist URL failed: ", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void stop() {
        try {
            table.close();
        } catch (IOException e) {
            logger.error("Closing crawledLink table failed: ", e);
            throw new IllegalStateException(e);
        }
    }
}