package in.nimbo.isDoing.searchEngine.crawler.persister.db;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.crawler.persister.PagePersisterImpl;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HBaseDBPersister implements DBPersister {
    private static final Logger logger = LoggerFactory.getLogger(HBaseDBPersister.class);

    private Connection connection;
    private List<Put> backLinkBulkPut;
    private TableName backLinkTableName;
    private String backLinkColumnFamily;

    private PagePersisterImpl pagePersister;

    public HBaseDBPersister(PagePersisterImpl pagePersister) {
        Engine.getOutput().show("Creating HBaseItemPersister...");
        this.pagePersister = pagePersister;
        logger.info("Creating HBaseItemPersister...");

        connection = HBaseClient.getConnection();

        backLinkBulkPut = new ArrayList<>();
        backLinkTableName = TableName.valueOf(Engine.getConfigs().get("crawler.persister.db.hbase.backLinks.tableName"));
        backLinkColumnFamily = Engine.getConfigs().get("crawler.persister.db.hbase.backLinks.columnFamily");

        logger.info("HBaseItemPersister Created With Settings");
    }

    @Override
    public void persist(Page page) throws Exception {

        String rowKey = HBaseClient.getInstance().generateRowKey(page.getUrl());
        byte[] rowKeyBytes = Bytes.toBytes(rowKey);


        Set<Map.Entry<String, String>> entries = page.getOutgoingUrls().entrySet();
        if (entries.size() == 0)
            return;

        Put backLinkPut = new Put(rowKeyBytes);

        byte[] backLinkCFBytes = Bytes.toBytes(backLinkColumnFamily);
        for (Map.Entry<String, String> entry : entries) {
            backLinkPut.addColumn(backLinkCFBytes, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
        }

        backLinkBulkPut.add(backLinkPut);

        flushIfNeeded();
    }

    private void flushIfNeeded() throws Exception {
        if (backLinkBulkPut.size() >= pagePersister.getHbaseFlushNumberLimit()) {
            flush();
        }
    }

    @Override
    public void flush() throws Exception {
        if (backLinkBulkPut.size() > 0) {

            Table backLinkTable = connection.getTable(backLinkTableName);
            backLinkTable.put(backLinkBulkPut);
            backLinkTable.close();

            backLinkBulkPut.clear();
        }
    }
}
