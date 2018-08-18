package in.nimbo.isDoing.searchEngine.crawler.persister.db;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;
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

public class HBaseDBPersister implements DBPersister {
    private static final Logger logger = LoggerFactory.getLogger(HBaseDBPersister.class);
    private static final String DEFAULT_FLUSH_NUMBER = "150";

    private Connection connection;
//    private List<Put> pagesBulkPut;
//    private TableName pagesTableName;
//    private String pagesColumnFamily;
//    private String[] pagesQualifiers;

    private List<Put> backLinkBulkPut;
    private TableName backLinkTableName;
    private String backLinkColumnFamily;

    private int hbaseFlushNumberLimit;

    public HBaseDBPersister() {
        Engine.getOutput().show("Creating HBaseItemPersister...");
        logger.info("Creating HBaseItemPersister...");

        connection = HBaseClient.getConnection();

//        pagesBulkPut = new ArrayList<>();
//        pagesTableName = TableName.valueOf(Engine.getConfigs().get("crawler.persister.db.hbase.pages.tableName"));
//        pagesColumnFamily = Engine.getConfigs().get("crawler.persister.db.hbase.pages.columnFamily");
//        pagesQualifiers = Engine.getConfigs().get("crawler.persister.db.hbase.pages.qualifiers").split(";");

        backLinkBulkPut = new ArrayList<>();
        backLinkTableName = TableName.valueOf(Engine.getConfigs().get("crawler.persister.db.hbase.backLinks.tableName"));
        backLinkColumnFamily = Engine.getConfigs().get("crawler.persister.db.hbase.backLinks.columnFamily");

        hbaseFlushNumberLimit = Integer.parseInt(Engine.getConfigs().get(
                "crawler.persister.db.hbase.flushNumberLimit", DEFAULT_FLUSH_NUMBER));

        logger.info("HBaseItemPersister Created With Settings");
    }

    @Override
    public void persist(Page page) throws Exception {

        String rowKey = HBaseClient.getInstance().generateRowKey(page.getUrl());
        byte[] rowKeyBytes = Bytes.toBytes(rowKey);

//        Put pagePut = new Put(rowKeyBytes);
//        pagePut.addColumn(Bytes.toBytes(pagesColumnFamily), Bytes.toBytes(pagesQualifiers[0]), Bytes.toBytes(page.getUrl().toExternalForm()));
//        pagePut.addColumn(Bytes.toBytes(pagesColumnFamily), Bytes.toBytes(pagesQualifiers[1]), Bytes.toBytes(page.getBody()));
//        pagesBulkPut.add(pagePut);

        Put backLinkPut = new Put(rowKeyBytes);

        int index = 0;
        byte[] backLinkCFBytes = Bytes.toBytes(backLinkColumnFamily);
        for (String link : page.getOutgoingUrls()) {
            backLinkPut.addColumn(backLinkCFBytes, Bytes.toBytes(index), Bytes.toBytes(link));
            index++;
        }
        if (index > 0) {
            backLinkBulkPut.add(backLinkPut);
        }
        flushIfNeeded();
    }

    private void flushIfNeeded() throws Exception {
        if (backLinkBulkPut.size() >= hbaseFlushNumberLimit) {
            flush();
        }
    }

    @Override
    public void flush() throws Exception {
        if (backLinkBulkPut.size() > 0) {
//            Table pagesTable = connection.getTable(pagesTableName);
//            pagesTable.put(pagesBulkPut);
//            pagesTable.close();

            Table backLinkTable = connection.getTable(backLinkTableName);
            backLinkTable.put(backLinkBulkPut);
            backLinkTable.close();

//            pagesBulkPut.clear();
            backLinkBulkPut.clear();
        }
    }
}
