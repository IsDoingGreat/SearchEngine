package in.nimbo.isDoing.searchEngine.news_reader.persister.db;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import in.nimbo.isDoing.searchEngine.news_reader.model.Item;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HBaseItemPersister implements DBPersister {
    private static final Logger logger = LoggerFactory.getLogger(HBaseItemPersister.class);
    private static final String DEFAULT_FLUSH_NUMBER = "150";

    private Connection connection;

    private List<Put> itemBulkPut;
    private TableName itemsTableName;
    private String itemsColumnFamily;

    private int hbaseFlushNumberLimit;

    public HBaseItemPersister() {
        Engine.getOutput().show("Creating HBaseItemPersister...");
        logger.info("Creating HBaseItemPersister...");

        connection = HBaseClient.getConnection();

        itemBulkPut = new ArrayList<>();
        itemsTableName = TableName.valueOf(Engine.getConfigs().get("newsReader.persister.db.hbase.items.tableName"));
        itemsColumnFamily = Engine.getConfigs().get("newsReader.persister.db.hbase.items.columnFamily");

        hbaseFlushNumberLimit = Integer.parseInt(Engine.getConfigs().get(
                "newsReader.persister.db.in.nimbo.isDoing.searchEngine.hbase.flushNumberLimit", DEFAULT_FLUSH_NUMBER));

        logger.info("HBaseItemPersister Created With Settings");
    }

    @Override
    public void persist(Item item) throws Exception {

        byte[] rowKeyBytes = Bytes.toBytes(item.getLink().toExternalForm());

        Put itemPut = new Put(rowKeyBytes);
        itemPut.addColumn(Bytes.toBytes(itemsColumnFamily), Bytes.toBytes("title"), Bytes.toBytes(item.getTitle()));
        itemPut.addColumn(Bytes.toBytes(itemsColumnFamily), Bytes.toBytes("text"), Bytes.toBytes(item.getText()));
        itemPut.addColumn(Bytes.toBytes(itemsColumnFamily), Bytes.toBytes("time"), Bytes.toBytes(item.getDate().getTime()));


        itemBulkPut.add(itemPut);


        flushIfNeeded();
    }

    private void flushIfNeeded() throws Exception {
        if (itemBulkPut.size() >= hbaseFlushNumberLimit) {
            flush();
        }
    }

    @Override
    public void flush() throws Exception {
        if (itemBulkPut.size() > 0) {
            Table itemsTable = connection.getTable(itemsTableName);
            itemsTable.put(itemBulkPut);
            itemsTable.close();

            itemBulkPut.clear();
        }
    }
}
