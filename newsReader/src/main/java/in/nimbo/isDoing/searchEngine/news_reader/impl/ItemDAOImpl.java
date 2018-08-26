package in.nimbo.isDoing.searchEngine.news_reader.impl;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import in.nimbo.isDoing.searchEngine.news_reader.dao.ItemDAO;
import in.nimbo.isDoing.searchEngine.news_reader.model.Item;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class ItemDAOImpl implements ItemDAO {
    private static Logger logger = LoggerFactory.getLogger(ItemDAOImpl.class);
    private final Table table;
    private Connection connection;
    private TableName itemsTableName;
    private String itemsColumnFamily;
    private int maxSize;
    private LoadingCache<String, Object> cache;
    private BlockingQueue<Item> queue;

    public ItemDAOImpl(BlockingQueue<Item> queue) {
        Engine.getOutput().show("Creating ItemDAO...");
        logger.info("Creating ItemDAO...");

        this.queue = queue;

        connection = HBaseClient.getConnection();

        itemsTableName = TableName.valueOf(Engine.getConfigs().get("newsReader.persister.db.hbase.items.tableName"));
        itemsColumnFamily = Engine.getConfigs().get("newsReader.persister.db.hbase.items.columnFamily");
        maxSize = Integer.parseInt(Engine.getConfigs().get("newsReader.persister.db.in.nimbo.isDoing.searchEngine.hbase.items.maxSize","100"));
        logger.info("Duplicate Checker Settings:\n" +
                "itemsTableName : " + itemsTableName +
                "\nitemsColumnFamily : " + itemsColumnFamily);

        try {
            table = connection.getTable(itemsTableName);
        } catch (IOException e) {
            logger.error("Get table of HBase connection failed: ", e);
            throw new IllegalStateException(e);
        }

        cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .build(key -> {
                    Get get = new Get(Bytes.toBytes(key));
                    return table.get(get).isEmpty() ? null : Boolean.TRUE;
                });

        logger.info("ItemDAO Created With Settings");
    }

    @Override
    public boolean checkItemExists(Item item) throws Exception {
        return cache.get(item.getLink().toExternalForm()) != null;

    }

    @Override
    public void insertItem(Item item) throws Exception {
        cache.put(item.getLink().toExternalForm(), Boolean.TRUE);
        queue.put(item);
    }

}
