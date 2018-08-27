package in.nimbo.isDoing.searchEngine.news_reader.impl;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import in.nimbo.isDoing.searchEngine.news_reader.dao.ChannelDAO;
import in.nimbo.isDoing.searchEngine.news_reader.model.Channel;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseChannelDAO implements ChannelDAO {
    private static final Logger logger = LoggerFactory.getLogger(HBaseChannelDAO.class);
    private final Table table;
    private Connection connection;
    private TableName channelsTableName;
    private String channelsColumnFamily;
    private ConcurrentHashMap<String, Channel> cache;

    public HBaseChannelDAO() {
        Engine.getOutput().show("Creating CaffeineDuplicateChecker...");
        cache = new ConcurrentHashMap<>();
        logger.info("Creating CaffeineDuplicateChecker...");

        connection = HBaseClient.getConnection();

        channelsTableName = TableName.valueOf(Engine.getConfigs().get("newsReader.persister.db.hbase.channels.tableName"));
        channelsColumnFamily = Engine.getConfigs().get("newsReader.persister.db.hbase.channels.columnFamily");
        logger.info("Duplicate Checker Settings:\n" +
                "channelsTableName : " + channelsTableName +
                "\nchannelsColumnFamily : " + channelsColumnFamily);

        try {
            table = connection.getTable(channelsTableName);
        } catch (IOException e) {
            logger.error("Get table of HBase connection failed: ", e);
            throw new IllegalStateException(e);
        }

        loadFromHbase();

        logger.info("ChannelDAO Created");
    }

    private void loadFromHbase() {
        try {
            Scan scan = new Scan();
            int loaded = 0;

            byte[] channelsColumnFamilyBytes = Bytes.toBytes(channelsColumnFamily);
            for (Result res : table.getScanner(scan)) {
                String rssLink = Bytes.toString(res.getRow());
                String name = Bytes.toString(res.getValue(channelsColumnFamilyBytes, Bytes.toBytes("name")));
                long lastUpdate = Bytes.toLong(res.getValue(channelsColumnFamilyBytes, Bytes.toBytes("lastUpdate")));
//                long lastUpdate = 0;
                cache.put(rssLink, new Channel(name, new URL(rssLink), lastUpdate));
                loaded++;
                if (loaded % 10 == 0) {
                    Engine.getOutput().show(loaded + " Cache Entries loaded!");
                }
            }
            Engine.getOutput().show(cache.toString());
            Engine.getOutput().show(loaded + " Cache Entries loaded!");

        } catch (IOException e) {
            logger.error("ERROR DURING LOADING CACHE FROM HBASE", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void insertChannel(Channel channel) {
        cache.put(channel.getRssLink().toExternalForm(), channel);
        try {
            Put put = new Put(Bytes.toBytes(channel.getRssLink().toExternalForm()));
            put.addColumn(Bytes.toBytes(channelsColumnFamily), Bytes.toBytes("name"), Bytes.toBytes(channel.getName()));
            put.addColumn(Bytes.toBytes(channelsColumnFamily), Bytes.toBytes("lastUpdate"), Bytes.toBytes(channel.getLastUpdate()));
            table.put(put);
        } catch (IOException e) {
            logger.error("Persist RSS failed: ", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Channel getChannel(URL rssLink) {
        return cache.get(rssLink.toExternalForm());
    }

    @Override
    public void updateChannelLastDate(Channel channel) {
        Channel mainChannel = cache.get(channel.getRssLink().toExternalForm());
        if (mainChannel == null)
            throw new IllegalStateException("Channel Does Not Exists");
        mainChannel.setLastUpdate(channel.getLastUpdate());
    }


    @Override
    public List<Channel> getChannelsUpdatedBefore(int minutes) {
        List<Channel> channels = new ArrayList<>();
        long now = new Date().getTime();
        for (Channel channel : cache.values()) {
            if (((now - channel.getLastUpdate()) / 60) > minutes)
                channels.add(channel);
        }
        return channels;
    }

    @Override
    public void stop() {
        try {
            List<Put> puts = new ArrayList<>(cache.size());
            for (Map.Entry<String, Channel> entry : cache.entrySet()) {
                Put put = new Put(Bytes.toBytes(entry.getValue().getRssLink().toExternalForm()));
                put.addColumn(Bytes.toBytes(channelsColumnFamily), Bytes.toBytes("name"), Bytes.toBytes(entry.getValue().getName()));
                put.addColumn(Bytes.toBytes(channelsColumnFamily), Bytes.toBytes("lastUpdate"), Bytes.toBytes(entry.getValue().getLastUpdate()));
                puts.add(put);
            }
            table.put(puts);
            table.close();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
}
