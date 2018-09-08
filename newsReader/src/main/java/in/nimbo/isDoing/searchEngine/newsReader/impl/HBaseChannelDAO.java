package in.nimbo.isDoing.searchEngine.newsReader.impl;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import in.nimbo.isDoing.searchEngine.newsReader.dao.ChannelDAO;
import in.nimbo.isDoing.searchEngine.newsReader.model.Channel;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private List<String> invalidChannels;
    private String initSource;

    public HBaseChannelDAO() throws IOException {
        Engine.getOutput().show("Creating CaffeineDuplicateChecker...");
        logger.info("Creating HBaseChannelDAO...");

        cache = new ConcurrentHashMap<>();
        invalidChannels = new ArrayList<>();

        connection = HBaseClient.getConnection();

        channelsTableName = TableName.valueOf(Engine.getConfigs().get("newsReader.persister.db.hbase.channels.tableName"));
        channelsColumnFamily = Engine.getConfigs().get("newsReader.persister.db.hbase.channels.columnFamily");
        logger.info("HBaseChannelDAO Settings:\n" +
                "channelsTableName : " + channelsTableName +
                "\nchannelsColumnFamily : " + channelsColumnFamily);

        try {
            table = connection.getTable(channelsTableName);
        } catch (IOException e) {
            logger.error("Get table of HBase connection failed: ", e);
            throw new IllegalStateException(e);
        }

        initSource = Engine.getConfigs().get("newsReader.persister.db.hbase.channels.initSource").toLowerCase();
        if (initSource.equals("file")) {
            loadFromFile();
        } else if (initSource.equals("hbase")) {
            loadFromHBase();
        } else {
            throw new IllegalStateException("Source of seen not valid");
        }

        logger.info("HBaseChannelDAO Created");
    }

    private void loadFromHBase() {
        try {
            Scan scan = new Scan();
            int loaded = 0;

            byte[] channelsColumnFamilyBytes = Bytes.toBytes(channelsColumnFamily);
            for (Result res : table.getScanner(scan)) {
                String rssLink = Bytes.toString(res.getRow());
                String category = Bytes.toString(res.getValue(channelsColumnFamilyBytes, Bytes.toBytes("category")));
                String name = Bytes.toString(res.getValue(channelsColumnFamilyBytes, Bytes.toBytes("name")));
                long lastUpdate = Bytes.toLong(res.getValue(channelsColumnFamilyBytes, Bytes.toBytes("lastUpdate")));
                cache.put(rssLink, new Channel(category, name, new URL(rssLink), lastUpdate));
                loaded++;
                if (loaded % 10 == 0) {
                    Engine.getOutput().show(loaded + " Cache Entries loaded!");
                }
            }
            Engine.getOutput().show(loaded + " Cache Entries loaded!");

        } catch (IOException e) {
            logger.error("ERROR DURING LOADING CACHE FROM HBASE", e);
            throw new IllegalStateException(e);
        }
    }

    private void loadFromFile() throws IOException {
        //file entry format {link;name;lastUpdate;category}

        Path seed = Paths.get("./newsSeeds.txt").toAbsolutePath();
        if (!Files.exists(seed))
            throw new IllegalStateException("Seed Not Exists");

        List<String> list = Files.readAllLines(seed);
        for (String line : list) {
            String[] tokens = line.split(";");
            if (tokens.length != 4) {
                Engine.getOutput().show(Output.Type.ERROR, "Not Valid " + tokens);
                continue;
            }

            try {
                Engine.getOutput().show(Output.Type.INFO, tokens[0] + " exist in invalidChannels : " + String.valueOf(invalidChannels.contains(tokens[0])));

                if (cache.get(tokens[0]) == null && !invalidChannels.contains(tokens[0])) {
                    cache.put(tokens[0], new Channel(tokens[3], tokens[1], new URL(tokens[0]), Long.valueOf(tokens[2])));
                }
            } catch (Exception e) {
                Engine.getOutput().show(Output.Type.ERROR, "Not Valid " + e.getMessage());
            }
        }

    }

    @Override
    public void insertChannel(Channel channel) {
        cache.put(channel.getRssLink().toExternalForm(), channel);
        try {
            Put put = new Put(Bytes.toBytes(channel.getRssLink().toExternalForm()));
            put.addColumn(Bytes.toBytes(channelsColumnFamily), Bytes.toBytes("category"), Bytes.toBytes(channel.getCategory()));
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
            if (((now - channel.getLastUpdate()) / (1000.0 * 60.0)) > minutes)
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
                put.addColumn(Bytes.toBytes(channelsColumnFamily), Bytes.toBytes("category"), Bytes.toBytes(entry.getValue().getCategory()));
                put.addColumn(Bytes.toBytes(channelsColumnFamily), Bytes.toBytes("name"), Bytes.toBytes(entry.getValue().getName()));
                put.addColumn(Bytes.toBytes(channelsColumnFamily), Bytes.toBytes("lastUpdate"), Bytes.toBytes(entry.getValue().getLastUpdate()));
                puts.add(put);
            }
            table.put(puts);
            table.close();
        } catch (Exception e) {
            logger.error("Failed to stop channelDAO", e);
        }
    }

    @Override
    public void reload() {
        try {
            if (initSource.equals("file")) {
                loadFromFile();
            } else if (initSource.equals("hbase")) {
                loadFromHBase();
            }
        } catch (IOException e) {
            logger.error("Failed to reload seed file ", e);
            Engine.getOutput().show(Output.Type.ERROR, "Failed to reload seed file " + e.getMessage());
        }
    }

    @Override
    public void removeChannel(Channel channel) {
        if (cache.containsKey(channel.getRssLink().toExternalForm())) {
            cache.remove(channel.getRssLink().toExternalForm());
        }
    }

    @Override
    public void insertInvalidChannel(Channel channel) {
        if (!invalidChannels.contains(channel.getRssLink().toExternalForm())) {
            Engine.getOutput().show(Output.Type.INFO, "Invalid channel : " + channel.getRssLink().toExternalForm());
            invalidChannels.add(channel.getRssLink().toExternalForm());
        }
        removeChannel(channel);
    }
}
