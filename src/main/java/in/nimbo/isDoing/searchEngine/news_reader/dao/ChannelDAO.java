package in.nimbo.isDoing.searchEngine.news_reader.dao;

import in.nimbo.isDoing.searchEngine.news_reader.model.Channel;

import java.net.URL;
import java.util.List;

public interface ChannelDAO {
    void insertChannel(Channel channel) throws Exception;

    Channel getChannel(URL rssLink);

    void updateChannelLastDate(Channel channel) throws Exception;


    List<Channel> getChannelsUpdatedBefore(int minutes) throws Exception;

    void stop();
}
