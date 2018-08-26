package in.nimbo.isDoing.searchEngine.news_reader.model;

import java.net.URL;
import java.util.Date;
import java.util.Objects;

public class Channel {
    private Integer id;
    private String name;
    private URL rssLink;
    private String link;
    private long lastUpdate;

    public Channel(String name, URL rssLink) {
        this(name, rssLink, new Date().getTime());
    }

    public Channel(String name, URL RssLink, long lastUpdate) {
        this(name, RssLink, lastUpdate, RssLink.getHost());
    }


    public Channel(String name, URL rssLink, long lastUpdate, String link) {
        this(null, name, rssLink, lastUpdate, link);
    }

    public Channel(Integer id, String name, URL rssLink, long lastUpdate, String link) {
        this.id = id;
        this.name = name;
        this.rssLink = rssLink;
        this.link = link;
        this.lastUpdate = lastUpdate;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public URL getRssLink() {
        return rssLink;
    }

    public void setRssLink(URL rssLink) {
        this.rssLink = rssLink;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Channel channel = (Channel) o;
        return Objects.equals(rssLink, channel.rssLink);
    }

    @Override
    public String toString() {
        return "Channel{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", rssLink=" + rssLink +
                ", link='" + link + '\'' +
                ", lastUpdate=" + lastUpdate +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(rssLink);
    }
}