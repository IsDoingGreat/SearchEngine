package in.nimbo.isDoing.searchEngine.newsReader.model;

import java.net.URL;
import java.util.Date;
import java.util.Objects;

public class Channel {
    private String category;
    private String name;
    private URL rssLink;
    private String link;
    private long lastUpdate;

    public Channel(String category, String name, URL rssLink) {
        this(category, name, rssLink, new Date().getTime());
    }

    public Channel(String category, String name, URL RssLink, long lastUpdate) {
        this(category, name, RssLink, lastUpdate, RssLink.getHost());
    }


    public Channel(String category, String name, URL rssLink, long lastUpdate, String link) {
        this.category = category;
        this.name = name;
        this.rssLink = rssLink;
        this.link = link;
        this.lastUpdate = lastUpdate;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
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
                "category='" + category + "\'" +
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