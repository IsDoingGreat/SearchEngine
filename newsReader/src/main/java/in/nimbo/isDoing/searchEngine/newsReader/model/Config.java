package in.nimbo.isDoing.searchEngine.newsReader.model;

public class Config {
    private Integer id;
    private String link;
    private String bodyPattern;
    private String adPatterns;


    public Config(String link, String bodyPattern) {
        this(link, bodyPattern, null);
    }

    public Config(String link, String bodyPattern, String[] adPatterns) {
        this(null, link, bodyPattern, adPatterns);
    }

    public Config(Integer id, String link, String bodyPattern, String adPatterns) {
        this.id = id;
        this.link = link;
        this.bodyPattern = bodyPattern;
        setAdPatterns(adPatterns);
    }

    public Config(Integer id, String link, String bodyPattern, String[] adPatterns) {
        this.id = id;
        this.link = link;
        this.bodyPattern = bodyPattern;
        setAdPatterns(adPatterns);
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getBodyPattern() {
        return bodyPattern;
    }

    public void setBodyPattern(String bodyPattern) {
        this.bodyPattern = bodyPattern;
    }

    public String[] getAdPatterns() {
        if (adPatterns == null || adPatterns.isEmpty())
            return new String[0];

        return adPatterns.split(";");

    }

    public String getAdPatternsString(){
        return adPatterns;
    }

    public void setAdPatterns(String[] adPatterns) {
        if (adPatterns == null || adPatterns.length == 0)
            this.adPatterns = null;
        else
            this.adPatterns = String.join(";", adPatterns);
    }

    public void setAdPatterns(String adPatterns) {
        if (adPatterns == null || adPatterns.isEmpty())
            this.adPatterns = null;
        else
            this.adPatterns = adPatterns;
    }


}
