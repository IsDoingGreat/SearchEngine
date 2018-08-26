package in.nimbo.isDoing.searchEngine.crawler.persister.db;

import org.junit.Test;

public class HBaseDBPersisterTest {

    @Test
    public void persist() throws Exception {
        return;
        /*

        Engine.start(new ConsoleOutput(), new Configs() {
            @Override
            public String get(String key) {
                switch (key) {
                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.crawledLink.tableName":
                        return "crawledLink";

                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.crawledLink.columnFamily":
                        return "partition";

                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.crawledLink.qualifier":
                        return "number";


                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.pages.tableName":
                        return "pages";

                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.pages.columnFamily":
                        return "data";

                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.pages.qualifiers":
                        return "link;context";


                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.backLinks.tableName":
                        return "backLinks";

                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.backLinks.columnFamily":
                        return "links";


                    case "in.nimbo.isDoing.searchEngine.hbase.site":
                        return "in.nimbo.isDoing.searchEngine.hbase-site.xml";

                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.flushNumberLimit":
                        return "150";
                }
                throw new RuntimeException();
            }

            @Override
            public String get(String key, String value) {
                switch (key) {
                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.crawledLink.tableName":
                        return "crawledLink";

                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.crawledLink.columnFamily":
                        return "partition";

                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.crawledLink.qualifier":
                        return "number";


                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.pages.tableName":
                        return "pages";

                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.pages.columnFamily":
                        return "data";

                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.pages.qualifiers":
                        return "link;context";


                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.backLinks.tableName":
                        return "backLinks";

                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.backLinks.columnFamily":
                        return "links";


                    case "in.nimbo.isDoing.searchEngine.hbase.site":
                        return "in.nimbo.isDoing.searchEngine.hbase-site.xml";

                    case "crawler.persister.db.in.nimbo.isDoing.searchEngine.hbase.flushNumberLimit":
                        return "150";
                }
                throw new RuntimeException();
            }
        });

        Page page = new Page() {
            @Override
            public void parse() {
            }

            @Override
            public String getBody() {
                return "testBody";
            }

            @Override
            public URL getUrl() {
                try {
                    return new URL("https://en.wikipedia.org/wiki/Main_Page");
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public String getExtractedText() throws Exception {
                return null;
            }

            @Override
            public String getText() {
                return null;
            }

            @Override
            public String getTitle() {
                return null;
            }

            @Override
            public String getDescription() {
                return null;
            }

            @Override
            public Set<String> getOutgoingUrls() {
                Set<String> set = new HashSet<>();
                set.add("https://en.wikipedia.org/wiki/Portal:Contents");
                set.add("https://en.wikipedia.org/wiki/Portal:Featured_content");
                return set;
            }

            @Override
            public String getLang() {
                return null;
            }
        };

        String testRowKey = "en.wikipedia.org/2ce2b0dc52e7b0de9231e007acb32eb4";
        String testPageLink = "https://en.wikipedia.org/wiki/Main_Page";
        String testPageBody = "testBody";

        String testBackLink1 = "https://en.wikipedia.org/wiki/Portal:Contents";
        String testBackLink2 = "https://en.wikipedia.org/wiki/Portal:Featured_content";

        HBaseItemPersister hBaseDBPersister = new HBaseItemPersister();
        hBaseDBPersister.persist(page);
        hBaseDBPersister.flush();

        Cell[] getPageCell = get("pages", "data", "en.wikipedia.org/2ce2b0dc52e7b0de9231e007acb32eb4");
        Cell[] getBackLinksCell = get("backLinks", "links", testRowKey);

        Assert.assertEquals(testPageLink, Bytes.toString(CellUtil.cloneValue(getPageCell[1])));
        Assert.assertEquals(testPageBody, Bytes.toString(CellUtil.cloneValue(getPageCell[0])));

        Assert.assertEquals(testBackLink1, Bytes.toString(CellUtil.cloneValue(getBackLinksCell[1])));
        Assert.assertEquals(testBackLink2, Bytes.toString(CellUtil.cloneValue(getBackLinksCell[0])));
    }

    public Cell[] get(String tableName, String columnFamily, String rowKey) {
        TableName tn = TableName.valueOf(tableName);
        Cell[] res = null;
        try {
            Table table = HBaseClient.getConnection().getTable(tn);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(Bytes.toBytes(columnFamily));
            Result result = table.get(get);
            res = result.rawCells();
            table.stop();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
        */
    }

    @Test
    public void flush() {
    }
}