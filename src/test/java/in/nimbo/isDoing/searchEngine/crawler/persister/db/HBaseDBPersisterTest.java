package in.nimbo.isDoing.searchEngine.crawler.persister.db;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Configs;
import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class HBaseDBPersisterTest {

    @Test
    public void persist() throws Exception {

        Engine.start(new ConsoleOutput(), new Configs() {
            @Override
            public String get(String key) {
                switch (key) {
                    case "crawler.persister.db.hbase.crawledLink.tableName":
                        return "crawledLink";

                    case "crawler.persister.db.hbase.crawledLink.columnFamily":
                        return "partition";

                    case "crawler.persister.db.hbase.crawledLink.qualifier":
                        return "number";


                    case "crawler.persister.db.hbase.pages.tableName":
                        return "pages";

                    case "crawler.persister.db.hbase.pages.columnFamily":
                        return "data";

                    case "crawler.persister.db.hbase.pages.qualifiers":
                        return "link;context";


                    case "crawler.persister.db.hbase.backLinks.tableName":
                        return "backLinks";

                    case "crawler.persister.db.hbase.backLinks.columnFamily":
                        return "links";


                    case "hbase.site":
                        return "hbase-site.xml";

                    case "crawler.persister.db.hbase.flushNumberLimit":
                        return "150";
                }
                throw new RuntimeException();
            }

            @Override
            public String get(String key, String value) {
                switch (key) {
                    case "crawler.persister.db.hbase.crawledLink.tableName":
                        return "crawledLink";

                    case "crawler.persister.db.hbase.crawledLink.columnFamily":
                        return "partition";

                    case "crawler.persister.db.hbase.crawledLink.qualifier":
                        return "number";


                    case "crawler.persister.db.hbase.pages.tableName":
                        return "pages";

                    case "crawler.persister.db.hbase.pages.columnFamily":
                        return "data";

                    case "crawler.persister.db.hbase.pages.qualifiers":
                        return "link;context";


                    case "crawler.persister.db.hbase.backLinks.tableName":
                        return "backLinks";

                    case "crawler.persister.db.hbase.backLinks.columnFamily":
                        return "links";


                    case "hbase.site":
                        return "hbase-site.xml";

                    case "crawler.persister.db.hbase.flushNumberLimit":
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

        HBaseDBPersister hBaseDBPersister = new HBaseDBPersister();
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
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    @Test
    public void flush() {
    }
}