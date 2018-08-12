package in.nimbo.isDoing.searchEngine.crawler.duplicate_checker;

import org.junit.Test;

public class CaffeineDuplicateCheckerTest {

    @Test
    public void checkDuplicateAndSet() throws Exception  {
        return;
        /*
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

        URL url = new URL("https://www.tutorialspoint.com/hbase/hbase_delete_data.htm");

        DuplicateChecker duplicateChecker = new CaffeineDuplicateChecker();
        Assert.assertFalse(duplicateChecker.checkDuplicateAndSet(url));
        Assert.assertTrue(duplicateChecker.checkDuplicateAndSet(url));*/

    }

    @Test
    public void stop() {

    }
}