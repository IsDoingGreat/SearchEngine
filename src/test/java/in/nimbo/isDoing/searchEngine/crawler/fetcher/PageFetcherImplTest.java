package in.nimbo.isDoing.searchEngine.crawler.fetcher;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PageFetcherImplTest {
    @Before
    public void Setup() throws Exception {
        Engine.start(new Output() {
            @Override
            public void show(String object) {
            }

            @Override
            public void show(Type type, String object) {
            }

            @Override
            public void show(Status status) {
            }
        });
    }

    @After
    public void Shutdown() {
        Engine.shutdown();
    }

    @Test
    public void fetchTest() throws Exception {
        PageFetcher fetcher = new PageFetcherImpl();
        Page page = fetcher.fetch(new URL("http://example.com/"));
        page.parse();
        assertEquals(fetchTestBody(), page.getBody());
        assertEquals(new URL("http://example.com/"), page.getUrl());
        assertNull(page.getDescription());
        assertEquals("en", page.getLang());
        Set<String> outgoingUrls= new HashSet<>();
        outgoingUrls.add("http://www.iana.org/domains/example");
        assertEquals(outgoingUrls, page.getOutgoingUrls());
        assertEquals("Example Domain", page.getTitle());
        assertEquals(
                "Example Domain " +
                "This domain is established to be used for illustrative examples in documents. You may use this domain in examples without prior coordination or asking for permission. " +
                "More information...", page.getText());
    }

    private String fetchTestBody() {
        String string = "<!doctype html>\n" +
                "<html>\n" +
                "<head>\n" +
                "    <title>Example Domain</title>\n" +
                "\n" +
                "    <meta charset=\"utf-8\" />\n" +
                "    <meta http-equiv=\"Content-type\" content=\"text/html; charset=utf-8\" />\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n" +
                "    <style type=\"text/css\">\n" +
                "    body {\n" +
                "        background-color: #f0f0f2;\n" +
                "        margin: 0;\n" +
                "        padding: 0;\n" +
                "        font-family: \"Open Sans\", \"Helvetica Neue\", Helvetica, Arial, sans-serif;\n" +
                "        \n" +
                "    }\n" +
                "    div {\n" +
                "        width: 600px;\n" +
                "        margin: 5em auto;\n" +
                "        padding: 50px;\n" +
                "        background-color: #fff;\n" +
                "        border-radius: 1em;\n" +
                "    }\n" +
                "    a:link, a:visited {\n" +
                "        color: #38488f;\n" +
                "        text-decoration: none;\n" +
                "    }\n" +
                "    @media (max-width: 700px) {\n" +
                "        body {\n" +
                "            background-color: #fff;\n" +
                "        }\n" +
                "        div {\n" +
                "            width: auto;\n" +
                "            margin: 0 auto;\n" +
                "            border-radius: 0;\n" +
                "            padding: 1em;\n" +
                "        }\n" +
                "    }\n" +
                "    </style>    \n" +
                "</head>\n" +
                "\n" +
                "<body>\n" +
                "<div>\n" +
                "    <h1>Example Domain</h1>\n" +
                "    <p>This domain is established to be used for illustrative examples in documents. You may use this\n" +
                "    domain in examples without prior coordination or asking for permission.</p>\n" +
                "    <p><a href=\"http://www.iana.org/domains/example\">More information...</a></p>\n" +
                "</div>\n" +
                "</body>\n" +
                "</html>\n";
        return string;
    }
}