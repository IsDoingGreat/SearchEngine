package in.nimbo.isDoing.searchEngine.crawler.page;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class WebPage implements Page {
    private final static Logger logger = LoggerFactory.getLogger(WebPage.class);
    private static LanguageDetector languageDetector;
    private static TextObjectFactory textObjectFactory;
    private String body;
    private Document document;
    private URL url;

    public WebPage(String body, URL url) {
        this.body = body;
        this.url = url;
    }

    @Override
    public String toString() {
        parse();
        return "\nTitle=" + getTitle() + "\nDescription=" + getDescription() + "\n";
    }

    @Override
    public void parse() {
        if (document == null) {
            document = Jsoup.parse(body, url.toExternalForm());
        }
    }

    @Override
    public String getBody() {
        return body;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public String getExtractedText() throws BoilerpipeProcessingException {
        Objects.requireNonNull(document);
        return ArticleExtractor.INSTANCE.getText(document.outerHtml());
    }

    @Override
    public String getText() {
        Objects.requireNonNull(document);
        Element body = document.body();
        if (body != null)
            return body.text();
        return "";
    }

    @Override
    public String getTitle() {
        Objects.requireNonNull(document);
        return document.title();
    }

    @Override
    public String getDescription() {
        Objects.requireNonNull(document);
        Elements metaTags = document.getElementsByTag("meta");
        for (Element metaTag : metaTags) {
            String content = metaTag.attr("content");
            String name = metaTag.attr("name");

            if ("description".equals(name)) {
                return content;
            }
        }
        return null;
    }

    @Override
    public Set<String> getOutgoingUrls() {
        Objects.requireNonNull(document);
        Set<String> urls = new HashSet<>();
        Elements links = document.select("a[href]");
        String externalForm = this.url.toExternalForm();
        for (Element link : links) {
            String url = link.attr("abs:href");

            if (url.equals(externalForm))
                continue;

            urls.add(url);
        }

        return urls;
    }

    @Override
    public String getLang() {
        if (languageDetector == null) {
            List<LanguageProfile> languageProfiles;
            try {
                languageProfiles = new LanguageProfileReader().readAllBuiltIn();
                languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                        .withProfiles(languageProfiles)
                        .build();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (textObjectFactory == null) {
            textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
        }

        TextObject textObject = textObjectFactory.forText(getText());
        Optional<LdLocale> language = languageDetector.detect(textObject);
        if (language.isPresent())
            return language.get().getLanguage();
        else
            throw new LanguageNotDetected();
    }

    private class LanguageNotDetected extends RuntimeException {
    }
}
