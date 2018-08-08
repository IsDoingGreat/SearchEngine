package in.nimbo.isDoing.searchEngine.crawler.page;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LanguageDetector {
    private final static Logger logger = LoggerFactory.getLogger(LanguageDetector.class);
    private final static Object MUTEX = new Object();
    private static volatile LanguageDetector instance;
    private com.optimaize.langdetect.LanguageDetector languageDetector;
    private TextObjectFactory textObjectFactory;

    private LanguageDetector() {
        logger.info("starting language detector...");
        Engine.getOutput().show("starting language detector...");
        List<LanguageProfile> languageProfiles;
        try {
            languageProfiles = new LanguageProfileReader().readAllBuiltIn();
            languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                    .withProfiles(languageProfiles)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
        logger.info("language detector started");
    }

    public static LanguageDetector getInstance() {
        if (instance == null) {
            synchronized (MUTEX) {
                if (instance == null) {
                    logger.info("Starting Language Detector...");
                    Engine.getOutput().show("Starting Language Detector...");
                    instance = new LanguageDetector();
                }
            }
        }
        return instance;
    }

    /**
     * @param text text to detect language
     * @return Language of Text
     * @throws LanguageNotDetected if language is not detected
     */
    public String detectLanguage(String text) {
        TextObject textObject = textObjectFactory.forText(text);
        Optional<LdLocale> language = languageDetector.detect(textObject);
        if (language.isPresent())
            return language.get().getLanguage();
        else
            throw new LanguageNotDetected();
    }

    private class LanguageNotDetected extends RuntimeException {
    }
}
