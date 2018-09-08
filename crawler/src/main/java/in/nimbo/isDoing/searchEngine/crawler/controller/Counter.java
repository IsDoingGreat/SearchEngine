package in.nimbo.isDoing.searchEngine.crawler.controller;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Stateful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class Counter implements Runnable, Stateful {
    private static final Logger logger = LoggerFactory.getLogger(Counter.class.getSimpleName());

    private final Meter totalMetric = SharedMetricRegistries.getDefault().meter("totalMetric");
    private Meter LRURejectedMetric = SharedMetricRegistries.getDefault().meter("LRURejectedMetric");
    private Meter duplicateMetric = SharedMetricRegistries.getDefault().meter("duplicateMetric");
    private Meter invalidLangMetric = SharedMetricRegistries.getDefault().meter("invalidLangMetric");
    private Meter fetcherErrorMetric = SharedMetricRegistries.getDefault().meter("fetcherErrorMetric");
    private Meter successfulMetric = SharedMetricRegistries.getDefault().meter("successfulMetric");
    private Meter persistedMetric = SharedMetricRegistries.getDefault().meter("persistedMetric");

    private long totalLast;
    private long LRURejectedLast;
    private long duplicateLast;
    private long invalidLangLast;
    private long fetcherErrorLast;
    private long successfulLast;
    private long persistedLast;


    public void increment(States state) {
        switch (state) {
            case TOTAL:
                totalMetric.mark();
                break;
            case DUPLICATE:
                totalMetric.mark();
                duplicateMetric.mark();
                break;
            case SUCCESSFUL:
                totalMetric.mark();
                successfulMetric.mark();
                break;
            case INVALID_LANG:
                totalMetric.mark();
                invalidLangMetric.mark();
                break;
            case FETCHER_ERROR:
                totalMetric.mark();
                fetcherErrorMetric.mark();
                break;
            case LRU_REJECTED:
                totalMetric.mark();
                LRURejectedMetric.mark();
                break;
            case PERSISTED:
                persistedMetric.mark();
                break;
        }
    }

    public long get(States state) {
        switch (state) {
            case TOTAL:
                return totalMetric.getCount();
            case DUPLICATE:
                return duplicateMetric.getCount();
            case SUCCESSFUL:
                return successfulMetric.getCount();
            case INVALID_LANG:
                return invalidLangMetric.getCount();
            case FETCHER_ERROR:
                return fetcherErrorMetric.getCount();
            case LRU_REJECTED:
                return LRURejectedMetric.getCount();
            case PERSISTED:
                return persistedMetric.getCount();
            default:
                return 0;
        }
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                Thread.sleep(1000);

                logger.info("Crawled Per Second:\n" +
                        "\ttotal= " + (totalMetric.getCount() - totalLast) + "\n" +
                        "\tLRURejected= " + (LRURejectedMetric.getCount() - LRURejectedLast) + "\n" +
                        "\tduplicate= " + (duplicateMetric.getCount() - duplicateLast) + "\n" +
                        "\tinvalidLang= " + (invalidLangMetric.getCount() - invalidLangLast) + "\n" +
                        "\tfetcherError= " + (fetcherErrorMetric.getCount() - fetcherErrorLast) + "\n" +
                        "\tsuccessful= " + (successfulMetric.getCount() - successfulLast) + "\n" +
                        "\tpersisted= " + (persistedMetric.getCount() - persistedLast) + "\n"
                );
                totalLast = totalMetric.getCount();
                LRURejectedLast = LRURejectedMetric.getCount();
                duplicateLast = duplicateMetric.getCount();
                invalidLangLast = invalidLangMetric.getCount();
                fetcherErrorLast = fetcherErrorMetric.getCount();
                successfulLast = successfulMetric.getCount();
                persistedLast = persistedMetric.getCount();
            }

        } catch (InterruptedException e) {
            logger.info("counterThread stopped");
        }
    }

    @Override
    public Map<String, Object> status() {
        Map<String, Object> map = new HashMap<>();

        map.put("total:", this.get(Counter.States.TOTAL));
        map.put("duplicate:", this.get(Counter.States.DUPLICATE));
        map.put("fetcher_error:", this.get(Counter.States.FETCHER_ERROR));
        map.put("lru_rejected:", this.get(Counter.States.LRU_REJECTED));
        map.put("invalid_lang:", this.get(Counter.States.INVALID_LANG));
        map.put("successful:", this.get(Counter.States.SUCCESSFUL));
        map.put("persisted:", this.get(Counter.States.PERSISTED));
        return map;
    }


    public enum States {
        TOTAL, LRU_REJECTED, DUPLICATE, INVALID_LANG, FETCHER_ERROR, SUCCESSFUL, PERSISTED
    }
}
