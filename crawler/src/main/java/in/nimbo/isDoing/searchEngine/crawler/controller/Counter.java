package in.nimbo.isDoing.searchEngine.crawler.controller;

import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.engine.interfaces.HaveStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class Counter implements Runnable, HaveStatus {
    private static final Logger logger = LoggerFactory.getLogger(Counter.class.getSimpleName());

    private AtomicInteger total = new AtomicInteger(0);
    private AtomicInteger LRURejected = new AtomicInteger(0);
    private AtomicInteger duplicate = new AtomicInteger(0);
    private AtomicInteger invalidLang = new AtomicInteger(0);
    private AtomicInteger fetcherError = new AtomicInteger(0);
    private AtomicInteger successful = new AtomicInteger(0);
    private AtomicInteger persisted = new AtomicInteger(0);


    private int totalLast;
    private int LRURejectedLast;
    private int duplicateLast;
    private int invalidLangLast;
    private int fetcherErrorLast;
    private int successfulLast;
    private int persistedLast;

    public void increment(States state) {
        switch (state) {
            case TOTAL:
                total.incrementAndGet();
                break;
            case DUPLICATE:
                total.incrementAndGet();
                duplicate.incrementAndGet();
                break;
            case SUCCESSFUL:
                total.incrementAndGet();
                successful.incrementAndGet();
                break;
            case INVALID_LANG:
                total.incrementAndGet();
                invalidLang.incrementAndGet();
                break;
            case FETCHER_ERROR:
                total.incrementAndGet();
                fetcherError.incrementAndGet();
                break;
            case LRU_REJECTED:
                total.incrementAndGet();
                LRURejected.incrementAndGet();
                break;
            case PERSISTED:
                persisted.incrementAndGet();
                break;
        }
    }

    public int get(States state) {
        switch (state) {
            case TOTAL:
                return total.get();
            case DUPLICATE:
                return duplicate.get();
            case SUCCESSFUL:
                return successful.get();
            case INVALID_LANG:
                return invalidLang.get();
            case FETCHER_ERROR:
                return fetcherError.get();
            case LRU_REJECTED:
                return LRURejected.get();
            case PERSISTED:
                return persisted.get();
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
                        "\ttotal= " + (total.get() - totalLast) + "\n" +
                        "\tLRURejected= " + (LRURejected.get() - LRURejectedLast) + "\n" +
                        "\tduplicate= " + (duplicate.get() - duplicateLast) + "\n" +
                        "\tinvalidLang= " + (invalidLang.get() - invalidLangLast) + "\n" +
                        "\tfetcherError= " + (fetcherError.get() - fetcherErrorLast) + "\n" +
                        "\tsuccessful= " + (successful.get() - successfulLast) + "\n" +
                        "\tpersisted= " + (persisted.get() - persistedLast) + "\n"
                );
                totalLast = total.get();
                LRURejectedLast = LRURejected.get();
                duplicateLast = duplicate.get();
                invalidLangLast = invalidLang.get();
                fetcherErrorLast = fetcherError.get();
                successfulLast = successful.get();
                persistedLast = persisted.get();
            }

        } catch (InterruptedException e) {
            logger.info("counterThread stopped");
        }
    }

    @Override
    public Status status() {
        Status status = new Status("Counter", "");

        status.addLine("Links TOTAL :" + this.get(Counter.States.TOTAL));
        status.addLine("Links DUPLICATE :" + this.get(Counter.States.DUPLICATE));
        status.addLine("Links FETCHER_ERROR :" + this.get(Counter.States.FETCHER_ERROR));
        status.addLine("Links LRU_REJECTED :" + this.get(Counter.States.LRU_REJECTED));
        status.addLine("Links INVALID_LANG :" + this.get(Counter.States.INVALID_LANG));
        status.addLine("Links SUCCESSFUL :" + this.get(Counter.States.SUCCESSFUL));
        status.addLine("Links PERSISTED :" + this.get(Counter.States.PERSISTED));
        return status;
    }


    public enum States {
        TOTAL, LRU_REJECTED, DUPLICATE, INVALID_LANG, FETCHER_ERROR, SUCCESSFUL, PERSISTED
    }
}
