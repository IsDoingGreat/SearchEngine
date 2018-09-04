package in.nimbo.isDoing.searchEngine.newsReader.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import static com.codahale.metrics.MetricRegistry.name;

public class JmxCounter {
    public static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final Counter successfulItemsOfHBasePersister =
            metricRegistry.counter(name(Counter.class, "persisted items in HBase"));
    private static final Counter successfulItemsOfElasticPersister =
            metricRegistry.counter(name(Counter.class, "persisted items in Elastichsearch"));
    private static final Counter invalidTexts =
            metricRegistry.counter(name(Counter.class, "Null pages"));

    public static void increamentSuccessfulItemsOfHBasePersister(int number) {
        successfulItemsOfHBasePersister.inc(number);
    }

    public static void increamentSuccessfulItemsOfHBasePersister() {
        successfulItemsOfHBasePersister.inc();
    }


    public static void setSuccessfulItemsOfElasticPersister(int number) {
        successfulItemsOfElasticPersister.inc(number);
    }

    public static void getInvalidTexts() {
        successfulItemsOfElasticPersister.inc();
    }

}
