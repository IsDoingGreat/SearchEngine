package in.nimbo.isDoing.searchEngine.newsReader.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import static com.codahale.metrics.MetricRegistry.name;

public class JmxCounter {
    public static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final Counter successfulItemsOfHBasePersister =
            metricRegistry.counter(name(Counter.class, "Persisted items in HBase"));
    private static final Counter successfulItemsOfElasticPersister =
            metricRegistry.counter(name(Counter.class, "Persisted items in Elasticsearch"));
    private static final Counter invalidChannels =
            metricRegistry.counter(name(Counter.class, "Null channel"));
    private static final Counter invalidItems =
            metricRegistry.counter(name(Counter.class, "Null date"));
    private static final Counter duplicateItems =
            metricRegistry.counter(name(Counter.class, "Duplicate items"));

    public static void incrementSuccessfulItemsOfHBasePersister(int number) {
        successfulItemsOfHBasePersister.inc(number);
    }


    public static void incrementSuccessfulItemsOfElasticPersister(int number) {
        successfulItemsOfElasticPersister.inc(number);
    }

    public static void incrementInvalidChannels() {
        invalidChannels.inc();
    }

    public static void incrementInvalidItems() {
        invalidItems.inc();
    }

    public static void incrementDuplicateItems() {
        duplicateItems.inc();
    }
}
