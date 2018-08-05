package in.nimbo.isDoing.searchEngine.engine;

import in.nimbo.isDoing.searchEngine.crawler.CrawlerService;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Configs;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import in.nimbo.isDoing.searchEngine.pipeline.Output;

import java.util.Objects;

public class Engine {
    private static volatile Engine instance;
    private Output output;
    private Configs configs;

    Engine(Output output, Configs config) {
        this.output = output;
        this.configs = config;
    }

    public synchronized static Engine start(Output out) throws Exception {
        return start(out, new SystemConfigs());
    }

    public synchronized static Engine start(Output out, Configs configs) throws Exception {
        if (instance != null)
            throw new RuntimeException("Engine has already started");

        instance = new Engine(out, configs);
        return instance;
    }

    public static Configs getConfigs() {
        Objects.requireNonNull(instance, "Engine Not Started");
        return instance.configs;
    }

    public static Output getOutput() {
        Objects.requireNonNull(instance, "Engine Not Started");
        return instance.output;
    }

    public void startService(String name) {
        if (name.equals("crawler"))
            startService(new CrawlerService());
        else
            output.show(Output.Type.ERROR, "Service Not Found");
    }

    public void startService(Service service) {
        service.start();
    }
}
