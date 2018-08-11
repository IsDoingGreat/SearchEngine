package in.nimbo.isDoing.searchEngine.engine;

import in.nimbo.isDoing.searchEngine.crawler.CrawlerService;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Configs;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import in.nimbo.isDoing.searchEngine.twitter_reader.TwitterReaderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Engine {
    private static final Logger logger = LoggerFactory.getLogger(Engine.class.getSimpleName());
    private static volatile Engine instance;
    private Output output;
    private Configs configs;
    private Map<String, Service> services = new HashMap<>();

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
        try {
            switch (name) {
                case "crawler":
                    startService(new CrawlerService());
                    break;
                case "twitterReader":
                    startService(new TwitterReaderService());
                    break;
                default:
                    output.show(Output.Type.ERROR, "Service Not Found");
                    break;
            }

        } catch (Exception e) {
            output.show(Output.Type.ERROR, e.toString());
        }
    }

    public void startService(Service service) {
        try {
            if (services.get(service.getName()) == null) {
                service.start();
                services.put(service.getName(), service);
            } else
                output.show(Output.Type.ERROR, "service Already Running");
        } catch (Exception e) {
            logger.error("Error During starting Service.", e);
            output.show(Output.Type.ERROR, e.toString());
            service.stop();
        }

    }

    public void stopService(String serviceName) {
        if (services.get(serviceName) == null) {
            output.show(Output.Type.ERROR, "service is not running");
            return;
        }

        try {
            services.get(serviceName).stop();
            services.remove(serviceName);
        } catch (Exception e) {
            logger.error("Error During Stopping Service.", e);
            getOutput().show(Output.Type.ERROR, "Error During Stopping Service." +
                    "Please See Logs");
        }
    }
}
